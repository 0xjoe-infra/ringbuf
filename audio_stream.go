package ringbuf

import (
	"encoding/binary"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

var (
	ErrFrameDropped         = errors.New("audio: frame dropped or evicted due to latency backpressure")
	ErrCorruptFrame         = errors.New("audio: buffer corrupted or invalid frame boundaries")
	ErrBufferClosed         = errors.New("audio: stream buffer is closed")
	ErrInvalidPayloadLength = errors.New("audio: incoming payload length does not match configured frame size")
	ErrOutBufferTooSmall    = errors.New("audio: caller destination buffer smaller than frame payload")
)

// OverflowPolicy defines real-time backpressure behavior.
type OverflowPolicy int

const (
	// PolicyRejectNew drops incoming frames when backpressure thresholds are met.
	PolicyRejectNew OverflowPolicy = iota
	// PolicyDropOldest discards the oldest head frames to keep conversational latency bounded.
	PolicyDropOldest
)

// HeaderSize is the 16-byte fixed binary envelope:
// [0..7]   Sequence ID (uint64, monotonic)
// [8..15]  Ingress Monotonic Timestamp (int64 nanoseconds relative to processOrigin)
const HeaderSize = 16

var processOrigin = time.Now()

// AudioStreamBuffer provides a Voice-AI audio pipeline with zero-alloc fast paths.
// Designed for a single-consumer model (e.g., worker feeding an audio sink / ASR stream).
type AudioStreamBuffer struct {
	// Metrics: sync/atomic typed fields are automatically 64-bit aligned across 32-bit and 64-bit platforms.
	acceptedFrames atomic.Uint64
	droppedFrames  atomic.Uint64
	evictedFrames  atomic.Uint64
	gapsDetected   atomic.Uint64

	ring        *RingBuffer
	policy      OverflowPolicy
	payloadSize int
	frameSize   int
	maxFrames   int // Strict conversational latency ceiling (e.g. 10 frames = 200ms)

	cond   *sync.Cond
	closed bool

	// Consumer tracking for deterministic sequence gap detection across stream lifecycle
	expectedNextSeq uint64
	streamStarted   bool
}

// NewAudioStreamBuffer initializes the buffer with latency-budget bounding.
// maxLatencyFrames limits queue depth to prevent buffer-bloat latency in Voice AI (e.g., 200ms = 10 frames).
func NewAudioStreamBuffer(capacityBytes int, framePayloadBytes int, maxLatencyFrames int, policy OverflowPolicy) (*AudioStreamBuffer, error) {
	if framePayloadBytes <= 0 {
		return nil, errors.New("audio: framePayloadBytes must be greater than zero")
	}

	totalFrameSize := HeaderSize + framePayloadBytes
	if capacityBytes < totalFrameSize {
		return nil, fmt.Errorf("capacity (%d bytes) must hold at least one frame (%d bytes)", capacityBytes, totalFrameSize)
	}

	maxPossibleFrames := capacityBytes / totalFrameSize
	if maxLatencyFrames <= 0 || maxLatencyFrames > maxPossibleFrames {
		maxLatencyFrames = maxPossibleFrames
	}

	rb, err := New(capacityBytes)
	if err != nil {
		return nil, err
	}

	asb := &AudioStreamBuffer{
		ring:        rb,
		policy:      policy,
		payloadSize: framePayloadBytes,
		frameSize:   totalFrameSize,
		maxFrames:   maxLatencyFrames,
	}
	asb.cond = sync.NewCond(&rb.mu)

	return asb, nil
}

// IngestFrame writes an audio frame with an external ingress timestamp.
// ingressTime must be sampled from a monotonic source (e.g. time.Now() at the socket read boundary).
func (a *AudioStreamBuffer) IngestFrame(seq uint64, ingressTime time.Time, payload []byte) error {
	if len(payload) != a.payloadSize {
		return ErrInvalidPayloadLength
	}

	a.ring.mu.Lock()
	defer a.ring.mu.Unlock()

	if a.closed {
		return ErrBufferClosed
	}

	// Initialize sequence anchor on the very first received frame so pre-drain evictions are detected
	if !a.streamStarted {
		a.expectedNextSeq = seq
		a.streamStarted = true
	}

	currentBufferedFrames := a.ring.cnt / a.frameSize

	// Latency budget and buffer capacity check
	hasCapacity := (a.ring.size-a.ring.cnt >= a.frameSize) && (currentBufferedFrames < a.maxFrames)
	if !hasCapacity {
		switch a.policy {
		case PolicyRejectNew:
			a.droppedFrames.Add(1)
			return ErrFrameDropped

		case PolicyDropOldest:
			// Zero-copy eviction: advance ring head pointer directly without memory copies
			for (a.ring.size-a.ring.cnt < a.frameSize) || (a.ring.cnt/a.frameSize >= a.maxFrames) {
				discarded := a.ring.discardUnsafe(a.frameSize)
				if discarded == 0 {
					break
				}
				a.evictedFrames.Add(1)
			}
		}
	}

	// Format 16-byte binary header on the stack (zero heap allocation)
	var hdr [HeaderSize]byte
	binary.LittleEndian.PutUint64(hdr[0:8], seq)
	monoNs := int64(ingressTime.Sub(processOrigin))
	binary.LittleEndian.PutUint64(hdr[8:16], uint64(monoNs))

	// Contiguous two-slice push with strict length verification
	nHdr := a.ring.pushUnsafe(hdr[:])
	if nHdr != HeaderSize {
		return ErrCorruptFrame
	}

	nPayload := a.ring.pushUnsafe(payload)
	if nPayload != a.payloadSize {
		return ErrCorruptFrame
	}

	a.acceptedFrames.Add(1)
	a.cond.Signal()
	return nil
}

// DrainFrame extracts the next audio chunk into outPayload with sequence gap detection.
// Returns seqGap = true if eviction or drop created a discontinuity in the stream (including before the first drain).
func (a *AudioStreamBuffer) DrainFrame(outPayload []byte, blocking bool) (seq uint64, latency time.Duration, seqGap bool, err error) {
	if len(outPayload) < a.payloadSize {
		return 0, 0, false, ErrOutBufferTooSmall
	}

	a.ring.mu.Lock()
	defer a.ring.mu.Unlock()

	for a.ring.cnt < a.frameSize {
		if a.closed {
			return 0, 0, false, ErrBufferClosed
		}
		if !blocking {
			return 0, 0, false, ErrBufferEmpty
		}
		a.cond.Wait()
	}

	// Drain fixed 16-byte header into local stack buffer
	var hdr [HeaderSize]byte
	n := a.ring.popUnsafe(hdr[:])
	if n < HeaderSize {
		return 0, 0, false, ErrCorruptFrame
	}

	seq = binary.LittleEndian.Uint64(hdr[0:8])
	ingressMonoNs := int64(binary.LittleEndian.Uint64(hdr[8:16]))

	// Drain audio payload directly into caller-provided slice
	n = a.ring.popUnsafe(outPayload[:a.payloadSize])
	if n < a.payloadSize {
		return 0, 0, false, ErrCorruptFrame
	}

	nowMonoNs := int64(time.Since(processOrigin))
	latency = time.Duration(nowMonoNs - ingressMonoNs)

	// Deterministic gap detection: checks against continuous expected stream progression
	if a.streamStarted && seq != a.expectedNextSeq {
		seqGap = true
		a.gapsDetected.Add(1)
	}
	a.expectedNextSeq = seq + 1

	return seq, latency, seqGap, nil
}

// Reset flushes all buffered frames in O(1) without memory copies (crucial for barge-in / speech cut-off).
func (a *AudioStreamBuffer) Reset() {
	a.ring.mu.Lock()
	defer a.ring.mu.Unlock()

	_ = a.ring.discardUnsafe(a.ring.cnt)
	a.streamStarted = false
}

// Close unblocks waiting consumers immediately and prevents further ingestion.
func (a *AudioStreamBuffer) Close() {
	a.ring.mu.Lock()
	defer a.ring.mu.Unlock()

	a.closed = true
	a.cond.Broadcast()
}

// Metrics accessors: safe for concurrent inspection on any architecture
func (a *AudioStreamBuffer) Accepted() uint64 { return a.acceptedFrames.Load() }
func (a *AudioStreamBuffer) Dropped() uint64  { return a.droppedFrames.Load() }
func (a *AudioStreamBuffer) Evicted() uint64  { return a.evictedFrames.Load() }
func (a *AudioStreamBuffer) Gaps() uint64     { return a.gapsDetected.Load() }

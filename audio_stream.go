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
	ErrFrameDropped    = errors.New("audio: incoming frame dropped due to backpressure")
	ErrCorruptFrame    = errors.New("audio: invalid frame payload boundary or malformed header")
	ErrBufferClosed    = errors.New("audio: stream buffer closed")
	ErrPayloadTooLarge = errors.New("audio: payload exceeds configured frame payload size")
)

// OverflowPolicy defines how the buffer reacts to capacity exhaustion.
type OverflowPolicy int

const (
	// PolicyRejectNew drops the incoming frame and returns ErrFrameDropped.
	PolicyRejectNew OverflowPolicy = iota
	// PolicyDropOldest evicts unread frames from the head to keep latency strictly bounded.
	PolicyDropOldest
)

// HeaderSize is the fixed 16-byte binary envelope:
// [0..7]   Sequence ID (uint64, monotonic)
// [8..15]  Monotonic Ingress Timestamp (int64 nanoseconds from process origin)
const HeaderSize = 16

var processOrigin = time.Now()

// AudioStreamBuffer provides an audio-aware, non-allocating wrapper around RingBuffer.
type AudioStreamBuffer struct {
	ring        *RingBuffer
	policy      OverflowPolicy
	payloadSize int
	frameSize   int

	cond   *sync.Cond
	closed bool

	// Reusable scratch buffer for eviction to prevent heap allocations
	evictScratch []byte
	headerScratch [HeaderSize]byte

	// Telemetry metrics
	AcceptedFrames uint64
	DroppedFrames  uint64
	EvictedFrames  uint64
}

// NewAudioStreamBuffer initializes an audio stream buffer with zero-alloc fast paths.
func NewAudioStreamBuffer(capacityBytes int, framePayloadBytes int, policy OverflowPolicy) (*AudioStreamBuffer, error) {
	if framePayloadBytes <= 0 {
		return nil, errors.New("audio: framePayloadBytes must be greater than zero")
	}

	totalFrameSize := HeaderSize + framePayloadBytes
	if capacityBytes < totalFrameSize {
		return nil, fmt.Errorf("capacity must hold at least one full frame (%d bytes)", totalFrameSize)
	}

	rb, err := New(capacityBytes)
	if err != nil {
		return nil, err
	}

	asb := &AudioStreamBuffer{
		ring:         rb,
		policy:       policy,
		payloadSize:  framePayloadBytes,
		frameSize:    totalFrameSize,
		evictScratch: make([]byte, totalFrameSize),
	}
	asb.cond = sync.NewCond(&rb.mu)

	return asb, nil
}

// IngestFrame streams an audio packet directly into the backing ring without allocating heap memory.
func (a *AudioStreamBuffer) IngestFrame(seq uint64, payload []byte) error {
	if len(payload) != a.payloadSize {
		return ErrPayloadTooLarge
	}

	// Lock the underlying ring buffer directly — zero double-locking churn
	a.ring.mu.Lock()
	defer a.ring.mu.Unlock()

	if a.closed {
		return ErrBufferClosed
	}

	// Handle capacity overflow deterministically
	if a.ring.size-a.ring.cnt < a.frameSize {
		switch a.policy {
		case PolicyRejectNew:
			atomic.AddUint64(&a.DroppedFrames, 1)
			return ErrFrameDropped

		case PolicyDropOldest:
			// Discard full frames until there is room for the new one
			for a.ring.size-a.ring.cnt < a.frameSize {
				n := a.ring.popUnsafe(a.evictScratch)
				if n == 0 {
					break
				}
				atomic.AddUint64(&a.EvictedFrames, 1)
			}
		}
	}

	// Prepare 16-byte fixed binary envelope on stack
	binary.LittleEndian.PutUint64(a.headerScratch[0:8], seq)
	// Monotonic nanoseconds relative to process origin (immune to wall-clock NTP steps)
	monoNs := int64(time.Since(processOrigin))
	binary.LittleEndian.PutUint64(a.headerScratch[8:16], uint64(monoNs))

	// Push header and payload directly into circular ring
	_ = a.ring.pushUnsafe(a.headerScratch[:])
	_ = a.ring.pushUnsafe(payload)

	atomic.AddUint64(&a.AcceptedFrames, 1)
	a.cond.Signal() // Wake waiting consumer immediately without spin/sleep latency
	return nil
}

// DrainFrame extracts the next audio chunk without allocations.
// If blocking is true, it sleeps on sync.Cond instead of polling.
func (a *AudioStreamBuffer) DrainFrame(outPayload []byte, blocking bool) (seq uint64, latency time.Duration, err error) {
	if len(outPayload) < a.payloadSize {
		return 0, 0, ErrCorruptFrame
	}

	a.ring.mu.Lock()
	defer a.ring.mu.Unlock()

	for a.ring.cnt < a.frameSize {
		if a.closed {
			return 0, 0, ErrBufferClosed
		}
		if !blocking {
			return 0, 0, ErrBufferEmpty
		}
		a.cond.Wait()
	}

	// Extract header
	n := a.ring.popUnsafe(a.headerScratch[:])
	if n < HeaderSize {
		return 0, 0, ErrCorruptFrame
	}

	seq = binary.LittleEndian.Uint64(a.headerScratch[0:8])
	ingressMonoNs := int64(binary.LittleEndian.Uint64(a.headerScratch[8:16]))

	// Extract audio payload directly into caller-supplied buffer
	n = a.ring.popUnsafe(outPayload[:a.payloadSize])
	if n < a.payloadSize {
		return 0, 0, ErrCorruptFrame
	}

	nowMonoNs := int64(time.Since(processOrigin))
	latency = time.Duration(nowMonoNs - ingressMonoNs)

	return seq, latency, nil
}

// Close unblocks any waiting consumers and seals the stream.
func (a *AudioStreamBuffer) Close() {
	a.ring.mu.Lock()
	defer a.ring.mu.Unlock()
	a.closed = true
	a.cond.Broadcast()
}

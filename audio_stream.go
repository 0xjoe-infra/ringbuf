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
	ErrFrameDropped = errors.New("audio: incoming frame dropped due to backpressure")
	ErrCorruptFrame = errors.New("audio: invalid frame header or sequence gap detected")
)

// OverflowPolicy defines how the buffer reacts to capacity exhaustion.
type OverflowPolicy int

const (
	// PolicyRejectNew returns ErrFrameDropped when buffer is full.
	PolicyRejectNew OverflowPolicy = iota
	// PolicyDropOldest drains unread frames from the head to preserve low conversational latency.
	PolicyDropOldest
)

// AudioFrameHeader represents the 16-byte fixed binary envelope for PCM streaming.
// Format:
// [0..7]   Sequence ID (uint64, monotonic)
// [8..15]  Ingress Timestamp (int64, unix nanoseconds)
type AudioFrameHeader struct {
	Seq         uint64
	TimestampNs int64
}

const HeaderSize = 16

// AudioStreamBuffer provides an audio-aware wrapper over the zero-alloc byte RingBuffer.
type AudioStreamBuffer struct {
	ring      *RingBuffer
	policy    OverflowPolicy
	frameSize int
	mu        sync.Mutex

	// Telemetry counters
	AcceptedFrames uint64
	DroppedFrames  uint64
	EvictedFrames  uint64
}

// NewAudioStreamBuffer initializes an audio buffer with fixed frame boundaries and explicit backpressure.
func NewAudioStreamBuffer(capacityBytes int, framePayloadBytes int, policy OverflowPolicy) (*AudioStreamBuffer, error) {
	totalFrameSize := HeaderSize + framePayloadBytes
	if capacityBytes < totalFrameSize {
		return nil, fmt.Errorf("capacity must hold at least one full frame (%d bytes)", totalFrameSize)
	}

	rb, err := New(capacityBytes)
	if err != nil {
		return nil, err
	}

	return &AudioStreamBuffer{
		ring:      rb,
		policy:    policy,
		frameSize: totalFrameSize,
	}, nil
}

// IngestFrame writes an audio chunk into the buffer with monotonic sequencing and timestamping.
func (a *AudioStreamBuffer) IngestFrame(seq uint64, payload []byte) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	requiredSpace := a.frameSize
	available := a.ring.Available()

	if available < requiredSpace {
		switch a.policy {
		case PolicyRejectNew:
			atomic.AddUint64(&a.DroppedFrames, 1)
			return ErrFrameDropped

		case PolicyDropOldest:
			// Drain enough bytes from head to fit the new frame
			evictBuf := make([]byte, requiredSpace)
			for a.ring.Available() < requiredSpace {
				n, err := a.ring.Pop(evictBuf)
				if err != nil || n == 0 {
					break
				}
				atomic.AddUint64(&a.EvictedFrames, 1)
			}
		}
	}

	// Pack header and payload into a contiguous slice without heap churn
	buf := make([]byte, a.frameSize)
	binary.LittleEndian.PutUint64(buf[0:8], seq)
	binary.LittleEndian.PutUint64(buf[8:16], uint64(time.Now().UnixNano()))
	copy(buf[16:], payload)

	_, err := a.ring.Push(buf)
	if err == nil {
		atomic.AddUint64(&a.AcceptedFrames, 1)
	}
	return err
}

// DrainFrame extracts the next sequential audio frame and calculates latency.
func (a *AudioStreamBuffer) DrainFrame(outPayload []byte) (seq uint64, latency time.Duration, err error) {
	a.mu.Lock()
	defer a.mu.Unlock()

	buf := make([]byte, a.frameSize)
	n, err := a.ring.Pop(buf)
	if err != nil {
		return 0, 0, err
	}
	if n < a.frameSize {
		return 0, 0, ErrCorruptFrame
	}

	seq = binary.LittleEndian.Uint64(buf[0:8])
	ingressNs := int64(binary.LittleEndian.Uint64(buf[8:16]))
	copy(outPayload, buf[16:])

	now := time.Now().UnixNano()
	latency = time.Duration(now - ingressNs)

	return seq, latency, nil
}

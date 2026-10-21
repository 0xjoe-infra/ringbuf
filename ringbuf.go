package ringbuf

import (
	"errors"
	"fmt"
	"sync"
)

var (
	ErrBufferFull   = errors.New("ringbuf: buffer is full")
	ErrBufferEmpty  = errors.New("ringbuf: buffer is empty")
	ErrInvalidSize  = errors.New("ringbuf: buffer size must be greater than zero")
)

// RingBuffer represents a high-throughput, bounded zero-alloc byte circular buffer.
// Safe for concurrent access across multiple reader/writer goroutines.
type RingBuffer struct {
	buf  []byte
	size int
	head int
	tail int
	cnt  int
	mu   sync.Mutex
}

// New creates a ring buffer with input validation and pre-allocated backing storage.
func New(size int) (*RingBuffer, error) {
	if size <= 0 {
		return nil, fmt.Errorf("%w: got %d", ErrInvalidSize, size)
	}
	return &RingBuffer{
		buf:  make([]byte, size),
		size: size,
	}, nil
}

// Push copies incoming audio chunks into the ring buffer using chunked copy operations.
// Eliminates per-byte modulo math on the fast path.
func (r *RingBuffer) Push(p []byte) (int, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	n := len(p)
	if n == 0 {
		return 0, nil
	}
	if n > r.size-r.cnt {
		return 0, ErrBufferFull
	}

	// Double-slice contiguous copy optimization
	firstChunk := min(n, r.size-r.tail)
	copy(r.buf[r.tail:r.tail+firstChunk], p[:firstChunk])

	if remaining := n - firstChunk; remaining > 0 {
		copy(r.buf[:remaining], p[firstChunk:])
		r.tail = remaining
	} else {
		r.tail = (r.tail + firstChunk) % r.size
	}

	r.cnt += n
	return n, nil
}

// Pop drains up to len(p) bytes using vectorized slice copies without allocations.
func (r *RingBuffer) Pop(p []byte) (int, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.cnt == 0 {
		return 0, ErrBufferEmpty
	}

	n := min(len(p), r.cnt)
	firstChunk := min(n, r.size-r.head)
	copy(p[:firstChunk], r.buf[r.head:r.head+firstChunk])

	if remaining := n - firstChunk; remaining > 0 {
		copy(p[firstChunk:n], r.buf[:remaining])
		r.head = remaining
	} else {
		r.head = (r.head + firstChunk) % r.size
	}

	r.cnt -= n
	return n, nil
}

// Available returns free byte capacity available for writing.
func (r *RingBuffer) Available() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.size - r.cnt
}

// Length returns the current number of buffered bytes.
func (r *RingBuffer) Length() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.cnt
}

package ringbuf

import (
	"errors"
	"sync"
)

var (
	ErrBufferFull  = errors.New("ringbuf: buffer is full")
	ErrBufferEmpty = errors.New("ringbuf: buffer is empty")
)

// RingBuffer represents a bounded, zero-alloc byte circular buffer.
type RingBuffer struct {
	buf  []byte
	size int
	head int
	tail int
	cnt  int
	mu   sync.Mutex
}

// New creates a ring buffer with a pre-allocated backing array.
func New(size int) *RingBuffer {
	return &RingBuffer{
		buf:  make([]byte, size),
		size: size,
	}
}

// Push appends data without heap allocations.
func (r *RingBuffer) Push(p []byte) (int, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	n := len(p)
	if n > r.size-r.cnt {
		return 0, ErrBufferFull
	}

	for i := 0; i < n; i++ {
		r.buf[r.tail] = p[i]
		r.tail = (r.tail + 1) % r.size
	}
	r.cnt += n
	return n, nil
}

// Pop extracts up to len(p) bytes without heap allocations.
func (r *RingBuffer) Pop(p []byte) (int, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.cnt == 0 {
		return 0, ErrBufferEmpty
	}

	n := len(p)
	if n > r.cnt {
		n = r.cnt
	}

	for i := 0; i < n; i++ {
		p[i] = r.buf[r.head]
		r.head = (r.head + 1) % r.size
	}
	r.cnt -= n
	return n, nil
}

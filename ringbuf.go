package ringbuf
import (
	"sync/atomic"
)

// RingBuffer реализует lock-free SPSC кольцевой буфер фиксированной емкости.
type RingBuffer struct {
	_pad0 [CacheLinePad]byte
	head  atomic.Uint64
	_pad1 [CacheLinePad]byte
	tail  atomic.Uint64
	_pad2 [CacheLinePad]byte
	mask  uint64
	buf   []byte
	_pad3 [CacheLinePad]byte
}

// New создает новый RingBuffer с размером, округленным до степени двойки.
func New(size int) *RingBuffer {
	if size < 2 {
		size = 2
	}
	cap := 1
	for cap < size {
		cap <<= 1
	}
	return &RingBuffer{
		mask: uint64(cap - 1),
		buf:  make([]byte, cap),
	}
}

// Write записывает срез байтов в буфер без аллокаций.
func (r *RingBuffer) Write(p []byte) (int, error) {
	n := len(p)
	if n == 0 {
		return 0, nil
	}
	head := r.head.Load()
	tail := r.tail.Load()
	free := int(r.mask+1) - int(head-tail)
	if n > free {
		return 0, ErrBufferFull
	}
	for i := 0; i < n; i++ {
		r.buf[(head+uint64(i))&r.mask] = p[i]
	}
	r.head.Add(uint64(n))
	return n, nil
}

// Read читает доступные байты из буфера в предоставленный срез p.
func (r *RingBuffer) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	tail := r.tail.Load()
	head := r.head.Load()
	avail := int(head - tail)
	if avail <= 0 {
		return 0, ErrBufferEmpty
	}
	n := len(p)
	if n > avail {
		n = avail
	}
	for i := 0; i < n; i++ {
		p[i] = r.buf[(tail+uint64(i))&r.mask]
	}
	r.tail.Add(uint64(n))
	return n, nil
}

// Len возвращает количество байтов, ожидающих чтения.
func (r *RingBuffer) Len() int {
	head := r.head.Load()
	tail := r.tail.Load()
	if head < tail {
		return 0
	}
	return int(head - tail)
}

// Cap возвращает полную емкость кольцевого буфера.
func (r *RingBuffer) Cap() int {
	return len(r.buf)
}

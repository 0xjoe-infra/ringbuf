package ringbuf

import (
	"errors"
	"sync/atomic"
)

// CacheLinePad учитывает парный Spatial Prefetcher (2 x 64 = 128 байт).
const CacheLinePad = 128

var (
	ErrBufferFull  = errors.New("ringbuf: buffer is full")
	ErrBufferEmpty = errors.New("ringbuf: buffer is empty")
)

// RingBuffer реализует экстремально быстрый lock-free SPSC буфер
// с локальным кэшированием индексов (исключает L1/L2 cache coherency трафик).
type RingBuffer struct {
	_pad0 [CacheLinePad]byte

	// Кэш-линия Producer (только запись)
	head       atomic.Uint64
	cachedTail uint64
	_pad1      [CacheLinePad - 16]byte

	// Кэш-линия Consumer (только чтение)
	tail       atomic.Uint64
	cachedHead uint64
	_pad2      [CacheLinePad - 16]byte

	// Read-only метаданные (инициализируются один раз)
	mask uint64
	buf  []byte
	_pad3 [CacheLinePad]byte
}

// New создает новый RingBuffer с емкостью, округленной вверх до степени двойки.
func New(size int) *RingBuffer {
	if size < 2 {
		size = 2
	}
	capacity := uint64(1)
	for capacity < uint64(size) {
		capacity <<= 1
	}
	return &RingBuffer{
		mask: capacity - 1,
		buf:  make([]byte, capacity),
	}
}

// Write записывает срез байтов в буфер (вызывается только одним Producer).
func (r *RingBuffer) Write(p []byte) (int, error) {
	n := uint64(len(p))
	if n == 0 {
		return 0, nil
	}

	capacity := r.mask + 1
	head := r.head.Load()

	// Проверяем свободное место по локальному кэшу без обращения к ядру Consumer
	free := capacity - (head - r.cachedTail)
	if n > free {
		// Обновляем кэш только при нехватке места (Acquire load)
		r.cachedTail = r.tail.Load()
		free = capacity - (head - r.cachedTail)
		if n > free {
			return 0, ErrBufferFull
		}
	}

	offset := head & r.mask
	firstChunk := capacity - offset
	if n <= firstChunk {
		copy(r.buf[offset:offset+n], p)
	} else {
		copy(r.buf[offset:], p[:firstChunk])
		copy(r.buf[:n-firstChunk], p[firstChunk:])
	}

	// Release store: публикует данные для Consumer
	r.head.Store(head + n)
	return int(n), nil
}

// Read читает доступные байты из буфера в срез p (вызывается только одним Consumer).
func (r *RingBuffer) Read(p []byte) (int, error) {
	maxRead := uint64(len(p))
	if maxRead == 0 {
		return 0, nil
	}

	tail := r.tail.Load()

	// Проверяем наличие данных по локальному кэшу без дерганья ядра Producer
	avail := r.cachedHead - tail
	if avail == 0 {
		// Обновляем кэш только когда локальные данные вычитаны (Acquire load)
		r.cachedHead = r.head.Load()
		avail = r.cachedHead - tail
		if avail == 0 {
			return 0, ErrBufferEmpty
		}
	}

	n := maxRead
	if n > avail {
		n = avail
	}

	capacity := r.mask + 1
	offset := tail & r.mask
	firstChunk := capacity - offset

	if n <= firstChunk {
		copy(p[:n], r.buf[offset:offset+n])
	} else {
		copy(p[:firstChunk], r.buf[offset:])
		copy(p[firstChunk:n], r.buf[:n-firstChunk])
	}

	// Release store: освобождает слоты для Producer
	r.tail.Store(tail + n)
	return int(n), nil
}

// Len возвращает актуальное количество байтов, ожидающих чтения.
func (r *RingBuffer) Len() int {
	tail := r.tail.Load()
	head := r.head.Load()
	return int(head - tail)
}

// Cap возвращает емкость буфера.
func (r *RingBuffer) Cap() int {
	return len(r.buf)
}

// Free возвращает актуальное количество свободных байтов для записи.
func (r *RingBuffer) Free() int {
	capacity := r.mask + 1
	head := r.head.Load()
	tail := r.tail.Load()
	occupied := head - tail
	if occupied >= capacity {
		return 0
	}
	return int(capacity - occupied)
}

package ringbuf

import (
	"errors"
	"sync/atomic"
)

var (
	ErrBufferFull  = errors.New("ringbuf: buffer full")
	ErrBufferEmpty = errors.New("ringbuf: buffer empty")
	ErrDstTooSmall = errors.New("ringbuf: destination slice too small")
)

const (
	CacheLinePad     = 64
	DefaultFrameSize = 960 // 20ms фрейм при 48kHz PCM/Opus
)

// AudioStream представляет lock-free SPSC буфер аудиокадров для real-time тракта.
type AudioStream struct {
	_pad0     [CacheLinePad]byte
	head      atomic.Uint64
	_pad1     [CacheLinePad]byte
	tail      atomic.Uint64
	_pad2     [CacheLinePad]byte
	dropped   atomic.Uint64
	_pad3     [CacheLinePad]byte
	mask      uint64
	frameSize int
	buffer    []byte
	_pad4     [CacheLinePad]byte
}

// NewAudioStream создает буфер заданной емкости кадров (округляется вверх до степени двойки).
func NewAudioStream(capacityFrames int, frameSize int) *AudioStream {
	if capacityFrames < 2 {
		capacityFrames = 2
	}
	cap := 1
	for cap < capacityFrames {
		cap <<= 1
	}

	return &AudioStream{
		mask:      uint64(cap - 1),
		frameSize: frameSize,
		buffer:    make([]byte, cap*frameSize),
	}
}

// PushFrame помещает аудиокадр в буфер.
// Безопасен для race detector (без пересечения памяти читателя и писателя).
func (s *AudioStream) PushFrame(src []byte) bool {
	if len(src) != s.frameSize {
		return false
	}

	head := s.head.Load()
	tail := s.tail.Load()
	capacity := s.mask + 1

	// Проверка переполнения без гонок по памяти
	if head-tail >= capacity {
		s.dropped.Add(1)
		return false
	}

	offset := int(head&s.mask) * s.frameSize
	copy(s.buffer[offset:offset+s.frameSize], src)

	s.head.Add(1)
	return true
}

// DrainFrame извлекает старейший аудиокадр в переданный буфер dst.
// Гарантирует нулевые аллокации (0 B/op, 0 allocs/op).
func (s *AudioStream) DrainFrame(dst []byte) (int, bool) {
	if len(dst) < s.frameSize {
		return 0, false
	}

	tail := s.tail.Load()
	head := s.head.Load()

	if tail >= head {
		return 0, false // Буфер пуст
	}

	offset := int(tail&s.mask) * s.frameSize
	copy(dst[:s.frameSize], s.buffer[offset:offset+s.frameSize])

	s.tail.Add(1)
	return s.frameSize, true
}

// Dropped возвращает число отброшенных кадров при переполнении.
func (s *AudioStream) Dropped() uint64 {
	return s.dropped.Load()
}

// Len возвращает текущее количество кадров в буфере.
func (s *AudioStream) Len() int {
	head := s.head.Load()
	tail := s.tail.Load()
	if head < tail {
		return 0
	}
	length := int(head - tail)
	capacity := int(s.mask + 1)
	if length > capacity {
		return capacity
	}
	return length
}

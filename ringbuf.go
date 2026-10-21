package ringbuf
​import (
"errors"
"fmt"
"sync"
)
​var (
ErrBufferFull  = errors.New("ringbuf: buffer is full")
ErrBufferEmpty = errors.New("ringbuf: buffer is empty")
ErrInvalidSize = errors.New("ringbuf: buffer size must be greater than zero")
)
​type RingBuffer struct {
buf  []byte
size int
head int
tail int
cnt  int
mu   sync.Mutex
}
​func New(size int) (*RingBuffer, error) {
if size <= 0 {
return nil, fmt.Errorf("%w: got %d", ErrInvalidSize, size)
}
return &RingBuffer{
buf:  make([]byte, size),
size: size,
}, nil
}
​func (r *RingBuffer) Push(p []byte) (int, error) {
r.mu.Lock()
defer r.mu.Unlock()
​n := len(p)
if n == 0 {
return 0, nil
}
if n > r.size-r.cnt {
return 0, ErrBufferFull
}
return r.pushUnsafe(p), nil
}
​func (r *RingBuffer) Pop(p []byte) (int, error) {
r.mu.Lock()
defer r.mu.Unlock()
​if r.cnt == 0 {
return 0, ErrBufferEmpty
}
return r.popUnsafe(p), nil
}
​func (r *RingBuffer) pushUnsafe(p []byte) int {
n := min(len(p), r.size-r.cnt)
if n == 0 {
return 0
}
​firstChunk := min(n, r.size-r.tail)
copy(r.buf[r.tail:r.tail+firstChunk], p[:firstChunk])
​if remaining := n - firstChunk; remaining > 0 {
copy(r.buf[:remaining], p[firstChunk:])
r.tail = remaining
} else {
r.tail = (r.tail + firstChunk) % r.size
}
​r.cnt += n
return n
}
​// pushFrameUnsafe guarantees all-or-nothing insertion for multiple contiguous chunks
// (e.g. fixed header + payload) under an already-held lock, eliminating partial writes by design.
func (r *RingBuffer) pushFrameUnsafe(header []byte, payload []byte) bool {
total := len(header) + len(payload)
if total > r.size-r.cnt {
return false
}
if r.pushUnsafe(header) != len(header) {
panic("ringbuf: invariant broken on atomic header push")
}
if r.pushUnsafe(payload) != len(payload) {
panic("ringbuf: invariant broken on atomic payload push")
}
return true
}
​func (r *RingBuffer) popUnsafe(p []byte) int {
n := min(len(p), r.cnt)
if n == 0 {
return 0
}
​firstChunk := min(n, r.size-r.head)
copy(p[:firstChunk], r.buf[r.head:r.head+firstChunk])
​if remaining := n - firstChunk; remaining > 0 {
copy(p[firstChunk:n], r.buf[:remaining])
r.head = remaining
} else {
r.head = (r.head + firstChunk) % r.size
}
​r.cnt -= n
return n
}
​func (r *RingBuffer) discardUnsafe(n int) int {
toDiscard := min(n, r.cnt)
if toDiscard == 0 {
return 0
}
​r.head = (r.head + toDiscard) % r.size
r.cnt -= toDiscard
return toDiscard
}
​func (r *RingBuffer) Available() int {
r.mu.Lock()
defer r.mu.Unlock()
return r.size - r.cnt
}
​func (r *RingBuffer) Length() int {
r.mu.Lock()
defer r.mu.Unlock()
return r.cnt
}

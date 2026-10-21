package ringbuf
import (
	"bytes"
	"sync"
	"testing"
)

func TestRingBuffer_WriteRead(t *testing.T) {
	rb := New(64)
	payload := []byte("zero-alloc-voice-frame")

	n, err := rb.Write(payload)
	if err != nil || n != len(payload) {
		t.Fatalf("unexpected write result: n=%d, err=%v", n, err)
	}

	if rb.Len() != len(payload) {
		t.Fatalf("expected len %d, got %d", len(payload), rb.Len())
	}

	out := make([]byte, len(payload))
	rn, err := rb.Read(out)
	if err != nil || rn != len(payload) {
		t.Fatalf("unexpected read result: n=%d, err=%v", rn, err)
	}

	if !bytes.Equal(payload, out) {
		t.Fatalf("data mismatch: expected %q, got %q", payload, out)
	}
}

func TestRingBuffer_OverflowAndEmpty(t *testing.T) {
	rb := New(16)
	data := make([]byte, 16)

	if _, err := rb.Read(data); err != ErrBufferEmpty {
		t.Fatalf("expected ErrBufferEmpty, got %v", err)
	}

	if _, err := rb.Write(data); err != nil {
		t.Fatalf("unexpected write error: %v", err)
	}

	if _, err := rb.Write([]byte{1}); err != ErrBufferFull {
		t.Fatalf("expected ErrBufferFull, got %v", err)
	}
}

func TestRingBuffer_ConcurrentSPSC(t *testing.T) {
	rb := New(1024)
	const iterations = 5000
	frame := []byte("01234567")

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		for i := 0; i < iterations; {
			if _, err := rb.Write(frame); err == nil {
				i++
			}
		}
	}()

	go func() {
		defer wg.Done()
		buf := make([]byte, len(frame))
		for readTotal := 0; readTotal < iterations*len(frame); {
			if n, err := rb.Read(buf); err == nil {
				readTotal += n
			}
		}
	}()

	wg.Wait()
}

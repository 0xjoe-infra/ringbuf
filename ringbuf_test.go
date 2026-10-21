package ringbuf

import (
	"bytes"
	"sync"
	"testing"
)

func TestRingBuffer_BasicPushPop(t *testing.T) {
	rb := New(1024)
	input := []byte("audio-chunk-pcm-16khz")

	n, err := rb.Push(input)
	if err != nil {
		t.Fatalf("unexpected push error: %v", err)
	}
	if n != len(input) {
		t.Fatalf("expected written %d bytes, got %d", len(input), n)
	}

	out := make([]byte, len(input))
	readBytes, err := rb.Pop(out)
	if err != nil {
		t.Fatalf("unexpected pop error: %v", err)
	}
	if readBytes != len(input) {
		t.Fatalf("expected read %d bytes, got %d", len(input), readBytes)
	}
	if !bytes.Equal(input, out) {
		t.Fatalf("payload mismatch: expected %s, got %s", input, out)
	}
}

func TestRingBuffer_OverflowProtection(t *testing.T) {
	rb := New(32)
	overflowData := make([]byte, 64)

	_, err := rb.Push(overflowData)
	if err != ErrBufferFull {
		t.Fatalf("expected ErrBufferFull, got %v", err)
	}
}

func TestRingBuffer_ConcurrentIngest(t *testing.T) {
	rb := New(64 * 1024)
	payload := []byte("frame-header-data")
	var wg sync.WaitGroup

	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 20; j++ {
				_, _ = rb.Push(payload)
			}
		}()
	}
	wg.Wait()
}

// Benchmark demonstrating zero heap allocations per push/pop cycle.
// Run with: go test -bench=. -benchmem
func BenchmarkRingBuffer_WriteRead(b *testing.B) {
	rb := New(64 * 1024)
	chunk := make([]byte, 256) // Typical 256-byte PCM audio frame
	readBuf := make([]byte, 256)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, _ = rb.Push(chunk)
		_, _ = rb.Pop(readBuf)
	}
}

package ringbuf

import (
	"bytes"
	package ringbuf

import (
	"bytes"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
)

func TestRingBuffer_InvalidCapacity(t *testing.T) {
	_, err := New(0)
	if !errors.Is(err, ErrInvalidSize) {
		t.Fatalf("expected ErrInvalidSize on zero capacity, got %v", err)
	}

	_, err = New(-64)
	if !errors.Is(err, ErrInvalidSize) {
		t.Fatalf("expected ErrInvalidSize on negative capacity, got %v", err)
	}
}

func TestRingBuffer_WraparoundIntegrity(t *testing.T) {
	rb, err := New(16)
	if err != nil {
		t.Fatal(err)
	}

	payload1 := []byte("1234567890")
	if _, err := rb.Push(payload1); err != nil {
		t.Fatalf("unexpected push error: %v", err)
	}
	out1 := make([]byte, 10)
	if _, err := rb.Pop(out1); err != nil || !bytes.Equal(payload1, out1) {
		t.Fatalf("pop mismatch or error: %v", err)
	}

	payload2 := []byte("abcdefghijkl")
	if _, err := rb.Push(payload2); err != nil {
		t.Fatalf("push wraparound failed: %v", err)
	}

	out2 := make([]byte, 12)
	n, err := rb.Pop(out2)
	if err != nil || n != 12 || !bytes.Equal(payload2, out2) {
		t.Fatalf("wraparound data corrupted: expected %s, got %s", payload2, out2)
	}
}

func TestRingBuffer_ConcurrentProducerConsumer_Checksum(t *testing.T) {
	const (
		bufCap    = 4096
		totalMsgs = 10000
		chunkSize = 32
	)

	rb, err := New(bufCap)
	if err != nil {
		t.Fatal(err)
	}

	var (
		wg         sync.WaitGroup
		writtenSum uint64
		readSum    uint64
	)

	wg.Add(2)

	go func() {
		defer wg.Done()
		for i := uint64(0); i < totalMsgs; i++ {
			chunk := make([]byte, chunkSize)
			for j := range chunk {
				chunk[j] = byte(i & 0xFF)
			}
			for {
				_, pushErr := rb.Push(chunk)
				if pushErr == nil {
					atomic.AddUint64(&writtenSum, uint64(len(chunk))*uint64(i&0xFF))
					break
				}
			}
		}
	}()

	go func() {
		defer wg.Done()
		out := make([]byte, chunkSize)
		consumed := 0
		for consumed < totalMsgs {
			n, popErr := rb.Pop(out)
			if popErr == nil && n == chunkSize {
				val := out[0]
				atomic.AddUint64(&readSum, uint64(n)*uint64(val))
				consumed++
			}
		}
	}()

	wg.Wait()

	if writtenSum != readSum {
		t.Fatalf("concurrency checksum mismatch: written %d != read %d", writtenSum, readSum)
	}
}

func BenchmarkRingBuffer_SingleThreaded(b *testing.B) {
	rb, _ := New(64 * 1024)
	chunk := make([]byte, 256)
	out := make([]byte, 256)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, _ = rb.Push(chunk)
		_, _ = rb.Pop(out)
	}
}

func BenchmarkRingBuffer_Concurrent_Contention(b *testing.B) {
	rb, _ := New(256 * 1024)
	chunk := make([]byte, 256)

	b.ResetTimer()
	b.ReportAllocs()

	b.RunParallel(func(pb *testing.PB) {
		readBuf := make([]byte, 256)
		for pb.Next() {
			_, err := rb.Push(chunk)
			if err == nil {
				_, _ = rb.Pop(readBuf)
			}
		}
	})
}

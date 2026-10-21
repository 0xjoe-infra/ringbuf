package ringbuf

import (
	"sync"
	"testing"
)

func TestAudioStream_ConcurrentPushDrain(t *testing.T) {
	const (
		capacity  = 64
		frameSize = 960
		totalOps  = 10000
	)

	stream := NewAudioStream(capacity, frameSize)
	var wg sync.WaitGroup
	wg.Add(2)

	// Producer
	go func() {
		defer wg.Done()
		src := make([]byte, frameSize)
		for i := 0; i < totalOps; i++ {
			src[0] = byte(i)
			stream.PushFrame(src)
		}
	}()

	// Consumer
	go func() {
		defer wg.Done()
		dst := make([]byte, frameSize)
		received := 0
		for received < totalOps {
			if n, ok := stream.DrainFrame(dst); ok {
				if n != frameSize {
					t.Errorf("expected frame size %d, got %d", frameSize, n)
				}
				received++
			}
			if received+int(stream.Dropped()) >= totalOps {
				break
			}
		}
	}()

	wg.Wait()
}

func BenchmarkAudioStream_ZeroAllocSustainedEviction(b *testing.B) {
	const (
		capacity  = 128
		frameSize = 960
	)

	stream := NewAudioStream(capacity, frameSize)
	src := make([]byte, frameSize)
	dst := make([]byte, frameSize)

	for i := range src {
		src[i] = 0xAA
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		stream.PushFrame(src)
		stream.DrainFrame(dst)
	}
}

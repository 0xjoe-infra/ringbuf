package ringbuf

import (
	"sync"
	"testing"
)

func TestAudioStream_PushDrain(t *testing.T) {
	stream := NewAudioStream(4, DefaultFrameSize)

	frameIn := make([]byte, DefaultFrameSize)
	for i := range frameIn {
		frameIn[i] = byte(i & 0xFF)
	}

	if !stream.PushFrame(frameIn) {
		t.Fatal("expected PushFrame to succeed")
	}

	if stream.Len() != 1 {
		t.Fatalf("expected Len 1, got %d", stream.Len())
	}

	frameOut := make([]byte, DefaultFrameSize)
	if !stream.DrainFrame(frameOut) {
		t.Fatal("expected DrainFrame to succeed")
	}

	for i := range frameOut {
		if frameOut[i] != frameIn[i] {
			t.Fatalf("mismatch at byte %d: got %d, want %d", i, frameOut[i], frameIn[i])
		}
	}
}

func TestAudioStream_OverflowDrops(t *testing.T) {
	stream := NewAudioStream(2, DefaultFrameSize)
	frame := make([]byte, DefaultFrameSize)

	if !stream.PushFrame(frame) || !stream.PushFrame(frame) {
		t.Fatal("expected first 2 frames to succeed")
	}

	if stream.PushFrame(frame) {
		t.Fatal("expected 3rd frame to be dropped on full buffer")
	}

	if stream.Dropped() != 1 {
		t.Fatalf("expected Dropped() == 1, got %d", stream.Dropped())
	}
}

func TestAudioStream_ConcurrencyRace(t *testing.T) {
	stream := NewAudioStream(64, DefaultFrameSize)
	const iterations = 10000

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		src := make([]byte, DefaultFrameSize)
		for i := 0; i < iterations; i++ {
			src[0] = byte(i)
			stream.PushFrame(src)
		}
	}()

	go func() {
		defer wg.Done()
		dst := make([]byte, DefaultFrameSize)
		for i := 0; i < iterations; i++ {
			stream.DrainFrame(dst)
		}
	}()

	wg.Wait()
}

func BenchmarkAudioStream_ZeroAlloc(b *testing.B) {
	stream := NewAudioStream(128, DefaultFrameSize)
	src := make([]byte, DefaultFrameSize)
	dst := make([]byte, DefaultFrameSize)

	b.ReportAllocs()
	b.SetBytes(int64(DefaultFrameSize))
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		stream.PushFrame(src)
		stream.DrainFrame(dst)
	}
}

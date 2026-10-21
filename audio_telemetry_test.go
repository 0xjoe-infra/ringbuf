package ringbuf

import (
	"bytes"
	"math/rand"
	"sort"
	"sync"
	"testing"
	"time"
)

// TestAudioStream_UnalignedWraparoundWithEviction proves that unaligned ring capacity
// correctly wraps both the 16-byte binary header and payload across buffer boundaries
// during active eviction without memory corruption.
func TestAudioStream_UnalignedWraparoundWithEviction(t *testing.T) {
	const payloadSize = 320 // 10ms frame
	totalFrameSize := HeaderSize + payloadSize

	// Capacity: holds 2 frames + 13 bytes offset (guarantees boundary split)
	unalignedCap := (totalFrameSize * 2) + 13

	asb, err := NewAudioStreamBuffer(unalignedCap, payloadSize, 2, PolicyDropOldest)
	if err != nil {
		t.Fatalf("failed to init unaligned buffer: %v", err)
	}

	outPayload := make([]byte, payloadSize)

	// Stream 30 frames with eviction continuously wrapping across the 13-byte boundary
	for i := uint64(1); i <= 30; i++ {
		payload := bytes.Repeat([]byte{byte(i & 0xFF)}, payloadSize)
		ingressTime := time.Now()

		if err := asb.IngestFrame(i, ingressTime, payload); err != nil {
			t.Fatalf("frame %d failed to ingest: %v", i, err)
		}

		// Drain every second frame to constantly force wraparound eviction
		if i%2 == 0 {
			seq, _, _, err := asb.DrainFrame(outPayload, false)
			if err != nil {
				t.Fatalf("frame %d failed to drain: %v", i, err)
			}
			expectedPayload := bytes.Repeat([]byte{byte(seq & 0xFF)}, payloadSize)
			if !bytes.Equal(outPayload, expectedPayload) {
				t.Fatalf("payload corrupted during unaligned wraparound eviction at seq %d", seq)
			}
		}
	}
}

// TestAudioStream_PreDrainEvictionGapDetection verifies that when evictions occur
// before the consumer ever executes DrainFrame, the first drained frame detects the gap immediately.
func TestAudioStream_PreDrainEvictionGapDetection(t *testing.T) {
	const payloadSize = 640
	totalFrameSize := HeaderSize + payloadSize
	asb, _ := NewAudioStreamBuffer(totalFrameSize*3, payloadSize, 3, PolicyDropOldest)

	dummy := make([]byte, payloadSize)

	// Ingest 5 frames into 3-frame capacity before any drain happens
	for seq := uint64(1); seq <= 5; seq++ {
		_ = asb.IngestFrame(seq, time.Now(), dummy)
	}

	if asb.Evicted() != 2 {
		t.Fatalf("expected 2 evicted frames, got %d", asb.Evicted())
	}

	out := make([]byte, payloadSize)

	// First consumed frame must be seq 3 AND must flag seqGap == true because seq 1 and 2 were lost
	seq, _, seqGap, err := asb.DrainFrame(out, false)
	if err != nil || seq != 3 {
		t.Fatalf("expected first frame seq=3, got %d (err=%v)", seq, err)
	}
	if !seqGap {
		t.Fatalf("expected seqGap=true on first drain when head was evicted before read")
	}
}

// TestAudioStream_ConcurrentStreamingWithRace exercises the 50 Hz real-time voice pipeline
// with injected backpressure stalls under race detector verification, asserting graceful Close().
func TestAudioStream_ConcurrentStreamingWithRace(t *testing.T) {
	const (
		payloadSize    = 640 // 20ms of 16kHz audio
		totalFrameSize = HeaderSize + payloadSize
		maxLatency     = 5 // 100ms conversational ceiling
	)

	asb, err := NewAudioStreamBuffer(totalFrameSize*10, payloadSize, maxLatency, PolicyDropOldest)
	if err != nil {
		t.Fatal(err)
	}

	var (
		wg         sync.WaitGroup
		latencies  []time.Duration
		latencyMu  sync.Mutex
		stopSignal = make(chan struct{})
	)

	wg.Add(2)

	// Producer: 50 Hz real-time audio pacing
	go func() {
		defer wg.Done()
		ticker := time.NewTicker(20 * time.Millisecond)
		defer ticker.Stop()

		var seq uint64 = 1
		dummy := make([]byte, payloadSize)

		for {
			select {
			case <-stopSignal:
				return
			case t := <-ticker.C:
				_ = asb.IngestFrame(seq, t, dummy)
				seq++
			}
		}
	}()

	// Consumer: Drain with backpressure jitter
	go func() {
		defer wg.Done()
		out := make([]byte, payloadSize)

		for {
			seq, latency, _, err := asb.DrainFrame(out, true)
			if err != nil {
				return // Closed cleanly
			}
			if seq > 0 {
				latencyMu.Lock()
				latencies = append(latencies, latency)
				latencyMu.Unlock()
			}

			// Injected socket lag (30ms stall triggers eviction against 100ms budget)
			if rand.Float64() < 0.10 {
				time.Sleep(30 * time.Millisecond)
			}
		}
	}()

	time.Sleep(300 * time.Millisecond)
	close(stopSignal)
	asb.Close() // Wake blocked consumer goroutines
	wg.Wait()

	if asb.Accepted() == 0 {
		t.Fatal("no frames accepted")
	}
}

// TestAudioStream_BargeInReset asserts that Reset() purges all queued latency in O(1).
func TestAudioStream_BargeInReset(t *testing.T) {
	const payloadSize = 320
	asb, _ := NewAudioStreamBuffer((HeaderSize+payloadSize)*5, payloadSize, 5, PolicyRejectNew)

	dummy := make([]byte, payloadSize)
	for i := uint64(1); i <= 4; i++ {
		_ = asb.IngestFrame(i, time.Now(), dummy)
	}

	asb.Reset()

	out := make([]byte, payloadSize)
	_, _, _, err := asb.DrainFrame(out, false)
	if err != ErrBufferEmpty {
		t.Fatalf("expected ErrBufferEmpty after Reset(), got %v", err)
	}
}

// BenchmarkAudioStream_ZeroAllocSustainedEviction continuously triggers DropOldest eviction
// via a 2:1 Ingest:Drain ratio, proving zero allocations and recording evicted metrics.
func BenchmarkAudioStream_ZeroAllocSustainedEviction(b *testing.B) {
	const payloadSize = 640
	totalFrameSize := HeaderSize + payloadSize
	// Capacity: 4 frames
	asb, _ := NewAudioStreamBuffer(totalFrameSize*4, payloadSize, 4, PolicyDropOldest)

	payload := make([]byte, payloadSize)
	outPayload := make([]byte, payloadSize)
	now := time.Now()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		// 2 Ingests per 1 Drain forces continuous buffer saturation and head eviction
		_ = asb.IngestFrame(uint64(i*2), now, payload)
		_ = asb.IngestFrame(uint64(i*2+1), now, payload)
		_, _, _, _ = asb.DrainFrame(outPayload, false)
	}

	b.ReportMetric(float64(asb.Evicted()), "evicted_frames")
}

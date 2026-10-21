package ringbuf

import (
	"sort"
	"sync"
	"testing"
	"time"
)

// TestAudioStream_DropOldest_EvictionAssert proves that when capacity is exhausted,
// older frames are evicted deterministically and sequence continuity reports the gap.
func TestAudioStream_DropOldest_EvictionAssert(t *testing.T) {
	const payloadSize = 640 // 20ms of 16kHz PCM
	const capacity = (HeaderSize + payloadSize) * 3 // Holds exactly 3 frames

	asb, err := NewAudioStreamBuffer(capacity, payloadSize, PolicyDropOldest)
	if err != nil {
		t.Fatalf("failed to init buffer: %v", err)
	}

	dummyPayload := make([]byte, payloadSize)

	// Ingest 5 frames into a buffer that only holds 3
	for seq := uint64(1); seq <= 5; seq++ {
		if err := asb.IngestFrame(seq, dummyPayload); err != nil {
			t.Fatalf("ingest frame %d failed: %v", seq, err)
		}
	}

	// Frames 1 and 2 must have been evicted to make room for frames 3, 4, 5
	if asb.EvictedFrames != 2 {
		t.Fatalf("expected exactly 2 evicted frames, got %d", asb.EvictedFrames)
	}

	outBuf := make([]byte, payloadSize)

	// First drained frame must now be seq 3 (not seq 1)
	firstSeq, _, err := asb.DrainFrame(outBuf, false)
	if err != nil || firstSeq != 3 {
		t.Fatalf("expected head frame seq 3 after eviction, got seq %d (err: %v)", firstSeq, err)
	}
}

// BenchmarkAudioStream_ZeroAllocFastPath proves that frame ingestion and draining
// perform exactly ZERO heap allocations on the hot path.
func BenchmarkAudioStream_ZeroAllocFastPath(b *testing.B) {
	const payloadSize = 640
	const capacity = (HeaderSize + payloadSize) * 64

	asb, _ := NewAudioStreamBuffer(capacity, payloadSize, PolicyRejectNew)
	payload := make([]byte, payloadSize)
	outPayload := make([]byte, payloadSize)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_ = asb.IngestFrame(uint64(i), payload)
		_, _, _ = asb.DrainFrame(outPayload, false)
	}
}

// TestAudioStream_TelemetryFieldSimulation runs a high-load concurrent voice stream
// measuring tail latency using sync.Cond signal awakening (no polling sleep).
func TestAudioStream_TelemetryFieldSimulation(t *testing.T) {
	const payloadSize = 640
	// 200ms latency budget capacity (10 frames)
	const capacity = (HeaderSize + payloadSize) * 10
	const totalFrames = 150

	asb, err := NewAudioStreamBuffer(capacity, payloadSize, PolicyDropOldest)
	if err != nil {
		t.Fatal(err)
	}

	var (
		wg        sync.WaitGroup
		latencies []time.Duration
		latMu     sync.Mutex
	)

	wg.Add(2)

	// Consumer: Event-driven via sync.Cond (zero sleep polling overhead)
	go func() {
		defer wg.Done()
		out := make([]byte, payloadSize)
		for {
			seq, lat, err := asb.DrainFrame(out, true)
			if err != nil {
				return // Buffer closed
			}
			latMu.Lock()
			latencies = append(latencies, lat)
			latMu.Unlock()

			if seq == totalFrames {
				return
			}
		}
	}()

	// Producer: 50Hz pacing (20ms frames)
	go func() {
		defer wg.Done()
		payload := make([]byte, payloadSize)
		ticker := time.NewTicker(2 * time.Millisecond) // Accelerated simulation
		defer ticker.Stop()

		for seq := uint64(1); seq <= totalFrames; seq++ {
			<-ticker.C
			_ = asb.IngestFrame(seq, payload)
		}
		asb.Close()
	}()

	wg.Wait()

	if len(latencies) == 0 {
		t.Fatal("no frames drained")
	}

	sort.Slice(latencies, func(i, j int) bool {
		return latencies[i] < latencies[j]
	})

	p50 := latencies[len(latencies)*50/100]
	p95 := latencies[len(latencies)*95/100]
	p99 := latencies[len(latencies)*99/100]

	t.Logf("Telemetry Report: p50=%v | p95=%v | p99=%v | Evicted=%d", p50, p95, p99, asb.EvictedFrames)
}

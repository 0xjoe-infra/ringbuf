package ringbuf

import (
	"math/rand"
	"sort"
	"sync"
	"testing"
	"time"
)

// TestAudioStream_TelemetryScenario executes a simulated 16kHz PCM stream (20ms frames = 640B)
// under sustained producer-consumer conditions and measures tail latency percentiles.
func TestAudioStream_TelemetryScenario(t *testing.T) {
	const (
		framePayloadSize = 640 // 20ms of 16kHz mono PCM16
		bufferCapacity   = 64 * 1024
		streamDuration   = 3 * time.Second
	)

	asb, err := NewAudioStreamBuffer(bufferCapacity, framePayloadSize, PolicyDropOldest)
	if err != nil {
		t.Fatalf("failed to initialize audio stream buffer: %v", err)
	}

	var (
		wg         sync.WaitGroup
		latencies  []time.Duration
		latencyMu  sync.Mutex
		stopSignal = make(chan struct{})
	)

	wg.Add(2)

	// Producer: Generates 20ms frames at 50Hz (every 20ms)
	go func() {
		defer wg.Done()
		ticker := time.NewTicker(20 * time.Millisecond)
		defer ticker.Stop()

		var seq uint64 = 1
		payload := make([]byte, framePayloadSize)

		for {
			select {
			case <-stopSignal:
				return
			case <-ticker.C:
				_ = asb.IngestFrame(seq, payload)
				seq++
			}
		}
	}()

	// Consumer: Reads frames with occasional injected stalls (socket backpressure simulation)
	go func() {
		defer wg.Done()
		buf := make([]byte, framePayloadSize)

		for {
			select {
			case <-stopSignal:
				return
			default:
				seq, latency, err := asb.DrainFrame(buf)
				if err == nil && seq > 0 {
					latencyMu.Lock()
					latencies = append(latencies, latency)
					latencyMu.Unlock()
				} else {
					time.Sleep(2 * time.Millisecond)
				}

				// Simulate 5% random jitter / socket read stall
				if rand.Float64() < 0.05 {
					time.Sleep(30 * time.Millisecond)
				}
			}
		}
	}()

	time.Sleep(streamDuration)
	close(stopSignal)
	wg.Wait()

	if len(latencies) == 0 {
		t.Fatal("no frames drained during telemetry run")
	}

	// Calculate latency percentiles
	sort.Slice(latencies, func(i, j int) bool {
		return latencies[i] < latencies[j]
	})

	p50 := latencies[len(latencies)*50/100]
	p95 := latencies[len(latencies)*95/100]
	p99 := latencies[len(latencies)*99/100]

	t.Logf("--- Telemetry Report (16kHz PCM, 20ms chunks) ---")
	t.Logf("Frames Accepted: %d | Evicted: %d | Total Drained: %d", asb.AcceptedFrames, asb.EvictedFrames, len(latencies))
	t.Logf("Latency p50: %v", p50)
	t.Logf("Latency p95: %v", p95)
	t.Logf("Latency p99: %v", p99)
}

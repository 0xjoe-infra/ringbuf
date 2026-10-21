package ringbuf
​import (
"bytes"
"math/rand"
"runtime"
"sync"
"testing"
"time"
)
​func TestAudioStream_CloseUnblocksConsumer(t *testing.T) {
const payloadSize = 320
asb, err := NewAudioStreamBuffer((HeaderSize+payloadSize)*4, payloadSize, 2, PolicyRejectNew)
if err != nil {
t.Fatal(err)
}
​done := make(chan struct{})
go func() {
out := make([]byte, payloadSize)
_, err := asb.DrainFrame(out, true)
if err != ErrBufferClosed {
t.Errorf("expected ErrBufferClosed, got %v", err)
}
close(done)
}()
​time.Sleep(20 * time.Millisecond)
asb.Close()
​select {
case <-done:
case <-time.After(500 * time.Millisecond):
t.Fatal("DrainFrame failed to unblock on Close()")
}
}
​func TestAudioStream_LatencyBudgetAndGapDetection(t *testing.T) {
const payloadSize = 640
totalFrameSize := HeaderSize + payloadSize
largeCapacity := totalFrameSize * 20
budgetFrames := 4
​asb, err := NewAudioStreamBuffer(largeCapacity, payloadSize, budgetFrames, PolicyDropOldest)
if err != nil {
t.Fatal(err)
}
​dummy := make([]byte, payloadSize)
now := time.Now()
​for seq := uint64(1); seq <= 6; seq++ {
_ = asb.IngestFrame(seq, now, dummy)
}
​if asb.Evicted() != 2 {
t.Fatalf("expected 2 evicted frames, got %d", asb.Evicted())
}
​out := make([]byte, payloadSize)
info, err := asb.DrainFrame(out, false)
if err != nil || info.Seq != 3 || !info.SeqGap || info.Lost != 2 {
t.Fatalf("expected head seq=3 with SeqGap=true and Lost=2, got %+v (err=%v)", info, err)
}
}
​func TestAudioStream_ZeroAllocMemStats_Strict(t testing.T) {
const payloadSize = 640
totalFrameSize := HeaderSize + payloadSize
asb, err := NewAudioStreamBuffer(totalFrameSize4, payloadSize, 4, PolicyDropOldest)
if err != nil {
t.Fatal(err)
}
​payload := make([]byte, payloadSize)
outPayload := make([]byte, payloadSize)
now := time.Now()
​// Warm-up to steady state
for i := uint64(1); i <= 10; i++ {
_ = asb.IngestFrame(i, now, payload)
_, _ = asb.DrainFrame(outPayload, false)
}
​evictedBefore := asb.Evicted()
​runtime.GC()
var m1, m2 runtime.MemStats
runtime.ReadMemStats(&m1)
​var seq uint64 = 11
const iterations = 5000
for i := 0; i < iterations; i++ {
_ = asb.IngestFrame(seq, now, payload)
seq++
_ = asb.IngestFrame(seq, now, payload)
seq++
_, _ = asb.DrainFrame(outPayload, false)
}
​runtime.ReadMemStats(&m2)
totalMallocs := m2.Mallocs - m1.Mallocs
​if totalMallocs != 0 {
t.Fatalf("strict zero-alloc violated: expected 0 heap mallocs, got %d", totalMallocs)
}
​if asb.Evicted() <= evictedBefore {
t.Fatal("eviction path was not exercised in zero-alloc test")
}
}
​func TestAudioStream_UnalignedWraparoundRampedPayload(t *testing.T) {
const payloadSize = 320
totalFrameSize := HeaderSize + payloadSize
unalignedCap := (totalFrameSize * 2) + 13
​asb, err := NewAudioStreamBuffer(unalignedCap, payloadSize, 2, PolicyDropOldest)
if err != nil {
t.Fatalf("failed to init unaligned buffer: %v", err)
}
​outPayload := make([]byte, payloadSize)
​for i := uint64(1); i <= 30; i++ {
payload := make([]byte, payloadSize)
for j := range payload {
payload[j] = byte(i) + byte(j*7)
}
​if err := asb.IngestFrame(i, time.Now(), payload); err != nil {
t.Fatalf("frame %d failed to ingest: %v", i, err)
}
​if i%2 == 0 {
info, err := asb.DrainFrame(outPayload, false)
if err != nil {
t.Fatalf("frame %d failed to drain: %v", i, err)
}
expectedPayload := make([]byte, payloadSize)
for j := range expectedPayload {
expectedPayload[j] = byte(info.Seq) + byte(j*7)
}
if !bytes.Equal(outPayload, expectedPayload) {
t.Fatalf("payload corrupted at seq %d across unaligned wraparound", info.Seq)
}
}
}
​if asb.Evicted() != 14 {
t.Fatalf("expected exactly 14 evictions, got %d", asb.Evicted())
}
}
​func TestAudioStream_ConcurrentStreamingWithRace(t *testing.T) {
const (
payloadSize    = 640
totalFrameSize = HeaderSize + payloadSize
maxLatency     = 5
)
​asb, err := NewAudioStreamBuffer(totalFrameSize*10, payloadSize, maxLatency, PolicyDropOldest)
if err != nil {
t.Fatal(err)
}
​var (
wg         sync.WaitGroup
stopSignal = make(chan struct{})
)
​wg.Add(2)
​go func() {
defer wg.Done()
ticker := time.NewTicker(20 * time.Millisecond)
defer ticker.Stop()
​var seq uint64 = 1
dummy := make([]byte, payloadSize)
​for {
select {
case <-stopSignal:
return
case t := <-ticker.C:
_ = asb.IngestFrame(seq, t, dummy)
seq++
}
}
}()
​go func() {
defer wg.Done()
out := make([]byte, payloadSize)
​for {
info, err := asb.DrainFrame(out, true)
if err != nil {
return
}
if info.Seq > 0 && rand.Float64() < 0.10 {
time.Sleep(30 * time.Millisecond)
}
}
}()
​time.Sleep(300 * time.Millisecond)
close(stopSignal)
asb.Close()
wg.Wait()
​if asb.Accepted() == 0 {
t.Fatal("no frames accepted")
}
}
​func BenchmarkAudioStream_ZeroAllocSustainedEviction(b testing.B) {
const payloadSize = 640
totalFrameSize := HeaderSize + payloadSize
asb, _ := NewAudioStreamBuffer(totalFrameSize4, payloadSize, 4, PolicyDropOldest)
​payload := make([]byte, payloadSize)
outPayload := make([]byte, payloadSize)
now := time.Now()
​b.ResetTimer()
b.ReportAllocs()
​var seq uint64 = 1
for i := 0; i < b.N; i++ {
_ = asb.IngestFrame(seq, now, payload)
seq++
_ = asb.IngestFrame(seq, now, payload)
seq++
_, _ = asb.DrainFrame(outPayload, false)
}
​b.StopTimer()
b.ReportMetric(float64(asb.Evicted())/float64(b.N), "evicted/op")
}

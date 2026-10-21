# ringbuf: Bounded Zero-Alloc Ring Buffer in Go

A lightweight, thread-safe circular byte buffer designed for low-latency streaming pipelines (Voice AI agents, WebSockets, real-time audio chunk ingestion).

Eliminates heap allocation churn in the fast path to prevent GC pressure during high-throughput burst traffic.

---

## Benchmarks

Measurements conducted under Go 1.23 on Linux x86_64:

```text
goos: linux
goarch: amd64
pkg: [github.com/jo3di1970-lab/ringbuf](https://github.com/jo3di1970-lab/ringbuf)
cpu: AMD EPYC 7763 64-Core Processor
BenchmarkRingBuffer_WriteRead-8   34189201   28.4 ns/op   0 B/op   0 allocs/op
PASS
ok      [github.com/jo3di1970-lab/ringbuf](https://github.com/jo3di1970-lab/ringbuf)    1.24s

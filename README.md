# ringbuf: High-Throughput Bounded Ring Buffer in Go

A thread-safe circular byte buffer optimized for real-time audio frame ingestion (Voice AI gateways, LiveKit / Retell WebSockets).

Eliminates memory allocations and reduces lock-hold duration on high-throughput streaming pipelines.

---

## Benchmarks

Measurements executed on Go 1.23 under Linux x86_64:

```text
goos: linux
goarch: amd64
pkg: [github.com/jo3di1970-lab/ringbuf](https://github.com/jo3di1970-lab/ringbuf)
cpu: AMD EPYC 7763 64-Core Processor
BenchmarkRingBuffer_SingleThreaded-8           48291032   19.2 ns/op   0 B/op   0 allocs/op
BenchmarkRingBuffer_Concurrent_Contention-8    14209148   74.1 ns/op   0 B/op   0 allocs/op
PASS
ok      [github.com/jo3di1970-lab/ringbuf](https://github.com/jo3di1970-lab/ringbuf)       2.18s
```

* **Zero Fast-Path Allocations:** Backing array is allocated once at initialization (`0 allocs/op`).
* **Optimized Block Memory Copies:** Eliminates per-byte modulo iterations in favor of contiguous `copy()` calls (at most two memory transfers per cycle).
* **Guaranteed Concurrency Integrity:** Tested under concurrent producer-consumer loads with automated checksum verification.

---

## Architectural Guarantees & Constraints

1. **Input Validation:** Rejects `size <= 0` with `ErrInvalidSize` during initialization to eliminate divide-by-zero panics.
2. **Deterministic Backpressure (Drop vs Reject):**
   * Default contract returns `ErrBufferFull` immediately when available space is smaller than the incoming audio frame.
   * Prevents runaway unbounded memory consumption under socket read stalls.
3. **Contiguous Chunked Transfers:** Replaces `for i := 0; i < n; i++` with hardware-accelerated memory moves via Go's built-in `copy()`.

---

## Verification & Stress Testing

```bash
# Verify race conditions and memory safety
go test -v -race ./...

# Run allocation and contention benchmarks
go test -bench=. -benchmem -cpu=1,4,8
```

## License
MIT

// Package heaptest measures heap and allocation in tests and benchmarks.
//
// Every measurement reads process-wide runtime counters, so a test using it must not run in
// parallel with others.
package heaptest

import (
	"runtime"
	"testing"
)

// Live collects garbage and returns the heap that survived: the resident set at this instant.
func Live() uint64 {
	var ms runtime.MemStats

	runtime.GC()
	runtime.ReadMemStats(&ms)

	return ms.HeapAlloc
}

// Allocated returns the bytes allocated while fn ran. It does not collect first; a caller that
// needs a settled heap calls [runtime.GC] before it.
func Allocated(fn func()) uint64 {
	var before, after runtime.MemStats

	runtime.ReadMemStats(&before)
	fn()
	runtime.ReadMemStats(&after)

	return after.TotalAlloc - before.TotalAlloc
}

// InuseGrowth returns how far HeapInuse moved while fn ran, wrapping if it shrank. It does not
// collect first; a caller that needs a settled heap calls [runtime.GC] before it.
func InuseGrowth(fn func()) uint64 {
	var before, after runtime.MemStats

	runtime.ReadMemStats(&before)
	fn()
	runtime.ReadMemStats(&after)

	return after.HeapInuse - before.HeapInuse
}

// BytesPerOp runs fn as a benchmark and returns its allocated bytes per call. Over a benchmark's
// iteration count, [sync.Pool] refills after a collection divide away.
func BytesPerOp(fn func()) int64 {
	return testing.Benchmark(func(b *testing.B) {
		b.Helper()
		b.ReportAllocs()

		for b.Loop() {
			fn()
		}
	}).AllocedBytesPerOp()
}

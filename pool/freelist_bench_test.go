package pool_test

import (
	"runtime"
	"testing"

	"github.com/oteldb/storage/pool"
)

// bigBuffer mirrors the decode buffer shape: an int64 + float64 column whose capacity
// is what we want to preserve across GC. The slice is sized so capacity loss is the
// dominant cost — exactly the chunk.resize pathology.
type bigBuffer struct {
	ts   []int64
	vals []float64
}

func newBigBuffer(rows int) *bigBuffer {
	return &bigBuffer{ts: make([]int64, 0, rows), vals: make([]float64, 0, rows)}
}

// BenchmarkFreeListUnderGC measures decoder-buffer reuse across collections.
func BenchmarkFreeListUnderGC(b *testing.B) {
	const rows = 60_000

	fl := pool.NewFreeList[bigBuffer](4)
	fl.Put(newBigBuffer(rows)) // prime

	b.ReportAllocs()
	b.ResetTimer()

	for range b.N {
		buf := fl.Get()
		if buf == nil {
			buf = newBigBuffer(rows)
		}
		buf.ts = buf.ts[:0]
		buf.vals = buf.vals[:0]
		buf.ts = append(buf.ts, make([]int64, rows)...)
		buf.vals = append(buf.vals, make([]float64, rows)...)
		fl.Put(buf)
		runtime.GC()
	}
}

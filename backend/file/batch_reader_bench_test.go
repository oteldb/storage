package file

import (
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/oteldb/storage/internal/readerpool"
	"github.com/oteldb/storage/internal/vfs"
)

// BenchmarkBatchRangeReads measures independent callers sharing the process-wide pool.
func BenchmarkBatchRangeReads(b *testing.B) {
	for _, size := range []int{24, 32 << 10} {
		for _, batch := range []bool{false, true} {
			b.Run(fmt.Sprintf("bytes=%d/batch=%t", size, batch), func(b *testing.B) { benchmarkBatchRangeReads(b, size, batch) })
		}
	}
}

func benchmarkBatchRangeReads(b *testing.B, size int, retain bool) {
	b.Helper()
	f, err := New(b.TempDir())
	if err != nil {
		b.Fatal(err)
	}
	names := []string{"part/c/0", "part/c/1", "part/sidx", "part/smax"}
	for _, name := range names {
		if err := f.Write(b.Context(), name, make([]byte, 64<<10)); err != nil {
			b.Fatal(err)
		}
	}
	var opens atomic.Int64
	originalOpen := f.open
	f.open = func() (vfs.FS, error) { opens.Add(1); return originalOpen() }
	const reads = 64
	b.ReportAllocs()
	b.SetBytes(int64(reads * size))
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		scope := readerpool.NewScope()
		ctx := b.Context()
		var batch *readerpool.Batch
		if retain {
			ctx, batch = readerpool.WithBatch(ctx)
		}
		ctx = scope.Context(ctx)
		defer func() {
			if batch != nil {
				_ = batch.Close()
			}
			_ = scope.Close()
		}()
		var dst []byte
		for _, name := range names {
			var err error
			dst, err = f.ReadAtAppend(ctx, dst[:0], name, 0, int64(size))
			if err != nil {
				b.Error(err)
				return
			}
		}
		for pb.Next() {
			for i := range reads {
				var err error
				dst, err = f.ReadAtAppend(ctx, dst[:0], names[i%len(names)], int64(i%16), int64(size))
				if err != nil || len(dst) != size {
					b.Errorf("range read: %d %v", len(dst), err)
					return
				}
			}
		}
	})
	b.ReportMetric(float64(opens.Load())/float64(b.N), "root-opens/op")
	b.ReportMetric(reads, "reads/op")
}

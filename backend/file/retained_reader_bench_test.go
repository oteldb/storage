package file

import (
	"fmt"
	"testing"

	"github.com/oteldb/storage/internal/readerpool"
	"github.com/oteldb/storage/internal/vfs"
)

func BenchmarkImmutableRangeReads(b *testing.B) {
	for _, size := range []int{8 << 10, 64 << 10} {
		b.Run(fmt.Sprintf("bytes=%d", size), func(b *testing.B) {
			f, err := New(b.TempDir())
			if err != nil {
				b.Fatal(err)
			}
			if err := f.Write(b.Context(), "part/c/0", make([]byte, size)); err != nil {
				b.Fatal(err)
			}
			scope := readerpool.NewScope()
			b.Cleanup(func() { _ = scope.Close() })
			ctx := scope.Context(b.Context())
			open := f.open
			opens := 0
			f.open = func() (vfs.FS, error) { opens++; return open() }
			dst := make([]byte, 0, size)
			dst, err = f.ReadAtAppend(ctx, dst[:0], "part/c/0", 0, int64(size))
			if err != nil {
				b.Fatal(err)
			}
			opens = 0
			b.ReportAllocs()
			b.SetBytes(int64(size))
			for b.Loop() {
				dst, err = f.ReadAtAppend(ctx, dst[:0], "part/c/0", 0, int64(size))
				if err != nil {
					b.Fatal(err)
				}
			}
			b.ReportMetric(float64(opens)/float64(b.N), "root-opens/op")
		})
	}
}

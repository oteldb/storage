package file_test

import (
	"fmt"
	"testing"

	"github.com/oteldb/storage/backend/file"
)

func BenchmarkReusableRangeRead(b *testing.B) {
	for _, size := range []int{8 << 10, 64 << 10} {
		for _, mode := range []string{"legacy", "append", "opened"} {
			b.Run(fmt.Sprintf("%d/%s", size, mode), func(b *testing.B) {
				f, err := file.New(b.TempDir())
				if err != nil {
					b.Fatal(err)
				}
				if err := f.Write(b.Context(), "object", make([]byte, size)); err != nil {
					b.Fatal(err)
				}
				buf := make([]byte, 0, size)
				r, _, err := f.OpenReader(b.Context(), "object")
				if err != nil {
					b.Fatal(err)
				}
				b.Cleanup(func() { _ = r.Close() })
				b.SetBytes(int64(size))
				b.ReportAllocs()
				for b.Loop() {
					switch mode {
					case "legacy":
						buf, err = f.ReadAt(b.Context(), "object", 0, int64(size))
					case "append":
						buf, err = f.ReadAtAppend(b.Context(), buf[:0], "object", 0, int64(size))
					case "opened":
						var n int
						n, err = r.ReadAt(buf[:size], 0)
						if n != size {
							b.Fatal(n)
						}
					}
					if err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}

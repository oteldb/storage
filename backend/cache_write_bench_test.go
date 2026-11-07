package backend_test

import (
	"strconv"
	"testing"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/backend/file"
)

func BenchmarkCacheFileWrite(b *testing.B) {
	for _, size := range []int{64 << 10, 1 << 20} {
		b.Run(strconv.Itoa(size), func(b *testing.B) {
			for _, read := range []bool{false, true} {
				name := "write-only"
				if read {
					name = "write-read"
				}
				b.Run(name, func(b *testing.B) {
					files, err := file.New(b.TempDir())
					if err != nil {
						b.Fatal(err)
					}
					c := backend.Cached(files, 8<<20)
					data := make([]byte, size)
					b.SetBytes(int64(size))
					b.ReportAllocs()
					for b.Loop() {
						if err := backend.WriteDeferred(b.Context(), c, "part/index", data); err != nil {
							b.Fatal(err)
						}
						if read {
							if _, err := backend.ReadView(b.Context(), c, "part/index"); err != nil {
								b.Fatal(err)
							}
						}
					}
				})
			}
		})
	}
}

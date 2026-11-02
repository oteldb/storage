package file_test

import (
	"testing"

	"github.com/oteldb/storage/backend/file"
)

func BenchmarkReadAtPathDepth(b *testing.B) {
	for _, key := range []struct{ name, key string }{
		{"shallow", "column"},
		{"deep", "tenant/metrics/0000000000000001/c/timestamps"},
	} {
		b.Run(key.name, func(b *testing.B) {
			for _, tc := range []struct {
				name        string
				off, length int64
				want, size  int
			}{
				{"valid", 0, 128, 128, 4096},
				{"clamped", 4000, 128, 96, 4096},
				{"EOF", 4096, 128, 0, 4096},
				{"frame8KiB", 4096, 8 << 10, 8 << 10, 16 << 10},
			} {
				b.Run(tc.name, func(b *testing.B) {
					store, err := file.New(b.TempDir())
					if err != nil {
						b.Fatal(err)
					}
					if err := store.Write(b.Context(), key.key, make([]byte, tc.size)); err != nil {
						b.Fatal(err)
					}
					b.SetBytes(int64(tc.want))
					b.ReportAllocs()
					for b.Loop() {
						value, err := store.ReadAt(b.Context(), key.key, tc.off, tc.length)
						if err != nil {
							b.Fatal(err)
						}
						if len(value) != tc.want {
							b.Fatalf("read %d bytes, want %d", len(value), tc.want)
						}
					}
				})
			}
		})
	}
}

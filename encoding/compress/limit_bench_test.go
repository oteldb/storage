package compress

import (
	"testing"
)

// BenchmarkDecompressLimit compares the bounded decode against the unbounded one on a frame of
// highly repetitive ids and on a frame of text, each 64 KiB decoded, into a reused buffer.
func BenchmarkDecompressLimit(b *testing.B) {
	c := NewCompressor(AlgorithmZSTD, LevelDefault)

	ids := make([]byte, 64<<10)
	for i := range ids {
		ids[i] = byte((i / 7) % 32)
	}

	for _, tc := range []struct {
		name string
		data []byte
	}{
		{"ids", ids},
		{"text", makeRepetitive(64<<10, "the quick brown fox jumps over the lazy dog 0123456789 ")},
	} {
		src := c.Compress(nil, tc.data)

		b.Run(tc.name+"/unbounded", func(b *testing.B) {
			b.SetBytes(int64(len(tc.data)))
			b.ReportAllocs()

			var buf []byte

			for b.Loop() {
				var err error
				if buf, err = c.Decompress(buf[:0], src); err != nil {
					b.Fatal(err)
				}
			}
		})

		b.Run(tc.name+"/bounded", func(b *testing.B) {
			b.SetBytes(int64(len(tc.data)))
			b.ReportAllocs()

			var buf []byte

			for b.Loop() {
				var err error
				if buf, err = c.DecompressLimit(buf[:0], src, len(tc.data)); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

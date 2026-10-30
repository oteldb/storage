package block

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/oteldb/storage/encoding/compress"
)

func BenchmarkKeptFullFrameReuse(b *testing.B) {
	benchmarkKeptFrameReuse(b, 1<<20)
}

func BenchmarkKeptLargeFrameReuse(b *testing.B) {
	for _, size := range []int{6 << 20, 9 << 20} {
		b.Run(fmt.Sprintf("MiB=%d", size>>20), func(b *testing.B) {
			benchmarkKeptFrameReuse(b, size)
		})
	}
}

func benchmarkKeptFrameReuse(b *testing.B, size int) {
	b.Helper()
	c := compress.NewFrameCompressor(compress.AlgorithmZSTD, compress.LevelFast, size)
	src := bytes.Repeat([]byte{1, 3, 5, 7}, size/4)
	encoded := c.Compress(nil, src)
	dst, err := decompressKept(c, nil, encoded, int64(len(src)), true)
	if err != nil {
		b.Fatal(err)
	}
	b.SetBytes(int64(len(src)))
	b.ReportAllocs()
	for b.Loop() {
		dst, err = decompressKept(c, dst, encoded, int64(len(src)), true)
		if err != nil {
			b.Fatal(err)
		}
	}
}

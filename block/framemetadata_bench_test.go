package block

import (
	"context"
	"strconv"
	"testing"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/encoding/compress"
)

func BenchmarkFrameDirectoryMetadata(b *testing.B) {
	for _, frames := range []int{1024, 65536} {
		b.Run(strconv.Itoa(frames), func(b *testing.B) {
			comp := compress.NewFrameCompressor(compress.AlgorithmNone, 0, 64)
			input := []byte{1, 2, 3, 4}
			var retained int64
			b.ReportAllocs()
			for b.Loop() {
				a := newBlockAccum(comp, len(input))
				if err := a.attach(func() (backend.ObjectWriter, error) { return discardFrameWriter{}, nil }); err != nil {
					b.Fatal(err)
				}
				for range frames {
					if err := a.addGranule(func(dst []byte) ([]byte, error) { return append(dst, input...), nil }); err != nil {
						b.Fatal(err)
					}
				}
				retained += a.residentBytes()
				if _, _, err := a.finish(context.Background(), 1, nil); err != nil {
					b.Fatal(err)
				}
			}
			b.ReportMetric(float64(retained)/float64(b.N), "resident-before-finish-B/op")
		})
	}
}

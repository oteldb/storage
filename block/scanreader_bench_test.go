package block

import (
	"context"
	"fmt"
	"math/rand/v2"
	"testing"

	"github.com/oteldb/storage/backend/file"
	"github.com/oteldb/storage/encoding/chunk"
	"github.com/oteldb/storage/encoding/compress"
)

func BenchmarkFileColumnScan(b *testing.B) {
	benchmarkFileColumnScan(b, 128<<10, []int64{64 << 10})
}

func BenchmarkFileColumnScanWindows(b *testing.B) {
	benchmarkFileColumnScan(b, 1<<20, []int64{32 << 10, 64 << 10, 128 << 10, 256 << 10, 1 << 20, 4 << 20})
}

func benchmarkFileColumnScan(b *testing.B, rows int, windows []int64) {
	b.Helper()
	f, err := file.New(b.TempDir())
	if err != nil {
		b.Fatal(err)
	}
	values := make([]int64, rows)
	rng := rand.New(rand.NewPCG(137, 139))
	for i := range values {
		values[i] = int64(rng.Uint64())
	}
	w := NewPartWriter(WithGranuleSize(256), WithCompressBlockBytes(8<<10), WithCompression(compress.AlgorithmLZ4))
	if err := w.AddColumn(Column{Name: "values", Kind: KindInt64, Codec: chunk.CodecT64, Int64: values, Block: true}); err != nil {
		b.Fatal(err)
	}
	if err := WritePart(b.Context(), f, "part", w); err != nil {
		b.Fatal(err)
	}
	r, err := OpenPart(b.Context(), f, "part", NewReadCompressors())
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = r.Close() })
	for _, tc := range []struct {
		name string
		open func(context.Context, string, int64) (*Decoder, error)
	}{
		{"legacy", r.ColumnScan}, {"opened", r.OpenColumnScan},
	} {
		for _, window := range windows {
			b.Run(fmt.Sprintf("%s/window=%d", tc.name, window), func(b *testing.B) {
				var scratch []int64
				b.SetBytes(int64(len(values) * 8))
				b.ReportAllocs()
				for b.Loop() {
					d, err := tc.open(b.Context(), "values", window)
					if err != nil {
						b.Fatal(err)
					}
					for i := range d.NumBlocks() {
						scratch, err = d.DecodeInt64Into(i, scratch[:0])
						if err != nil {
							_ = d.Close()
							b.Fatal(err)
						}
						lo, _ := d.BlockSpan(i)
						if len(scratch) == 0 || scratch[0] != values[lo] {
							_ = d.Close()
							b.Fatal("decoded values changed")
						}
					}
					if err := d.Close(); err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}

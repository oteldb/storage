package block

import (
	"math/rand/v2"
	"testing"

	"github.com/oteldb/storage/backend/file"
	"github.com/oteldb/storage/encoding/chunk"
	"github.com/oteldb/storage/encoding/compress"
)

func BenchmarkSparseColumnReads(b *testing.B) {
	f, err := file.New(b.TempDir())
	if err != nil {
		b.Fatal(err)
	}
	const rows = 4096
	values := make([]int64, rows)
	rng := rand.New(rand.NewPCG(17, 23))
	for i := range values {
		values[i] = int64(rng.Uint64())
	}
	w := NewPartWriter(WithGranuleSize(128), WithCompressBlockBytes(1024), WithCompression(compress.AlgorithmZSTD), WithSizingStats())
	if err := w.AddColumn(Column{Name: "values", Kind: KindInt64, Codec: chunk.CodecT64, Int64: values, Block: true}); err != nil {
		b.Fatal(err)
	}
	if err := WritePart(b.Context(), f, "part", w); err != nil {
		b.Fatal(err)
	}
	r, err := OpenPart(b.Context(), f, "part")
	if err != nil {
		b.Fatal(err)
	}
	// Optional cleanup keeps this benchmark runnable on older PartReader implementations.
	if closer, ok := any(r).(interface{ Close() error }); ok {
		b.Cleanup(func() { _ = closer.Close() })
	}
	scratch := make([]int64, 0, 128)
	read := func() {
		d, err := r.ColumnBlocks(b.Context(), "values")
		if err != nil {
			b.Fatal(err)
		}
		for _, block := range []int{0, 23} {
			scratch, err = d.DecodeInt64Into(block, scratch[:0])
			if err != nil {
				b.Fatal(err)
			}
			if len(scratch) != 128 {
				b.Fatal(len(scratch))
			}
		}
	}
	read()
	b.ReportAllocs()
	b.SetBytes(2 * 128 * 8)
	for b.Loop() {
		read()
	}
}

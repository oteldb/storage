package block

import (
	"context"
	"fmt"
	"testing"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/encoding/chunk"
	"github.com/oteldb/storage/encoding/compress"
)

// BenchmarkManyPartReaders decodes one zstd column once from each of many open parts, the read path's
// shape over a store of many small parts. The readers share their owner's decompressors, as an
// engine's do, so a decoder is built once rather than once per part.
func BenchmarkManyPartReaders(b *testing.B) {
	const rows = 2 * defaultGranuleSize

	ctx := context.Background()

	for _, parts := range []int{64, 1024} {
		be := backend.Memory()
		readers := make([]*PartReader, parts)
		vals := make([]int64, rows)
		shared := NewReadCompressors()

		for p := range readers {
			for i := range vals {
				vals[i] = int64(p*rows + i*3)
			}

			w := NewPartWriter(WithCompression(compress.AlgorithmZSTD))
			if err := w.AddColumn(Column{Name: "v", Kind: KindInt64, Codec: chunk.CodecT64, Int64: vals, Block: true}); err != nil {
				b.Fatal(err)
			}

			prefix := fmt.Sprintf("p%d", p)
			if err := WritePart(ctx, be, prefix, w); err != nil {
				b.Fatal(err)
			}

			r, err := OpenPart(ctx, be, prefix, shared)
			if err != nil {
				b.Fatal(err)
			}

			readers[p] = r
		}

		b.Run(fmt.Sprintf("parts=%d", parts), func(b *testing.B) {
			b.SetBytes(int64(parts) * rows * 8)
			b.ReportAllocs()

			var dst []int64

			for b.Loop() {
				for _, r := range readers {
					col, err := r.Column(ctx, "v")
					if err != nil {
						b.Fatal(err)
					}

					if dst, err = col.Int64(dst[:0]); err != nil {
						b.Fatal(err)
					}
				}
			}
		})
	}
}

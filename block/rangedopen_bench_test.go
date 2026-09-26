package block

import (
	"context"
	"testing"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/backend/backendtest"
	"github.com/oteldb/storage/encoding/compress"
)

// BenchmarkColumnBlocksOpen is the single-granule query on a shared-dictionary column: open the
// column by range (its dictionary and directory) and decode one granule. Sized by that granule's
// values; readB/op and reads/op are what the open costs the backend.
func BenchmarkColumnBlocksOpen(b *testing.B) {
	const (
		granules = 64
		rows     = 1024
	)

	ctx := context.Background()

	for _, tc := range []struct {
		name string
		self map[int]bool
	}{
		{"all_shared", nil},
		{"mixed", map[int]bool{3: true, 17: true, 40: true}},
	} {
		vals := mixedSharedValues(granules, rows, tc.self, rows)
		counter := backendtest.NewSizedByteCounter(backend.Memory())

		w := NewPartWriter(WithGranuleSize(rows), WithCompression(compress.AlgorithmZSTD))
		if err := w.AddColumn(Column{Name: "attrs", Kind: KindBytes, Bytes: vals, Block: true}); err != nil {
			b.Fatal(err)
		}

		if err := WritePart(ctx, counter, "p", w); err != nil {
			b.Fatal(err)
		}

		r, err := OpenPart(ctx, counter, "p")
		if err != nil {
			b.Fatal(err)
		}

		const blk = granules / 2

		var logical int64
		for _, v := range vals[blk*rows : (blk+1)*rows] {
			logical += int64(len(v))
		}

		b.Run(tc.name, func(b *testing.B) {
			b.SetBytes(logical)
			b.ReportAllocs()
			counter.Reset()

			for b.Loop() {
				d, err := r.ColumnBlocks(ctx, "attrs")
				if err != nil {
					b.Fatal(err)
				}

				if _, _, err := d.DecodeBytesBlock(blk); err != nil {
					b.Fatal(err)
				}
			}

			b.ReportMetric(float64(counter.Bytes())/float64(b.N), "readB/op")
			b.ReportMetric(float64(counter.Reads())/float64(b.N), "reads/op")
		})
	}
}

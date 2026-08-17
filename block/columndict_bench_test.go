package block

import (
	"fmt"
	"testing"

	"github.com/oteldb/storage/encoding/compress"
)

// BenchmarkBuildColumnBytesForm compares the writer's inputs for one block-framed bytes column: blob
// is what a merge produces today by expanding a source part's dictionary, split is the dictionary it
// already held. The objects are byte-identical, so the difference is only the hashing the split form
// skips.
//
// The two shapes are the two populations a log part holds, and they exercise different code:
// attrs-like values repeat enough that every granule joins the column-wide shared dictionary, so the
// encode is [encodeSharedDictBytes] and the saving is its two hash probes per row; body-like values
// are near-unique, so every granule declines and self-encodes through the chunk codec, where the
// saving is that codec's own per-row probe.
func BenchmarkBuildColumnBytesForm(b *testing.B) {
	const (
		rows      = 1 << 16
		blockRows = 8192
	)

	for _, shape := range []struct {
		name string
		cell func(i int) []byte
	}{
		{
			name: "shared", // 512 distinct: every granule joins the shared dictionary
			cell: func(i int) []byte {
				return fmt.Appendf(nil, "svc=api pod=worker-%03d level=info handler=/v1/resource", i%512)
			},
		},
		{
			name: "selfencoded", // near-unique: every granule declines and self-encodes
			cell: func(i int) []byte {
				return fmt.Appendf(nil, "request %d completed in %dms for tenant %d", i, i%977, i*7)
			},
		},
	} {
		b.Run(shape.name, func(b *testing.B) {
			cells := make([][]byte, rows)
			for i := range cells {
				cells[i] = shape.cell(i)
			}

			blob := []byte{}
			offsets := make([]int32, 1, rows+1)

			for _, v := range cells {
				blob = append(blob, v...)
				offsets = append(offsets, int32(len(blob)))
			}

			entries, ids := splitBytesForm(cells)

			logical := int64(len(blob))
			comp := compress.NewCompressor(compress.AlgorithmNone, compress.LevelDefault)

			forms := []struct {
				name string
				col  Column
			}{
				{name: "blob", col: Column{BytesBlob: blob, BytesOffsets: offsets}},
				{name: "split", col: Column{BytesDict: entries, BytesIDs: ids}},
			}

			for i := range forms {
				b.Run(forms[i].name, func(b *testing.B) {
					c := forms[i].col
					c.Name, c.Kind, c.Block = "c", KindBytes, true

					b.SetBytes(logical)
					b.ReportAllocs()

					for b.Loop() {
						if _, _, err := buildColumn(c, comp, blockRows, defaultCompressBlockBytes); err != nil {
							b.Fatal(err)
						}
					}
				})
			}
		})
	}
}

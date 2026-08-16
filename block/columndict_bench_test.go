package block

import (
	"fmt"
	"testing"

	"github.com/oteldb/storage/encoding/compress"
)

// BenchmarkBuildColumnBytesForm compares the writer's inputs for one repetitive block-framed bytes
// column — the shape a log part's attributes or body column has. blob is what a merge produces today
// by expanding a source part's dictionary; split is the dictionary it already held. The objects are
// byte-identical, so the difference is the two per-row hash probes the shared-dictionary build pays
// on the flat input and skips on the split one.
func BenchmarkBuildColumnBytesForm(b *testing.B) {
	const (
		rows      = 1 << 16
		distinct  = 512
		blockRows = 8192
	)

	cells := make([][]byte, rows)
	for i := range cells {
		cells[i] = fmt.Appendf(nil, "svc=api pod=worker-%03d level=info handler=/v1/resource", i%distinct)
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

	for _, tc := range []struct {
		name string
		col  Column
	}{
		{name: "blob", col: Column{BytesBlob: blob, BytesOffsets: offsets}},
		{name: "split", col: Column{BytesDict: entries, BytesIDs: ids}},
	} {
		b.Run(tc.name, func(b *testing.B) {
			c := tc.col
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
}

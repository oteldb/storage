package block

import (
	"runtime"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/encoding/chunk"
)

// TestColumnRowsBoundDecode checks that a column whose stream states more rows than the part does is
// rejected against the part's count before the decoder sizes anything by the stream's. The streams
// here are small and honest; the reader is told the part has fewer rows, which is what a corrupt
// stream header claiming a huge count looks like from the other side.
//
//nolint:paralleltest // the allocation check reads process-wide counters a parallel sibling would skew
func TestColumnRowsBoundDecode(t *testing.T) {
	const rows = 64

	ids := make([]chunk.U128, rows)
	consts := make([]int64, rows)
	ramp := make([]int64, rows)

	for i := range rows {
		ids[i] = chunk.U128{Lo: uint64(i / 8)}
		consts[i] = 7
		ramp[i] = int64(i * i)
	}

	for _, tc := range []struct {
		name string
		col  Column
		read func(r *ColumnReader) error
	}{
		{"u128", Column{Name: "series", Kind: KindInt128, Int128: ids}, func(r *ColumnReader) error {
			_, err := r.ID128(nil)
			return err
		}},
		{"t64", Column{Name: "c", Kind: KindInt64, Codec: chunk.CodecT64, Int64: ramp}, func(r *ColumnReader) error {
			_, err := r.Int64(nil)
			return err
		}},
		{"t64/blocked", Column{Name: "c", Kind: KindInt64, Codec: chunk.CodecT64, Int64: ramp, Block: true}, func(r *ColumnReader) error {
			_, err := r.Int64(nil)
			return err
		}},
		{"t64/range", Column{Name: "c", Kind: KindInt64, Codec: chunk.CodecT64, Int64: ramp, Block: true}, func(r *ColumnReader) error {
			_, err := r.RangeInt64(nil, 48, 58)
			return err
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			desc, obj, err := buildColumn(tc.col, noneComp(), 16, defaultCompressBlockBytes)
			require.NoError(t, err)
			require.False(t, desc.Const)

			require.NoError(t, tc.read(newColumnReader(desc, obj, noneComp(), rows)), "the honest count decodes")

			var before, after runtime.MemStats

			runtime.ReadMemStats(&before)
			err = tc.read(newColumnReader(desc, obj, noneComp(), rows-5))
			runtime.ReadMemStats(&after)

			require.Error(t, err)
			require.Less(t, after.TotalAlloc-before.TotalAlloc, uint64(1<<20))
		})
	}
}

func TestBlockRowsAt(t *testing.T) {
	t.Parallel()

	dir := blockDir{blockRows: 16}

	for _, tc := range []struct{ rows, blk, want int }{
		{64, 0, 16},
		{64, 3, 16},
		{64, 4, 0},
		{70, 4, 6},
		{10, 0, 10},
		{0, 0, 0},
	} {
		require.Equalf(t, tc.want, blockRowsAt(dir, tc.rows, tc.blk), "rows %d blk %d", tc.rows, tc.blk)
	}
}

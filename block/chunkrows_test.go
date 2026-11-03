package block

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/encoding/chunk"
	"github.com/oteldb/storage/encoding/compress"
)

func TestIndependentGroupChunks(t *testing.T) {
	t.Parallel()
	for _, alg := range []compress.Algorithm{compress.AlgorithmNone, compress.AlgorithmZSTD} {
		t.Run(alg.String(), func(t *testing.T) {
			t.Parallel()
			ctx := t.Context()
			mem := backend.Memory()
			w := NewStreamWriterTo(ctx, mem, "p", WithIndependentGroups(), WithGranuleSize(64), WithSortKey("ts"), WithCompression(alg))
			defer w.Abort()
			require.NoError(t, w.AddColumn(Column{Name: "ts", Kind: KindInt64, Codec: chunk.CodecDoDScaled, Block: true}))
			require.NoError(t, w.AddColumn(Column{Name: "v", Kind: KindFloat64, Codec: chunk.CodecFloatGroups, Block: true}))
			var times []int64
			var values []float64
			var spans [][2]int
			for group, n := range []int{1, 128, 7, 200, 1} {
				start := len(times)
				for j := 0; j < n; j += 17 {
					end := min(j+17, n)
					ts := make([]int64, end-j)
					vs := make([]float64, end-j)
					for k := range ts {
						ts[k] = int64(j+k) * 15e9
						vs[k] = float64(group*1000 + j + k)
						if group == 4 {
							vs[k] = math.Copysign(0, -1)
						}
					}
					require.NoError(t, w.AppendInt64(0, ts))
					require.NoError(t, w.AppendFloat64Group(1, vs, j == 0))
					times = append(times, ts...)
					values = append(values, vs...)
				}
				for lo := start; lo < len(times); lo += 64 {
					spans = append(spans, [2]int{lo, min(lo+64, len(times))})
				}
				require.NoError(t, w.EndGroup())
				require.NoError(t, w.EndGroup())
			}
			require.NoError(t, WriteStreamPart(ctx, mem, "p", w))
			r, err := OpenPart(ctx, mem, "p")
			require.NoError(t, err)
			col, err := r.Column(ctx, "v")
			require.NoError(t, err)
			got, err := col.Float64(nil)
			require.NoError(t, err)
			requireFloatBits(t, values, got)
			got, err = col.RangeFloat64(nil, 2, 95)
			require.NoError(t, err)
			requireFloatBits(t, values[2:95], got)
			got, err = col.DecodeBlocksFloat64(nil, []int{2})
			require.NoError(t, err)
			requireFloatBits(t, values[spans[2][0]:spans[2][1]], got[spans[2][0]:spans[2][1]])
			d, err := r.ColumnBlocks(ctx, "v")
			require.NoError(t, err)
			require.Equal(t, len(spans), d.NumBlocks())
			for i, span := range spans {
				lo, hi := d.BlockSpan(i)
				actual := [2]int{lo, hi}
				require.Equal(t, span, actual)
				b, err := d.BlockForRow(lo)
				require.NoError(t, err)
				require.Equal(t, i, b)
				b, err = d.BlockForRow(hi - 1)
				require.NoError(t, err)
				require.Equal(t, i, b)
			}
			tc, err := r.Column(ctx, "ts")
			require.NoError(t, err)
			ts, err := tc.Int64(nil)
			require.NoError(t, err)
			require.Equal(t, times, ts)
			cursor, err := col.FloatCursor()
			require.NoError(t, err)
			for _, v := range values {
				got, err := cursor.Next()
				require.NoError(t, err)
				require.Equal(t, math.Float64bits(v), math.Float64bits(got))
			}
			extents, err := col.Frames()
			require.NoError(t, err)
			require.Len(t, extents, len(spans))
			for i, e := range extents {
				actual := [2]int{e.StartRow, e.EndRow}
				require.Equal(t, spans[i], actual)
			}
			timestamps, err := r.ColumnBlocks(ctx, "ts")
			require.NoError(t, err)
			for i, span := range spans {
				lo, hi, ok := timestamps.BlockTimeBounds(i)
				require.True(t, ok)
				require.Equal(t, times[span[0]], lo)
				require.Equal(t, times[span[1]-1], hi)
			}

			steady, _, ok := r.ScanBound(ctx, "v", 0)
			require.True(t, ok)
			scan, err := r.ColumnScan(ctx, "v", 0)
			require.NoError(t, err)
			require.GreaterOrEqual(t, steady, scan.ResidentBytes())
		})
	}
}

func TestIndependentTimeBoundsExtremes(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	be := backend.Memory()
	w := NewStreamWriterTo(ctx, be, "bounds", WithIndependentGroups(), WithGranuleSize(4), WithSortKey("ts"))
	require.NoError(t, w.AddColumn(Column{Name: "ts", Kind: KindInt64, Codec: chunk.CodecDoDScaled, Block: true}))
	groups := [][]int64{{math.MinInt64, math.MinInt64 + 1}, {-1, 0, math.MaxInt64}}
	for _, times := range groups {
		require.NoError(t, w.AppendInt64(0, times))
		require.NoError(t, w.EndGroup())
	}
	require.NoError(t, WriteStreamPart(ctx, be, "bounds", w))
	r, err := OpenPart(ctx, be, "bounds")
	require.NoError(t, err)
	d, err := r.ColumnBlocks(ctx, "ts")
	require.NoError(t, err)
	for i, want := range groups {
		lo, hi, ok := d.BlockTimeBounds(i)
		require.True(t, ok)
		require.Equal(t, want[0], lo)
		require.Equal(t, want[len(want)-1], hi)
	}
}

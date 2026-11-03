package block

import (
	"context"
	"math"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/backend/backendtest"
	"github.com/oteldb/storage/encoding/chunk"
	"github.com/oteldb/storage/encoding/compress"
)

func requireFloatBits(tb testing.TB, want, got []float64) {
	tb.Helper()
	require.Len(tb, got, len(want))
	for i, v := range want {
		require.Equal(tb, math.Float64bits(v), math.Float64bits(got[i]), "row %d", i)
	}
}

func TestGroupedColumnWriters(t *testing.T) {
	t.Parallel()
	values := []float64{1, 1, 2, 3, 4, math.Pi, math.E, math.Float64frombits(0x7ff0000000000002), math.Copysign(0, -1), 0, 99, 99, 99, 99, 99, 99, 99, 99, 99}
	ends := []int{1, 3, 5, 7, 9, len(values)}
	for _, alg := range []compress.Algorithm{compress.AlgorithmNone, compress.AlgorithmZSTD} {
		for _, split := range []int{1, 3, 4, 5, 7, 100} {
			t.Run(alg.String()+"/split"+string(rune('a'+split)), func(t *testing.T) {
				t.Parallel()
				opts := []PartOption{WithGranuleSize(4), WithCompressBlockBytes(32), WithCompression(alg), WithSizingStats()}
				buffered := NewPartWriter(opts...)
				require.NoError(t, buffered.AddColumn(Column{Name: "v", Kind: KindFloat64, Codec: chunk.CodecFloatGroups, Float64: values, FloatGroupEnds: ends, Block: true}))
				built, err := buffered.build()
				require.NoError(t, err)
				ctx := t.Context()
				mem := backend.Memory()
				require.NoError(t, built.write(ctx, mem, "batch"))
				stream := NewStreamWriterTo(ctx, mem, "stream", opts...)
				defer stream.Abort()
				require.NoError(t, stream.AddColumn(Column{Name: "v", Kind: KindFloat64, Codec: chunk.CodecFloatGroups, Block: true}))
				pos := 0
				for _, end := range ends {
					first := true
					require.NoError(t, stream.AppendFloat64Group(0, nil, true))
					for pos < end {
						hi := min(pos+split, end)
						require.NoError(t, stream.AppendFloat64Group(0, values[pos:hi], first))
						first = false
						pos = hi
					}
				}
				require.Positive(t, stream.ResidentBytes())
				require.NoError(t, WriteStreamPart(ctx, mem, "stream", stream))
				checkGroupedReads(t, ctx, mem, "batch", values)
				checkGroupedReads(t, ctx, mem, "stream", values)
				// Buffered and streamed granules use the same logical groups, regardless of append splits.
				a, err := OpenPart(ctx, mem, "batch")
				require.NoError(t, err)
				b, err := OpenPart(ctx, mem, "stream")
				require.NoError(t, err)
				ca, err := a.Column(ctx, "v")
				require.NoError(t, err)
				cb, err := b.Column(ctx, "v")
				require.NoError(t, err)
				da, err := ca.blockDir()
				require.NoError(t, err)
				db, err := cb.blockDir()
				require.NoError(t, err)
				sa, sb := newBlockStreams(da, ca.comp), newBlockStreams(db, cb.comp)
				for i := range da.nBlocks() {
					x, err := sa.granule(i)
					require.NoError(t, err)
					y, err := sb.granule(i)
					require.NoError(t, err)
					require.Equal(t, x, y)
				}
			})
		}
	}
}

func checkGroupedReads(t *testing.T, ctx context.Context, b backend.Backend, prefix string, want []float64) {
	t.Helper()
	r, err := OpenPart(ctx, b, prefix)
	require.NoError(t, err)
	c, err := r.Column(ctx, "v")
	require.NoError(t, err)
	got, err := c.Float64(nil)
	require.NoError(t, err)
	requireFloatBits(t, want, got)
	got, err = c.RangeFloat64(nil, 2, len(want)-1)
	require.NoError(t, err)
	requireFloatBits(t, want[2:len(want)-1], got)
	got, err = c.DecodeBlocksFloat64(nil, []int{0, 2})
	require.NoError(t, err)
	requireFloatBits(t, want[:4], got[:4])
	requireFloatBits(t, want[8:12], got[8:12])
	d, err := r.ColumnScan(ctx, "v", 64)
	require.NoError(t, err)
	for _, cursor := range []func() (chunk.FloatDecoder, error){c.FloatCursor, d.FloatCursor} {
		it, err := cursor()
		require.NoError(t, err)
		for _, v := range want {
			next, err := it.Next()
			require.NoError(t, err)
			require.Equal(t, math.Float64bits(v), math.Float64bits(next))
		}
		_, err = it.Next()
		require.Error(t, err)
	}
}

func TestGroupedColumnConstantAndValidation(t *testing.T) {
	t.Parallel()
	oversized := NewStreamWriter(WithGranuleSize(chunk.MaxFloatGroupRows + 1))
	require.Error(t, oversized.AddColumn(Column{Name: "v", Kind: KindFloat64, Codec: chunk.CodecFloatGroups, Block: true}))
	for _, c := range []Column{
		{Name: "v", Kind: KindInt64, Codec: chunk.CodecFloatGroups, Block: true},
		{Name: "v", Kind: KindFloat64, Codec: chunk.CodecFloatGroups},
		{Name: "v", Kind: KindFloat64, Codec: chunk.CodecFloatGroups, Block: true, AutoCodec: true},
		{Name: "v", Kind: KindFloat64, Codec: chunk.CodecFloatGroups, Block: true, FloatPrecisionBits: 20},
	} {
		w := NewStreamWriter()
		require.Error(t, w.AddColumn(c))
	}
	for _, ends := range [][]int{nil, {0}, {2}, {1, 1}} {
		w := NewPartWriter()
		require.NoError(t, w.AddColumn(Column{Name: "v", Kind: KindFloat64, Codec: chunk.CodecFloatGroups, Block: true, Float64: []float64{1}, FloatGroupEnds: ends}))
		_, err := w.build()
		require.Error(t, err)
	}
	ctx := t.Context()
	b := backendtest.NewStreamingMemory()
	w := NewStreamWriterTo(ctx, b, "constant", WithGranuleSize(4))
	require.NoError(t, w.AddColumn(Column{Name: "v", Kind: KindFloat64, Codec: chunk.CodecFloatGroups, Block: true}))
	require.NoError(t, w.AppendFloat64Group(0, make([]float64, 100), true))
	require.NoError(t, WriteStreamPart(ctx, b, "constant", w))
	r, err := OpenPart(ctx, b, "constant")
	require.NoError(t, err)
	d, ok := r.ColumnDescByName("v")
	require.True(t, ok)
	require.True(t, d.Const)
	keys, err := b.List(ctx, "constant/c/")
	require.NoError(t, err)
	require.Empty(t, keys)
}

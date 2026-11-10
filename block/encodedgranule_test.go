package block

import (
	"encoding/binary"
	"hash/crc32"
	"math"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/encoding/chunk"
	"github.com/oteldb/storage/encoding/compress"
)

func encodedTestWriter(t *testing.T, be backend.Backend, prefix string, alg compress.Algorithm) *StreamWriter {
	t.Helper()
	w := NewStreamWriterTo(t.Context(), be, prefix, WithIndependentGroups(), WithGranuleSize(4), WithSortKey("ts"), WithCompression(alg))
	t.Cleanup(w.Abort)
	require.NoError(t, w.AddColumn(Column{Name: "ts", Kind: KindInt64, Codec: chunk.CodecAdaptiveTimestamps, Block: true}))
	require.NoError(t, w.AddColumn(Column{Name: "v", Kind: KindFloat64, Codec: chunk.CodecFloatGroups, Block: true}))
	return w
}

func TestEncodedGranuleCopy(t *testing.T) {
	t.Parallel()
	for _, alg := range []compress.Algorithm{compress.AlgorithmNone, compress.AlgorithmZSTD, compress.AlgorithmLZ4} {
		t.Run(alg.String(), func(t *testing.T) {
			t.Parallel()
			be := backend.Memory()
			source := encodedTestWriter(t, be, "source", alg)
			groups := [][]float64{{math.Pi, math.Pi, math.Pi}, {0.001, 0.002, 0.003}, {0, math.Copysign(0, -1), math.Float64frombits(0x7ff8000000004321)}, {math.Inf(1), math.Inf(-1)}, {math.Float64frombits(0x7ff8000000001234), math.Float64frombits(0x7ff8000000005678)}}
			wantTS, want := []int64{-15}, []float64{-10}
			for _, values := range groups {
				ts := make([]int64, len(values))
				for i := range ts {
					ts[i] = int64(len(wantTS)-1+i) * 15
				}
				require.NoError(t, source.AppendInt64(0, ts))
				require.NoError(t, source.AppendFloat64Group(1, values, true))
				require.NoError(t, source.EndGroup())
				wantTS = append(wantTS, ts...)
				want = append(want, values...)
			}
			require.NoError(t, WriteStreamPart(t.Context(), be, "source", source))
			r, err := OpenPart(t.Context(), be, "source")
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, r.Close()) })
			times, err := r.ColumnScan(t.Context(), "ts", 1)
			require.NoError(t, err)
			defer times.Close()
			values, err := r.ColumnScan(t.Context(), "v", 1)
			require.NoError(t, err)
			defer values.Close()
			target := encodedTestWriter(t, be, "target", compress.AlgorithmZSTD)
			require.NoError(t, target.AppendInt64(0, wantTS[:1]))
			require.NoError(t, target.AppendFloat64Group(1, want[:1], true))
			require.NoError(t, target.EndGroup())
			for b := range times.NumBlocks() {
				ts, err := times.EncodedGranule(b)
				require.NoError(t, err)
				require.True(t, ts.Ordered)
				lo, hi := times.BlockSpan(b)
				require.Equal(t, wantTS[lo+1], ts.MinTime)
				require.Equal(t, wantTS[hi], ts.MaxTime)
				var visited []float64
				vs, err := values.VisitEncodedGranule(b, func(v float64) { visited = append(visited, v) })
				require.NoError(t, err)
				requireFloatBits(t, want[lo+1:hi+1], visited)
				require.NoError(t, target.AppendEncodedGranule(0, ts, GranuleStats{}))
				require.NoError(t, target.AppendEncodedGranule(1, vs, vs.FloatStats()))
			}
			require.NoError(t, times.Close())
			require.NoError(t, values.Close())
			_, err = values.EncodedGranule(0)
			require.Error(t, err)
			wantTS = append(wantTS, 1000)
			want = append(want, 10)
			require.NoError(t, target.AppendInt64(0, []int64{1000}))
			require.NoError(t, target.AppendFloat64Group(1, []float64{10}, true))
			require.NoError(t, WriteStreamPart(t.Context(), be, "target", target))
			gotPart, err := OpenPart(t.Context(), be, "target")
			require.NoError(t, err)
			defer gotPart.Close()
			vc, err := gotPart.Column(t.Context(), "v")
			require.NoError(t, err)
			got, err := vc.Float64(nil)
			require.NoError(t, err)
			requireFloatBits(t, want, got)
			tc, err := gotPart.Column(t.Context(), "ts")
			require.NoError(t, err)
			gotTS, err := tc.Int64(nil)
			require.NoError(t, err)
			require.Equal(t, wantTS, gotTS)
			desc, ok := gotPart.ColumnDescByName("v")
			require.True(t, ok)
			require.False(t, desc.Const)
			require.Equal(t, compress.AlgorithmZSTD, desc.Compress)
			require.True(t, math.IsInf(desc.MinFloat64, -1))
			require.True(t, math.IsInf(desc.MaxFloat64, 1))
		})
	}
}

func encodedDecoder(kind Kind, codec chunk.Codec, rows int, data []byte) *Decoder {
	comp := compress.NewCompressor(compress.AlgorithmNone, compress.LevelFast)
	frame := comp.Compress(nil, data)
	dir := blockDir{blockRows: rows, rowStarts: []int64{0, int64(rows)}, granules: 1, frameOff: []int32{0, int32(len(frame))}, frameRaw: []int32{int32(len(data))}, frameCRC: []uint32{crc32.Checksum(frame, castagnoli)}, data: frame}
	return &Decoder{kind: kind, codec: codec, rows: rows, streams: newBlockStreams(dir, comp)}
}

func TestEncodedGranuleRejectsCheckedMalformed(t *testing.T) {
	t.Parallel()
	gorilla := chunk.EncodeFloats(nil, []float64{math.Pi, math.E})
	for _, body := range [][]byte{gorilla[:len(gorilla)-1], append(append([]byte(nil), gorilla...), 0), {2, 0}} {
		data := []byte{1, 2, 1, byte(chunk.CodecGorilla), 2}
		data = binary.AppendUvarint(data, uint64(len(body)))
		data = append(data, body...)
		d := encodedDecoder(KindFloat64, chunk.CodecFloatGroups, 2, data)
		_, err := d.EncodedGranule(0)
		require.Error(t, err)
		_, err = d.VisitEncodedGranule(0, func(float64) {})
		require.Error(t, err)
	}
	ts, _ := chunk.EncodeTimestampsAdaptive(nil, []int64{1, 4, 9})
	d := encodedDecoder(KindInt64, chunk.CodecAdaptiveTimestamps, 3, ts)
	d.streams.dir.minKeys = []int64{0}
	d.streams.dir.maxKeys = []int64{9}
	_, err := d.EncodedGranule(0)
	require.ErrorIs(t, err, ErrCorrupt)
	d.streams.dir.minKeys[0] = 1
	g, err := d.EncodedGranule(0)
	require.NoError(t, err)
	g.Data[0] ^= 1
	w := encodedTestWriter(t, backend.Memory(), "reject", compress.AlgorithmNone)
	require.Error(t, w.AppendEncodedGranule(0, g, GranuleStats{}))
	require.Zero(t, w.Rows())
	require.Error(t, w.AppendEncodedGranule(0, EncodedGranule{Data: ts, Kind: KindInt64, Codec: chunk.CodecAdaptiveTimestamps, Rows: 3}, GranuleStats{}))
}

func TestEncodedFloatStats(t *testing.T) {
	t.Parallel()
	for _, values := range [][]float64{{1, 2}, {math.NaN(), 3, 2}, {math.NaN(), math.NaN()}, {math.Copysign(0, -1), 0}, {math.Inf(1), math.Inf(-1)}} {
		var enc chunk.FloatGroupEncoder
		data, err := enc.Encode(nil, values, []int{len(values)})
		require.NoError(t, err)
		d := encodedDecoder(KindFloat64, chunk.CodecFloatGroups, len(values), data)
		plain, err := d.EncodedGranule(0)
		require.NoError(t, err)
		visited, err := d.VisitEncodedGranule(0, func(float64) {})
		require.NoError(t, err)
		expected := &streamColumn{allSame: true}
		for _, v := range values {
			expected.trackFloat(v)
		}
		for _, g := range []EncodedGranule{plain, visited} {
			stats := g.FloatStats()
			require.Equal(t, math.Float64bits(expected.minF64), math.Float64bits(stats.MinFloat64))
			require.Equal(t, math.Float64bits(expected.maxF64), math.Float64bits(stats.MaxFloat64))
		}
	}
	c := &streamColumn{allSame: true}
	c.trackFloat(1)
	c.trackFloat(2)
	c.trackEncodedFloatStats(GranuleStats{MinFloat64: math.NaN(), MaxFloat64: math.NaN()})
	require.Equal(t, math.Float64bits(1), math.Float64bits(c.minF64))
	require.Equal(t, math.Float64bits(2), math.Float64bits(c.maxF64))
	require.False(t, c.allSame)
}

func TestBlockedCursorSkipToRow(t *testing.T) {
	t.Parallel()
	be := backend.Memory()
	w := encodedTestWriter(t, be, "skip", compress.AlgorithmLZ4)
	ts := []int64{1, 2, 3, 4, 5, 6, 7, 8, 9}
	vs := []float64{1, 2, 3, 4, 5, 6, 7, 8, 9}
	require.NoError(t, w.AppendInt64(0, ts))
	require.NoError(t, w.AppendFloat64Group(1, vs, true))
	require.NoError(t, WriteStreamPart(t.Context(), be, "skip", w))
	r, err := OpenPart(t.Context(), be, "skip")
	require.NoError(t, err)
	defer r.Close()
	for _, name := range []string{"ts", "v"} {
		d, err := r.ColumnScan(t.Context(), name, 1)
		require.NoError(t, err)
		if name == "ts" {
			cur, err := d.TsCursor()
			require.NoError(t, err)
			testSkipCursor(t, cur, ts)
		} else {
			cur, err := d.FloatCursor()
			require.NoError(t, err)
			testSkipCursor(t, cur, vs)
		}
		require.NoError(t, d.Close())
	}
}

func testSkipCursor[T any](t *testing.T, cur interface {
	Next() (T, error)
	Pos() int
}, want []T,
) {
	t.Helper()
	skip, ok := any(cur).(interface{ SkipToRow(row int) error })
	require.True(t, ok)
	got, err := cur.Next()
	require.NoError(t, err)
	require.Equal(t, want[0], got)
	require.Error(t, skip.SkipToRow(2))
	require.Equal(t, 1, cur.Pos())
	require.NoError(t, skip.SkipToRow(4))
	require.Equal(t, 4, cur.Pos())
	got, err = cur.Next()
	require.NoError(t, err)
	require.Equal(t, want[4], got)
	require.Error(t, skip.SkipToRow(4))
	require.NoError(t, skip.SkipToRow(8))
	got, err = cur.Next()
	require.NoError(t, err)
	require.Equal(t, want[8], got)
	require.NoError(t, skip.SkipToRow(len(want)))
	_, err = cur.Next()
	require.Error(t, err)
	require.Error(t, skip.SkipToRow(len(want)+1))
}

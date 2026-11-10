package block

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/encoding/chunk"
	"github.com/oteldb/storage/encoding/compress"
)

func TestEncodedRunCoalescing(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name    string
		groups  [][]int64
		spans   []int
		endEach bool
	}{
		{name: "regular and cap", groups: [][]int64{{1, 3}, {5, 7}, {9, 11}}, spans: []int{4, 2}},
		{name: "single row stride inference", groups: [][]int64{{1}, {3}, {5, 7}}, spans: []int{4}},
		{name: "single row then regular", groups: [][]int64{{1}, {3, 5}, {7}}, spans: []int{4}},
		{name: "stride mismatch", groups: [][]int64{{1, 3}, {5, 8}}, spans: []int{2, 2}},
		{name: "gap", groups: [][]int64{{1, 3}, {6, 8}}, spans: []int{2, 2}},
		{name: "irregular", groups: [][]int64{{1}, {3, 5, 8}, {10}}, spans: []int{1, 3, 1}},
		{name: "logical group boundary", groups: [][]int64{{1, 3}, {5, 7}}, spans: []int{2, 2}, endEach: true},
		{name: "wrapped stride", groups: [][]int64{{math.MinInt64, math.MaxInt64}, {1, 2}}, spans: []int{2, 2}},
		{name: "duplicates", groups: [][]int64{{1, 1}, {2, 2}}, spans: []int{2, 2}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			be := backend.Memory()
			w := encodedTestWriter(t, be, "joined", compress.AlgorithmLZ4)
			var wantTS []int64
			var want []float64
			bits := []float64{math.Copysign(0, -1), 0, math.Float64frombits(0x7ff8000000004321), math.Inf(1), math.Inf(-1), math.Pi}
			for _, ts := range tc.groups {
				vals := make([]float64, len(ts))
				for i := range vals {
					vals[i] = bits[(len(want)+i)%len(bits)]
				}
				encodedTS, err := chunk.EncodeTimestampsAdaptive(nil, ts)
				require.NoError(t, err)
				td := encodedDecoder(KindInt64, chunk.CodecAdaptiveTimestamps, len(ts), encodedTS)
				var enc chunk.FloatGroupEncoder
				data, err := enc.Encode(nil, vals, []int{len(vals)})
				require.NoError(t, err)
				vd := encodedDecoder(KindFloat64, chunk.CodecFloatGroups, len(vals), data)
				tg, err := td.EncodedGranule(0)
				require.NoError(t, err)
				vg, err := vd.VisitEncodedGranule(0, func(float64) {})
				require.NoError(t, err)
				before := w.ResidentBytes() + w.FinishBytes()
				require.NoError(t, w.AppendEncodedRun(0, 1, tg, vg, vg.FloatStats()))
				wantTS = append(wantTS, ts...)
				want = append(want, vals...)
				require.Equal(t, len(wantTS), w.Rows())
				// The paired append owns the bytes before it returns.
				for i := range tg.Data {
					tg.Data[i] = 0
				}
				for i := range vg.Data {
					vg.Data[i] = 0
				}
				require.Positive(t, before+w.ResidentBytes()+w.FinishBytes())
				if tc.endEach {
					require.NoError(t, w.EndGroup())
				}
			}
			// Finish flushes the last staged pair without requiring EndGroup.
			require.NoError(t, WriteStreamPart(t.Context(), be, "joined", w))
			r, err := OpenPart(t.Context(), be, "joined")
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, r.Close()) })
			tsCol, err := r.Column(t.Context(), "ts")
			require.NoError(t, err)
			gotTS, err := tsCol.Int64(nil)
			require.NoError(t, err)
			require.Equal(t, wantTS, gotTS)
			vCol, err := r.Column(t.Context(), "v")
			require.NoError(t, err)
			got, err := vCol.Float64(nil)
			require.NoError(t, err)
			requireFloatBits(t, want, got)
			for _, name := range []string{"ts", "v"} {
				d, err := r.ColumnBlocks(t.Context(), name)
				require.NoError(t, err)
				require.Equal(t, len(tc.spans), d.NumBlocks())
				for b, n := range tc.spans {
					lo, hi := d.BlockSpan(b)
					require.Equal(t, n, hi-lo)
				}
			}
		})
	}
}

func TestEncodedRunNormalMix(t *testing.T) {
	t.Parallel()
	for _, grouped := range []bool{false, true} {
		be := backend.Memory()
		w := encodedTestWriter(t, be, "mixed", compress.AlgorithmZSTD)
		ts, err := chunk.EncodeTimestampsAdaptive(nil, []int64{1, 2})
		require.NoError(t, err)
		td := encodedDecoder(KindInt64, chunk.CodecAdaptiveTimestamps, 2, ts)
		var enc chunk.FloatGroupEncoder
		floats, err := enc.Encode(nil, []float64{1, 2}, []int{2})
		require.NoError(t, err)
		vd := encodedDecoder(KindFloat64, chunk.CodecFloatGroups, 2, floats)
		tg, err := td.EncodedGranule(0)
		require.NoError(t, err)
		vg, err := vd.EncodedGranule(0)
		require.NoError(t, err)
		require.NoError(t, w.AppendEncodedRun(0, 1, tg, vg, vg.FloatStats()))
		if grouped {
			require.NoError(t, w.AppendFloat64Group(1, []float64{3}, true))
		} else {
			require.NoError(t, w.AppendFloat64(1, []float64{3}))
		}
		require.NoError(t, w.AppendInt64(0, []int64{3}))
		require.NoError(t, w.EndGroup())
		require.NoError(t, w.AppendEncodedRun(0, 1, tg, vg, vg.FloatStats()))
		require.NoError(t, WriteStreamPart(t.Context(), be, "mixed", w))
		r, err := OpenPart(t.Context(), be, "mixed")
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, r.Close()) })
		c, err := r.Column(t.Context(), "v")
		require.NoError(t, err)
		got, err := c.Float64(nil)
		require.NoError(t, err)
		require.Equal(t, []float64{1, 2, 3, 1, 2}, got)
		d, err := r.ColumnBlocks(t.Context(), "v")
		require.NoError(t, err)
		require.Equal(t, 3, d.NumBlocks())
	}
}

func TestEncodedBorrowMetadataMutation(t *testing.T) { //nolint:paralleltest // AllocsPerRun requires a sequential test.
	data, err := chunk.EncodeTimestampsAdaptive(nil, []int64{1, 2})
	require.NoError(t, err)
	d := encodedDecoder(KindInt64, chunk.CodecAdaptiveTimestamps, 2, data)
	g, err := d.EncodedGranule(0)
	require.NoError(t, err)
	for _, change := range []func(*EncodedGranule){
		func(g *EncodedGranule) { g.Kind = KindFloat64 }, func(g *EncodedGranule) { g.Codec = chunk.CodecDoD },
		func(g *EncodedGranule) { g.Rows = 1 }, func(g *EncodedGranule) { g.MinTime = 0 },
		func(g *EncodedGranule) { g.MaxTime = 3 }, func(g *EncodedGranule) { g.Ordered = false },
	} {
		changed := g
		change(&changed)
		require.False(t, changed.validBorrow())
	}
	require.Zero(t, testing.AllocsPerRun(100, func() {
		if !g.validBorrow() {
			panic("borrow changed")
		}
	}))
	require.Zero(t, testing.AllocsPerRun(100, func() {
		if _, err := d.EncodedGranule(0); err != nil {
			panic(err)
		}
	}))
}

func BenchmarkEncodedGranuleBorrow(b *testing.B) {
	data, err := chunk.EncodeTimestampsAdaptive(nil, []int64{1, 2, 3, 4})
	if err != nil {
		b.Fatal(err)
	}
	d := encodedDecoder(KindInt64, chunk.CodecAdaptiveTimestamps, 4, data)
	if _, err := d.EncodedGranule(0); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.SetBytes(4 * 8)
	for b.Loop() {
		if _, err := d.EncodedGranule(0); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkEncodedGranuleVisit(b *testing.B) {
	values := []float64{math.Pi, math.E, math.Copysign(0, -1), math.NaN()}
	var enc chunk.FloatGroupEncoder
	data, err := enc.Encode(nil, values, []int{len(values)})
	if err != nil {
		b.Fatal(err)
	}
	d := encodedDecoder(KindFloat64, chunk.CodecFloatGroups, len(values), data)
	if _, err := d.EncodedGranule(0); err != nil {
		b.Fatal(err)
	}
	count := 0
	visit := func(float64) { count++ }
	b.ReportAllocs()
	b.SetBytes(int64(len(values)) * 8)
	for b.Loop() {
		if _, err := d.VisitEncodedGranule(0, visit); err != nil {
			b.Fatal(err)
		}
	}
	if count == 0 {
		b.Fatal("no visits")
	}
}

func FuzzEncodedRun(f *testing.F) {
	f.Add([]byte{1, 3, 3, 7, 8, 12, 255, 0})
	f.Add([]byte{0, 0, 0, 0, 0, 0, 0, 0})
	f.Fuzz(func(t *testing.T, input []byte) {
		if len(input) == 0 || len(input) > 128 {
			t.Skip()
		}
		be := backend.Memory()
		w := encodedTestWriter(t, be, "fuzz", compress.AlgorithmNone)
		var wantTS []int64
		var want []float64
		for group := range min(len(input), 12) {
			size := int(input[group]%4) + 1
			ts := make([]int64, size)
			values := make([]float64, size)
			for i := range ts {
				ts[i] = int64(len(wantTS)+i) * 15
				if input[group]&16 != 0 {
					ts[i] += int64(i * i)
				}
				var bits uint64
				for j := range 8 {
					bits = bits<<8 | uint64(input[(group+i+j)%len(input)])
				}
				values[i] = math.Float64frombits(bits)
			}
			data, err := chunk.EncodeTimestampsAdaptive(nil, ts)
			require.NoError(t, err)
			td := encodedDecoder(KindInt64, chunk.CodecAdaptiveTimestamps, size, data)
			var enc chunk.FloatGroupEncoder
			data, err = enc.Encode(nil, values, []int{size})
			require.NoError(t, err)
			vd := encodedDecoder(KindFloat64, chunk.CodecFloatGroups, size, data)
			tg, err := td.EncodedGranule(0)
			require.NoError(t, err)
			vg, err := vd.VisitEncodedGranule(0, func(float64) {})
			require.NoError(t, err)
			require.NoError(t, w.AppendEncodedRun(0, 1, tg, vg, vg.FloatStats()))
			if input[group]&32 != 0 {
				require.NoError(t, w.EndGroup())
			}
			wantTS = append(wantTS, ts...)
			want = append(want, values...)
		}
		require.NoError(t, WriteStreamPart(t.Context(), be, "fuzz", w))
		r, err := OpenPart(t.Context(), be, "fuzz")
		require.NoError(t, err)
		defer r.Close()
		c, err := r.Column(t.Context(), "ts")
		require.NoError(t, err)
		gotTS, err := c.Int64(nil)
		require.NoError(t, err)
		require.Equal(t, wantTS, gotTS)
		c, err = r.Column(t.Context(), "v")
		require.NoError(t, err)
		got, err := c.Float64(nil)
		require.NoError(t, err)
		requireFloatBits(t, want, got)
	})
}

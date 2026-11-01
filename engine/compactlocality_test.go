package engine

import (
	"math"
	"slices"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend/file"
	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/signal"
	"github.com/oteldb/storage/wal"
)

func localityPart(first, last uint64, bytes int64) *part {
	return &part{diskBytes: bytes, index: partIndex{ids: []signal.SeriesID{{Lo: first}, {Lo: last}}}}
}

func TestSeriesRangeSpan(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		name        string
		first, last signal.SeriesID
		want        float64
	}{
		{name: "singleton", want: 1},
		{name: "low", first: signal.SeriesID{Lo: 100}, last: signal.SeriesID{Lo: 104}, want: 5},
		{name: "borrow", first: signal.SeriesID{Lo: math.MaxUint64}, last: signal.SeriesID{Hi: 1}, want: 2},
		{name: "high", last: signal.SeriesID{Hi: 1}, want: math.Ldexp(1, 64)},
		{name: "full", last: signal.SeriesID{Hi: math.MaxUint64, Lo: math.MaxUint64}, want: math.Ldexp(1, 128)},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			require.InEpsilon(t, tt.want, seriesRangeSpan(tt.first, tt.last), 1e-15)
		})
	}
}

func TestPreferSeriesLocalRun(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		name   string
		change func(current, local []*part) (eligible []*part, capBytes int64, prefer bool)
	}{
		{name: "overlap", change: func(current, local []*part) ([]*part, int64, bool) {
			return append(current, local...), 0, true
		}},
		{name: "byte balance", change: func(current, local []*part) ([]*part, int64, bool) {
			local[3].diskBytes = 200
			return append(current, local...), 0, false
		}},
		{name: "disk cap", change: func(current, local []*part) ([]*part, int64, bool) {
			for _, p := range local {
				p.diskBytes = 110
			}
			return append(current, local...), 400, false
		}},
		{name: "unknown bounds", change: func(current, local []*part) ([]*part, int64, bool) {
			current[0].index = partIndex{paged: &pagedIndex{n: 2}}
			return append(current, local...), 0, false
		}},
		{name: "inverted bounds", change: func(current, local []*part) ([]*part, int64, bool) {
			current[0].index.ids[0].Lo = 100000
			return append(current, local...), 0, false
		}},
		{name: "disjoint", change: func(current, local []*part) ([]*part, int64, bool) {
			for i, p := range local {
				p.index.ids = []signal.SeriesID{{Lo: uint64(i * 10)}, {Lo: uint64(i*10 + 9)}}
			}
			return append(current, local...), 0, false
		}},
		{name: "already local", change: func(current, local []*part) ([]*part, int64, bool) {
			for _, p := range current {
				p.index.ids = []signal.SeriesID{{Lo: 1000}, {Lo: 1099}}
			}
			return append(current, local...), 0, false
		}},
		{name: "paged bounds", change: func(current, local []*part) ([]*part, int64, bool) {
			for _, p := range local {
				p.index = partIndex{paged: &pagedIndex{bounded: true, n: 2, first: signal.SeriesID{Lo: 10}, last: signal.SeriesID{Lo: 19}}}
			}
			return append(current, local...), 0, true
		}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			current := []*part{
				localityPart(1000, 1099, 100), localityPart(1100, 1199, 100),
				localityPart(1200, 1299, 100), localityPart(1300, 1399, 100),
			}
			local := []*part{
				localityPart(10, 19, 100), localityPart(10, 19, 100),
				localityPart(10, 19, 100), localityPart(10, 19, 100),
			}
			eligible, capBytes, prefer := tt.change(current, local)
			got := preferSeriesLocalRun(eligible, current, capBytes)
			want := current
			if prefer {
				want = local
			}
			require.Equal(t, want, got)
		})
	}
}

func TestLocalMergeKeepsSourceOrder(t *testing.T) {
	t.Parallel()
	parts := make([]*part, 0, 32)
	for i := range 32 {
		p := localityPart(uint64(i%2*100), uint64(i%2*100+9), 1<<20)
		p.prefix = strconv.Itoa(i)
		parts = append(parts, p)
	}
	got := pickMergeRun(parts, 0, 0)
	require.Len(t, got, maxMergeParts)
	positions := make(map[*part]int, len(parts))
	for i, p := range parts {
		positions[p] = i
	}
	require.True(t, slices.IsSortedFunc(got, func(a, b *part) int { return positions[a] - positions[b] }))
	for _, p := range got {
		require.Equal(t, got[0].index.ids, p.index.ids)
	}
}

func populateLocalityParts(tb testing.TB, e *Engine, perRange int) []signal.SeriesID {
	tb.Helper()
	const ranges, rounds = 8, 8
	identities := make([]signal.Series, ranges*perRange)
	for i := range identities {
		identities[i] = orderBenchSeries(i)
	}
	slices.SortFunc(identities, func(a, b signal.Series) int { return a.Hash().Compare(b.Hash()) })
	ids := make([]signal.SeriesID, len(identities))
	for i := range identities {
		ids[i] = identities[i].Hash()
	}
	ts, values := make([]int64, perRange), make([]float64, perRange)
	for round := range rounds {
		for i := range ts {
			ts[i], values[i] = int64(100+round), float64(round)
		}
		for bucket := range ranges {
			lo, hi := bucket*perRange, (bucket+1)*perRange
			result, err := e.AppendBatch(ids[lo:hi], ts, values, nil,
				func(i int) signal.Series { return identities[lo+i] }, AppendLimits{})
			if err != nil || result.Accepted != perRange {
				tb.Fatalf("append: accepted=%d, err=%v", result.Accepted, err)
			}
			if err := e.Flush(tb.Context()); err != nil {
				tb.Fatal(err)
			}
		}
	}
	return ids
}

func TestSeriesLocalCompactionFileWALReopen(t *testing.T) {
	t.Parallel()
	files, err := file.New(t.TempDir())
	require.NoError(t, err)
	dir := t.TempDir()
	w, err := wal.Create(dir, 0)
	require.NoError(t, err)
	cfg := Config{Backend: files, WAL: w, Prefix: "metrics", AggregateStats: true}
	e := New(cfg)
	t.Cleanup(func() { require.NoError(t, e.CloseWAL()) })
	const perRange = 64
	ids := populateLocalityParts(t, e, perRange)
	require.Len(t, e.parts, 64)
	selected := selectMergeParts(e.parts, MergeOptions{}, e.mergeCapBytes(t.Context()), 0)
	require.Len(t, selected, maxMergeParts)
	unique := make(map[signal.SeriesID]struct{})
	for _, p := range selected {
		require.NoError(t, p.index.forEachID(t.Context(), func(id signal.SeriesID) { unique[id] = struct{}{} }))
	}
	require.LessOrEqual(t, len(unique), 3*perRange, "selection combines repeated ranges across flushes")
	changed, err := e.MergeStep(t.Context(), MergeOptions{})
	require.NoError(t, err)
	require.True(t, changed)
	require.Len(t, e.parts, 49)
	assertRows := func(e *Engine) {
		it, err := e.Fetch(t.Context(), fetch.Request{Start: 0, End: 1000})
		require.NoError(t, err)
		rows, err := fetch.Drain(t.Context(), it)
		require.NoError(t, err)
		require.Len(t, rows, len(ids))
		for _, row := range rows {
			require.Equal(t, []int64{100, 101, 102, 103, 104, 105, 106, 107}, row.Timestamps)
			require.Equal(t, []float64{0, 1, 2, 3, 4, 5, 6, 7}, row.Values)
		}
	}
	assertRows(e)
	require.NoError(t, e.CloseWAL())
	resumed, err := wal.Create(dir, 0)
	require.NoError(t, err)
	cfg.WAL = resumed
	restored := New(cfg)
	t.Cleanup(func() { require.NoError(t, restored.CloseWAL()) })
	require.NoError(t, restored.LoadParts(t.Context()))
	require.NoError(t, restored.Replay(t.Context(), dir))
	assertRows(restored)
}

func FuzzSeriesLocalMergeSelection(f *testing.F) {
	f.Add([]byte{1, 4, 7, 2, 8, 4, 3, 2, 5})
	f.Add([]byte{255, 0, 255, 8, 1, 1, 8, 0, 0, 8, 1, 0, 8})
	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) < 4 {
			return
		}
		parts := make([]*part, 0, min((len(data)-1)/3, 128))
		for i := 1; i+2 < len(data) && len(parts) < cap(parts); i += 3 {
			first := uint64(data[i]) * 100
			parts = append(parts, localityPart(first, first+uint64(data[i+1]), (int64(data[i+2])+1)<<20))
		}
		capBytes := int64(data[0]) << 20
		selected := pickMergeRun(parts, capBytes, 0)
		if len(selected) == 0 {
			return
		}
		if len(selected) < minMergeParts || len(selected) > maxMergeParts {
			t.Fatalf("invalid input count: %d", len(selected))
		}
		positions := make(map[*part]int, len(parts))
		for i, p := range parts {
			positions[p] = i
		}
		previous, bytes := -1, int64(0)
		for _, p := range selected {
			position, ok := positions[p]
			if !ok || position <= previous || sealed(p, capBytes) {
				t.Fatal("selection changed source order, repeated an input, or selected a sealed part")
			}
			previous = position
			bytes += p.sizeBytes()
		}
		if capBytes > 0 && bytes > capBytes {
			t.Fatalf("selection exceeds byte cap: %d > %d", bytes, capBytes)
		}
	})
}

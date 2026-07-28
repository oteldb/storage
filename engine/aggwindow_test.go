package engine_test

import (
	"context"
	"fmt"
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/engine"
	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/signal"
)

// anyMatcher selects every series carrying name, whatever its value.
func anyMatcher(name string) fetch.Matcher {
	return fetch.Matcher{
		Name:  []byte(name),
		Match: func(v signal.Value) bool { return v.Kind() != signal.KindEmpty },
	}
}

// ceilTo rounds ts up to a multiple of step (the test's own copy, so the reference fold shares no
// arithmetic with the implementation it checks).
func ceilTo(ts, step int64) int64 {
	r := ts % step
	if r < 0 {
		r += step
	}

	if r == 0 {
		return ts
	}

	return ts - r + step
}

// windowFold is the brute-force reference: for every step-aligned evaluation timestamp t, fold the
// samples in the half-open window (t-window, t]. Windows past the request end are not evaluated —
// their data is not all fetched.
func windowFold(b *fetch.Batch, step, window, end int64) []engine.WindowAgg {
	if len(b.Timestamps) == 0 {
		return nil
	}

	lo, hi := b.Timestamps[0], b.Timestamps[0]
	for _, ts := range b.Timestamps {
		lo, hi = min(lo, ts), max(hi, ts)
	}

	var out []engine.WindowAgg

	last := min(end, hi+window-1)
	for t := ceilTo(lo, step); t <= last; t += step {
		var a engine.SeriesAgg

		for i, ts := range b.Timestamps {
			if ts > t-window && ts <= t {
				a.Count++
				a.Sum += b.Values[i]
			}
		}

		if a.Count > 0 {
			out = append(out, engine.WindowAgg{
				End:       t,
				SeriesAgg: engine.SeriesAgg{Count: a.Count, Sum: a.Sum, Min: math.NaN(), Max: math.NaN()},
			})
		}
	}

	return out
}

func windowsFromBatches(batches []*fetch.Batch, step, window, end int64) map[signal.SeriesID][]engine.WindowAgg {
	out := make(map[signal.SeriesID][]engine.WindowAgg, len(batches))
	for _, b := range batches {
		if w := windowFold(b, step, window, end); len(w) > 0 {
			out[b.ID] = w
		}
	}

	return out
}

// assertWindows compares window-for-window. Min/Max are compared structurally by neither: they are
// NaN by contract, and NaN is not equal to itself.
func assertWindows(t *testing.T, want, got []engine.WindowAgg, msg string) {
	t.Helper()

	require.Lenf(t, got, len(want), "%s: window count", msg)

	for i := range want {
		assert.Equalf(t, want[i].End, got[i].End, "%s: window %d end", msg, i)
		assert.Equalf(t, want[i].Count, got[i].Count, "%s: window %d count", msg, i)
		assert.InDeltaf(t, want[i].Sum, got[i].Sum, 1e-9, "%s: window %d sum", msg, i)
		assert.Truef(t, math.IsNaN(got[i].Min), "%s: window %d min must be NaN", msg, i)
		assert.Truef(t, math.IsNaN(got[i].Max), "%s: window %d max must be NaN", msg, i)
	}
}

func assertWindowsMatchFetch(t *testing.T, e *engine.Engine, r fetch.Request, step, window int64) {
	t.Helper()

	got, err := e.AggregateWindow(context.Background(), r, step, window)
	require.NoError(t, err)

	want := windowsFromBatches(fetchAll(t, e, r), step, window, r.End)
	require.Len(t, got, len(want))

	for id, w := range want {
		assertWindows(t, w, got[id], fmt.Sprintf("step=%d window=%d series=%v", step, window, id))
	}
}

// windowEngine holds three flushed parts — two wholly inside a 100-wide fine bucket (the sidecar
// pushdown) and one straddling a boundary (decode) — plus unflushed head data, across two series.
func windowEngine(t *testing.T) (*engine.Engine, fetch.Request) {
	t.Helper()

	ctx := context.Background()
	e := aggEngine()
	api, web := mkSeries("job", "api"), mkSeries("job", "web")
	both := []*signal.Series{&api, &web}

	for _, s := range both {
		mustAppend(t, e, *s, 10, 1)
		mustAppend(t, e, *s, 40, 3)
	}

	require.NoError(t, e.Flush(ctx)) // ⊂ bucket (0,100]

	for _, s := range both {
		mustAppend(t, e, *s, 150, 5)
		mustAppend(t, e, *s, 180, 7)
	}

	require.NoError(t, e.Flush(ctx)) // ⊂ bucket (100,200]

	for _, s := range both {
		mustAppend(t, e, *s, 290, 9)
		mustAppend(t, e, *s, 320, 11)
	}

	require.NoError(t, e.Flush(ctx)) // straddles (200,300] and (300,400]

	mustAppend(t, e, api, 410, 13) // head only for api

	return e, fetch.Request{Start: 0, End: 1000, Matchers: []fetch.Matcher{anyMatcher("job")}}
}

func TestAggregateWindowMatchesFetch(t *testing.T) {
	t.Parallel()

	e, req := windowEngine(t)

	for _, tc := range []struct{ step, window int64 }{
		{100, 100}, // no overlap: the disjoint case
		{100, 200}, // 2x
		{50, 200},  // 4x
		{25, 500},  // 20x
		{100, 250}, // misaligned ⇒ per-sample fallback
		{30, 100},  // misaligned, window not a multiple of a finer step either
	} {
		assertWindowsMatchFetch(t, e, req, tc.step, tc.window)
	}
}

// TestAggregateWindowDefaultsToStep pins window ≤ 0 as "no overlap".
func TestAggregateWindowDefaultsToStep(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	e, req := windowEngine(t)

	want, err := e.AggregateWindow(ctx, req, 100, 100)
	require.NoError(t, err)

	for _, window := range []int64{0, -1} {
		got, err := e.AggregateWindow(ctx, req, 100, window)
		require.NoError(t, err)
		require.Len(t, got, len(want))

		for id, w := range want {
			assertWindows(t, w, got[id], fmt.Sprintf("window=%d", window))
		}
	}
}

func TestAggregateWindowPartialRange(t *testing.T) {
	t.Parallel()

	e, req := windowEngine(t)

	// A range that cuts parts in half makes the plan pushdown-unsafe: the fine buckets come from a
	// decode, and the windows must still match.
	for _, r := range []fetch.Request{
		{Start: 100, End: 315, Matchers: req.Matchers},
		{Start: 0, End: 250, Matchers: req.Matchers},
		{Start: 45, End: 400, Matchers: req.Matchers},
	} {
		assertWindowsMatchFetch(t, e, r, 100, 300)
		assertWindowsMatchFetch(t, e, r, 50, 150)
	}
}

// TestAggregateWindowHalfOpen pins the boundary that is the likely defect: (t-window, t] excludes a
// sample exactly at t-window and includes one exactly at t.
func TestAggregateWindowHalfOpen(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	e := aggEngine()
	s := mkSeries("job", "api")

	for _, ts := range []int64{10, 20, 30} {
		mustAppend(t, e, s, ts, float64(ts)/10)
	}

	require.NoError(t, e.Flush(ctx))

	got, err := e.AggregateWindow(ctx, fetch.Request{
		Start: 0, End: 100, Matchers: []fetch.Matcher{eqMatcher("job", "api")},
	}, 10, 20)
	require.NoError(t, err)

	nan := math.NaN()
	assertWindows(t, []engine.WindowAgg{
		{End: 10, SeriesAgg: engine.SeriesAgg{Count: 1, Sum: 1, Min: nan, Max: nan}}, // (-10,10]: 10
		{End: 20, SeriesAgg: engine.SeriesAgg{Count: 2, Sum: 3, Min: nan, Max: nan}}, // (0,20]: 10,20
		{End: 30, SeriesAgg: engine.SeriesAgg{Count: 2, Sum: 5, Min: nan, Max: nan}}, // (10,30]: 20,30 — not 10
		{End: 40, SeriesAgg: engine.SeriesAgg{Count: 1, Sum: 3, Min: nan, Max: nan}}, // (20,40]: 30
	}, got[s.Hash()], "half-open window")
}

// TestAggregateWindowMatchesStep checks the degenerate case against the disjoint primitive: at
// window == step each window (t-step, t] holds the same samples as the bucket [t-step, t) does when
// no sample sits exactly on a grid multiple.
func TestAggregateWindowMatchesStep(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	e := aggEngine()
	s := mkSeries("job", "api")

	for b := int64(0); b < 200; b += 10 { // two per bucket, never on a multiple of 10
		mustAppend(t, e, s, b+3, float64(b+3))
		mustAppend(t, e, s, b+7, float64(b+7))
	}

	require.NoError(t, e.Flush(ctx))

	req := fetch.Request{Start: 0, End: 1000, Matchers: []fetch.Matcher{eqMatcher("job", "api")}}

	buckets, err := e.AggregateStep(ctx, req, 10)
	require.NoError(t, err)

	windows, err := e.AggregateWindow(ctx, req, 10, 10)
	require.NoError(t, err)

	want := buckets[s.Hash()]
	got := windows[s.Hash()]
	require.Len(t, got, len(want))

	for i := range want {
		assert.Equal(t, want[i].Start+10, got[i].End, "window %d ends where the bucket does", i)
		assert.Equal(t, want[i].Count, got[i].Count, "window %d count", i)
		assert.InDelta(t, want[i].Sum, got[i].Sum, 1e-9, "window %d sum", i)
	}
}

func TestAggregateWindowDedupsOverlappingParts(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	e := aggEngine()
	s := mkSeries("job", "api")

	mustAppend(t, e, s, 10, 1)
	mustAppend(t, e, s, 20, 2)
	require.NoError(t, e.Flush(ctx))
	mustAppend(t, e, s, 20, 9) // same ts, newer ⇒ unsafe ⇒ merge before bucketing
	mustAppend(t, e, s, 130, 4)
	require.NoError(t, e.Flush(ctx))

	req := fetch.Request{Start: 0, End: 1000, Matchers: []fetch.Matcher{eqMatcher("job", "api")}}
	assertWindowsMatchFetch(t, e, req, 50, 200)
	assertWindowsMatchFetch(t, e, req, 50, 175) // misaligned fallback on the same unsafe plan
}

// TestAggregateWindowSparseGrid covers the map-backed fine grid: samples 1e6 apart at a step of 1
// would need millions of dense slots.
func TestAggregateWindowSparseGrid(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	e := aggEngine()
	s := mkSeries("job", "api")

	for i := range int64(8) {
		mustAppend(t, e, s, i*1_000_000, float64(i))
	}

	require.NoError(t, e.Flush(ctx))
	mustAppend(t, e, s, 9_000_000, 9) // head, past the flushed part

	req := fetch.Request{Start: 0, End: 10_000_000, Matchers: []fetch.Matcher{eqMatcher("job", "api")}}
	assertWindowsMatchFetch(t, e, req, 1_000_000, 3_000_000)
	assertWindowsMatchFetch(t, e, req, 2_000_000, 2_000_000)
}

// TestAggregateWindowSkipsGaps guards the empty-window jump: a long gap between samples must not be
// walked step by step, and must not emit windows that hold nothing.
func TestAggregateWindowSkipsGaps(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	e := aggEngine()
	s := mkSeries("job", "api")

	mustAppend(t, e, s, 5, 1)
	mustAppend(t, e, s, 1_000_005, 2)
	require.NoError(t, e.Flush(ctx))

	req := fetch.Request{Start: 0, End: 2_000_000, Matchers: []fetch.Matcher{eqMatcher("job", "api")}}

	got, err := e.AggregateWindow(ctx, req, 10, 30)
	require.NoError(t, err)

	// Three windows per sample (the sample enters at its own step and expires 30 later), none in
	// between — not the 200,000 the span would hold.
	require.Len(t, got[s.Hash()], 6)
	assertWindows(t, windowFold(fetchAll(t, e, req)[0], 10, 30, req.End), got[s.Hash()], "gap")
}

// TestAggregateWindowPushdown proves the sidecar still answers the overlapping case: a part wholly
// inside one fine bucket never decodes its value column, however many windows it feeds.
func TestAggregateWindowPushdown(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	e := engine.New(engine.Config{
		Backend: backend.Memory(), Prefix: "default/metrics", DecodeCacheBytes: 1 << 20, AggregateStats: true,
	})
	s := mkSeries("job", "api")

	for ts := int64(1); ts <= 50; ts++ {
		mustAppend(t, e, s, ts, float64(ts))
	}

	require.NoError(t, e.Flush(ctx)) // [1,50] ⊂ the fine bucket (0,100]

	req := fetch.Request{Start: 0, End: 1000, Matchers: []fetch.Matcher{eqMatcher("job", "api")}}

	_, err := e.AggregateWindow(ctx, req, 100, 1000) // 10x overlap
	require.NoError(t, err)

	st, _ := e.DecodeCacheStats()
	assert.Equal(t, int64(0), st.Misses, "a contained part feeds every overlapping window from its sidecar")

	_, err = e.AggregateWindow(ctx, req, 100, 150) // misaligned ⇒ per-sample fallback ⇒ decode
	require.NoError(t, err)

	st, _ = e.DecodeCacheStats()
	assert.Positive(t, st.Misses, "a window that is not a multiple of the step decodes")
}

func TestAggregateWindowNamed(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	e, req := windowEngine(t)

	named, err := e.AggregateWindowNamed(ctx, req, 100, 300)
	require.NoError(t, err)

	byID, err := e.AggregateWindow(ctx, req, 100, 300)
	require.NoError(t, err)

	require.Len(t, named, len(byID))

	for _, na := range named {
		assertWindows(t, byID[na.Series.Hash()], na.Windows, "named")
	}
}

func TestAggregateWindowRejectsNonPositiveStep(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	e, req := windowEngine(t)

	for _, step := range []int64{0, -1} {
		_, err := e.AggregateWindow(ctx, req, step, 100)
		require.Errorf(t, err, "step=%d", step)

		_, err = e.AggregateWindowNamed(ctx, req, step, 100)
		require.Errorf(t, err, "step=%d", step)
	}
}

// BenchmarkAggregateWindow is the point of the feature: the cost must be flat in the overlap factor
// (window/step), not proportional to it.
func BenchmarkAggregateWindow(b *testing.B) {
	ctx := context.Background()
	e := engine.New(engine.Config{
		Backend: backend.Memory(), Prefix: "default/metrics", AggregateStats: true, DecodeCacheBytes: 1 << 28,
	})

	const (
		series  = 50
		samples = 20_000
		scrape  = 15
	)

	for i := range series {
		s := mkSeries("job", fmt.Sprintf("job-%d", i))
		for j := range int64(samples) {
			if _, err := e.Append(s, j*scrape, float64(j%97)); err != nil {
				b.Fatal(err)
			}
		}
	}

	require.NoError(b, e.Flush(ctx))

	req := fetch.Request{Start: 0, End: samples * scrape, Matchers: []fetch.Matcher{anyMatcher("job")}}

	const step = 300 // 5m at a 15s scrape

	for _, overlap := range []int64{1, 12, 60} {
		b.Run(fmt.Sprintf("overlap=%dx", overlap), func(b *testing.B) {
			b.ReportAllocs()

			for b.Loop() {
				out, err := e.AggregateWindow(ctx, req, step, step*overlap)
				if err != nil {
					b.Fatal(err)
				}

				if len(out) != series {
					b.Fatalf("got %d series", len(out))
				}
			}
		})
	}
}

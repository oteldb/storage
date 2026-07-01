package promql

import (
	"context"
	"math"
	"testing"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/signal"
)

const sec = int64(1e9)

// fakeFetcher returns a fixed set of batches and records the request it received (so a test
// can assert which matchers were pushed down).
type fakeFetcher struct {
	batches []*fetch.Batch
	last    fetch.Request
}

func (f *fakeFetcher) Fetch(_ context.Context, r fetch.Request) (fetch.Iterator, error) {
	f.last = r

	return fetch.NewSliceIterator(f.batches), nil
}

func series(name, route string, samples ...[2]int64) *fetch.Batch {
	s := signal.Series{Attributes: signal.NewAttributes(
		signal.KeyValue{Key: []byte("__name__"), Value: signal.StringValue([]byte(name))},
		signal.KeyValue{Key: []byte("route"), Value: signal.StringValue([]byte(route))},
	)}
	b := &fetch.Batch{ID: s.Hash(), Series: s}
	for _, sm := range samples {
		b.Timestamps = append(b.Timestamps, sm[0]*sec)
		b.Values = append(b.Values, float64(sm[1]))
	}

	return b
}

func selectSeries(t *testing.T, f *fakeFetcher, ms ...*labels.Matcher) []storage.Series {
	t.Helper()
	q, err := NewQueryable(f, "default").Querier(0, 10_000_000)
	require.NoError(t, err)
	t.Cleanup(func() { _ = q.Close() })

	ss := q.Select(context.Background(), true, nil, ms...)
	var out []storage.Series
	for ss.Next() {
		out = append(out, ss.At())
	}
	require.NoError(t, ss.Err())

	return out
}

// lastByRoute reads each selected series' final sample, keyed by its route label.
func lastByRoute(t *testing.T, series []storage.Series) map[string]float64 {
	t.Helper()
	out := make(map[string]float64, len(series))
	for _, s := range series {
		it := s.Iterator(nil)
		var last float64
		for it.Next() != chunkenc.ValNone {
			_, last = it.At()
		}
		require.NoError(t, it.Err())
		out[s.Labels().Get("route")] = last
	}

	return out
}

func eq(t *testing.T, name, value string) *labels.Matcher {
	t.Helper()
	m, err := labels.NewMatcher(labels.MatchEqual, name, value)
	require.NoError(t, err)

	return m
}

func neq(t *testing.T, name, value string) *labels.Matcher {
	t.Helper()
	m, err := labels.NewMatcher(labels.MatchNotEqual, name, value)
	require.NoError(t, err)

	return m
}

// TestSelectZeroCopyAndRelease covers the zero-copy series path: iterator values are the batch's
// ns timeline converted to ms, Seek positions correctly, and the held batches are released only on
// querier.Close (the Recycle lifecycle), not during Select.
func TestSelectZeroCopyAndRelease(t *testing.T) {
	t.Parallel()

	released := 0
	b := series("node_x", "r0", [2]int64{1, 10}, [2]int64{2, 20}, [2]int64{3, 30})
	b.SetRelease(func(*fetch.Batch) { released++ })
	f := &fakeFetcher{batches: []*fetch.Batch{b}}

	q, err := NewQueryable(f, "default").Querier(0, 10_000_000)
	require.NoError(t, err)

	ss := q.Select(context.Background(), false, nil, eq(t, "__name__", "node_x"))

	var got [][2]float64

	require.True(t, ss.Next())
	s := ss.At()
	require.False(t, ss.Next(), "exactly one series")

	it := s.Iterator(nil)
	for it.Next() != chunkenc.ValNone {
		ts, v := it.At()
		got = append(got, [2]float64{float64(ts), v})
	}

	require.NoError(t, it.Err())
	// series() stamps ts in ns (×1e9); At() returns ms (÷1e6), so the second is 1000× the input.
	assert.Equal(t, [][2]float64{{1000, 10}, {2000, 20}, {3000, 30}}, got)

	// Seek lands on the first sample at/after the target (ms).
	it2 := s.Iterator(nil)
	require.Equal(t, chunkenc.ValFloat, it2.Seek(2000))
	ts, v := it2.At()
	assert.Equal(t, int64(2000), ts)
	assert.InDelta(t, 20.0, v, 0)
	assert.Equal(t, chunkenc.ValNone, it2.Seek(9999), "past the last sample ⇒ exhausted")

	assert.Equal(t, 0, released, "batches must stay valid until Close")
	require.NoError(t, q.Close())
	assert.Equal(t, 1, released, "Close releases the held batch")
}

func TestSelectPushesPositiveMatcher(t *testing.T) {
	t.Parallel()

	f := &fakeFetcher{batches: []*fetch.Batch{
		series("m", "/a", [2]int64{100, 1}, [2]int64{110, 2}),
		series("m", "/b", [2]int64{100, 9}),
	}}

	got := selectSeries(t, f, eq(t, "__name__", "m"))
	assert.Equal(t, map[string]float64{"/a": 2, "/b": 9}, lastByRoute(t, got))

	// The positive __name__ matcher is pushed into the fetch request.
	require.Len(t, f.last.Matchers, 1)
	assert.Equal(t, []byte("__name__"), f.last.Matchers[0].Name)
	// The querier window is finite (not the MinInt64/MaxInt64 sentinels).
	assert.Positive(t, f.last.End)
	assert.GreaterOrEqual(t, f.last.Start, int64(0))
}

func TestSelectNegativeMatcherNotPushed(t *testing.T) {
	t.Parallel()

	f := &fakeFetcher{batches: []*fetch.Batch{
		series("m", "/a", [2]int64{100, 1}),
		series("m", "/b", [2]int64{100, 2}),
	}}

	got := selectSeries(t, f, eq(t, "__name__", "m"), neq(t, "route", "/a"))
	assert.Equal(t, map[string]float64{"/b": 2}, lastByRoute(t, got), "negative matcher filters to /b")

	// Only the index-safe __name__ matcher is pushed down; the negated one is enforced in the
	// post-fetch re-check, not the postings index.
	require.Len(t, f.last.Matchers, 1)
	assert.Equal(t, []byte("__name__"), f.last.Matchers[0].Name)
}

func TestSelectHidesReservedLabels(t *testing.T) {
	t.Parallel()

	// A series carrying an internal reserved label (__unit__) must not expose it to PromQL.
	s := signal.Series{Attributes: signal.NewAttributes(
		signal.KeyValue{Key: []byte("__name__"), Value: signal.StringValue([]byte("m"))},
		signal.KeyValue{Key: []byte("__unit__"), Value: signal.StringValue([]byte("By"))},
	)}
	b := &fetch.Batch{ID: s.Hash(), Series: s, Timestamps: []int64{100 * sec}, Values: []float64{1}}
	f := &fakeFetcher{batches: []*fetch.Batch{b}}

	got := selectSeries(t, f, eq(t, "__name__", "m"))
	require.Len(t, got, 1)
	assert.Equal(t, "m", got[0].Labels().Get("__name__"))
	assert.Empty(t, got[0].Labels().Get("__unit__"), "__unit__ is hidden")
}

func TestQuerierLabelMetadata(t *testing.T) {
	t.Parallel()

	f := &fakeFetcher{batches: []*fetch.Batch{
		series("http.requests", "/a", [2]int64{100, 1}),
		series("http.requests", "/b", [2]int64{100, 2}),
		series("cpu.seconds", "/a", [2]int64{100, 3}),
	}}
	q, err := NewQueryable(f, "default").Querier(0, 10_000_000)
	require.NoError(t, err)
	t.Cleanup(func() { _ = q.Close() })
	ctx := context.Background()

	names, _, err := q.LabelNames(ctx, nil)
	require.NoError(t, err)
	assert.Equal(t, []string{"__name__", "route"}, names)

	// __name__ values are the metric names; dotted (UTF-8) names are preserved, not normalized.
	vals, _, err := q.LabelValues(ctx, "__name__", nil)
	require.NoError(t, err)
	assert.Equal(t, []string{"cpu.seconds", "http.requests"}, vals)

	vals, _, err = q.LabelValues(ctx, "route", nil)
	require.NoError(t, err)
	assert.Equal(t, []string{"/a", "/b"}, vals)

	// Matchers scope the metadata: only the routes of the cpu.seconds metric.
	vals, _, err = q.LabelValues(ctx, "route", nil, eq(t, "__name__", "cpu.seconds"))
	require.NoError(t, err)
	assert.Equal(t, []string{"/a"}, vals)

	// An empty result (no matching series) is empty, not an error.
	vals, _, err = q.LabelValues(ctx, "route", nil, eq(t, "__name__", "nope"))
	require.NoError(t, err)
	assert.Empty(t, vals)
}

// TestMsToNsClamp covers the millisecond→nanosecond window conversion: finite values convert, while
// any out-of-range magnitude (the MinInt64/MaxInt64 sentinels and Prometheus' MinTime/MaxTime, which
// an unbounded label query arrives with) collapses to the open-ended clamp instead of overflowing.
func TestMsToNsClamp(t *testing.T) {
	t.Parallel()

	const maxMs = math.MaxInt64 / nsPerMs
	assert.Equal(t, int64(1000)*nsPerMs, msToNsClamp(1000, math.MinInt64), "finite ms converts")

	for _, ms := range []int64{math.MinInt64, math.MaxInt64, -maxMs - 1, maxMs + 1, -9_000_000_000_000_000} {
		assert.Equalf(t, int64(math.MinInt64), msToNsClamp(ms, math.MinInt64), "ms=%d start clamp", ms)
		assert.Equalf(t, int64(math.MaxInt64), msToNsClamp(ms, math.MaxInt64), "ms=%d end clamp", ms)
	}
}

// TestExportedHelpers covers the exported projection helpers an embedder's pushdown path calls
// directly: PromLabels mirrors the [Queryable]'s label projection, MatchesAll is the
// post-fetch full-set re-check, and PushableMatchers lowers only the index-safe subset.
func TestExportedHelpers(t *testing.T) {
	t.Parallel()

	s := signal.Series{Attributes: signal.NewAttributes(
		signal.KeyValue{Key: []byte("__name__"), Value: signal.StringValue([]byte("m"))},
		signal.KeyValue{Key: []byte("__unit__"), Value: signal.StringValue([]byte("By"))},
		signal.KeyValue{Key: []byte("route"), Value: signal.StringValue([]byte("/a"))},
	)}

	// PromLabels hides the reserved __unit__ and keeps __name__/route.
	lset := PromLabels(s)
	assert.Equal(t, "m", lset.Get("__name__"))
	assert.Equal(t, "/a", lset.Get("route"))
	assert.Empty(t, lset.Get("__unit__"), "__unit__ hidden")

	// MatchesAll treats an absent label as "" (Prometheus semantics): a "!=" matcher passes.
	assert.True(t, MatchesAll(lset, []*labels.Matcher{neq(t, "absent", "x")}))
	assert.False(t, MatchesAll(lset, []*labels.Matcher{eq(t, "route", "/b")}))

	// PushableMatchers keeps only index-safe matchers: the equality __name__ (with a serializable
	// Spec) is pushed, the negated route matcher (which matches "") is not.
	pushed := PushableMatchers([]*labels.Matcher{eq(t, "__name__", "m"), neq(t, "route", "/x")})
	require.Len(t, pushed, 1)
	assert.Equal(t, []byte("__name__"), pushed[0].Name)
	assert.NotNil(t, pushed[0].Spec, "equality matcher carries a serializable Spec")
}

// TestLabelCacheSharedAcrossQueryables covers the engine-lifetime interning contract: a LabelCache
// shared across successive Queryables (each over a fresh fetcher, as an embedder does per query)
// projects each series' labels once and reuses the entry, so the resident set is bounded by
// cardinality rather than rebuilt per query.
func TestLabelCacheSharedAcrossQueryables(t *testing.T) {
	t.Parallel()

	cache := NewLabelCache()
	require.Zero(t, cache.Len())

	batches := []*fetch.Batch{
		series("node_x", "r0", [2]int64{1, 10}),
		series("node_x", "r1", [2]int64{1, 20}),
	}

	drain := func() []labels.Labels {
		// A fresh fetcher per query, mirroring the embedder taking a new fetcher to see the latest head.
		q, err := NewQueryableWithCache(&fakeFetcher{batches: batches}, "default", cache).Querier(0, 10_000_000)
		require.NoError(t, err)
		t.Cleanup(func() { _ = q.Close() })

		ss := q.Select(context.Background(), true, nil, eq(t, "__name__", "node_x"))
		var out []labels.Labels
		for ss.Next() {
			out = append(out, ss.At().Labels())
		}
		require.NoError(t, ss.Err())

		return out
	}

	first := drain()
	require.Len(t, first, 2)
	require.Equal(t, 2, cache.Len(), "each distinct series interned once")

	second := drain()
	require.Len(t, second, 2)
	require.Equal(t, 2, cache.Len(), "second query reuses entries, no growth")

	// The projections are stable across queries (same content for the same series identity).
	for i := range first {
		assert.Equal(t, first[i].String(), second[i].String())
	}
}

// TestNewQueryableWithNilCache falls back to a private cache instead of panicking.
func TestNewQueryableWithNilCache(t *testing.T) {
	t.Parallel()

	q, err := NewQueryableWithCache(&fakeFetcher{}, "default", nil).Querier(0, 10)
	require.NoError(t, err)
	require.NoError(t, q.Close())
}

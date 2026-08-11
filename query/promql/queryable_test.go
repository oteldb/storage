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

// groupCountingFetcher implements fetch.GroupCounter, recording the grouped-count request.
type groupCountingFetcher struct {
	fakeFetcher

	label  string
	groups map[string]int
}

func (f *groupCountingFetcher) CountBy(_ context.Context, r fetch.Request, label []byte) (map[string]int, error) {
	f.last = r
	f.label = string(label)

	return f.groups, nil
}

func TestCountSeriesByPushdown(t *testing.T) {
	t.Parallel()

	f := &groupCountingFetcher{groups: map[string]int{"0": 2, "1": 1, "": 1}}
	q, err := NewQueryable(f, "default").Querier(0, 10_000)
	require.NoError(t, err)
	t.Cleanup(func() { _ = q.Close() })

	counter, ok := q.(interface {
		CountSeriesBy(ctx context.Context, startMs, endMs int64, label string, matchers ...*labels.Matcher) (map[string]uint64, error)
	})
	require.True(t, ok, "querier exposes the grouped-count pushdown hook")

	got, err := counter.CountSeriesBy(context.Background(), 0, 10_000, "cpu", eq(t, "__name__", "node_cpu"))
	require.NoError(t, err)
	assert.Equal(t, map[string]uint64{"0": 2, "1": 1, "": 1}, got)
	assert.Equal(t, "cpu", f.label, "the grouping label reaches the engine primitive")
	assert.Len(t, f.last.Matchers, 1, "the pushable matcher is pushed")
}

func TestCountSeriesByRecheckFallback(t *testing.T) {
	t.Parallel()

	// route=a twice, route=b once; one series matches the metric but will be dropped by the
	// non-pushable != matcher.
	f := &fakeFetcher{batches: []*fetch.Batch{
		series("http_requests", "a", [2]int64{1, 1}),
		series("http_requests", "a2", [2]int64{2, 2}),
		series("http_requests", "b", [2]int64{3, 3}),
	}}

	q, err := NewQueryable(f, "default").Querier(0, 10_000)
	require.NoError(t, err)
	t.Cleanup(func() { _ = q.Close() })

	counter := q.(interface {
		CountSeriesBy(ctx context.Context, startMs, endMs int64, label string, matchers ...*labels.Matcher) (map[string]uint64, error)
	})

	// The != matcher is not pushable, so the fallback fetches and re-checks; grouping by __name__
	// buckets the two surviving series under the metric name.
	got, err := counter.CountSeriesBy(
		context.Background(), 0, 10_000, "route",
		eq(t, "__name__", "http_requests"), neq(t, "route", "b"),
	)
	require.NoError(t, err)
	assert.Equal(t, map[string]uint64{"a": 1, "a2": 1}, got)

	// A plain fakeFetcher implements no GroupCounter: even with fully pushable matchers the hook
	// answers via the exact Fetch-based grouping (the engine cannot re-plan mid-query, so this
	// path must never error on an unsupported chain).
	all, err := counter.CountSeriesBy(context.Background(), 0, 10_000, "route", eq(t, "__name__", "http_requests"))
	require.NoError(t, err)
	assert.Equal(t, map[string]uint64{"a": 1, "a2": 1, "b": 1}, all)
}

// listingFetcher implements fetch.SeriesLister, returning identities without samples and recording
// whether the sample path was taken. Its batches back the Fetch fallback, so a test can tell the two
// paths apart by giving them different contents.
type listingFetcher struct {
	fakeFetcher

	series  []signal.Series
	lastReq fetch.Request
	calls   int
}

func (f *listingFetcher) Series(_ context.Context, r fetch.Request) ([]signal.Series, error) {
	f.lastReq = r
	f.calls++

	return f.series, nil
}

// TestLabelMetadataUsesSeriesLister pins the enumeration pushdown: with a [fetch.SeriesLister] in
// the chain, the label endpoints answer from identities alone — no fetch, no sample materialization
// — while still re-checking the non-pushable matchers.
func TestLabelMetadataUsesSeriesLister(t *testing.T) {
	t.Parallel()

	f := &listingFetcher{series: []signal.Series{
		series("http.requests", "/a").Series,
		series("http.requests", "/b").Series,
		series("cpu.seconds", "/a").Series,
	}}
	// Distinct contents on the sample path: anything sourced from it would show up as "/x".
	f.batches = []*fetch.Batch{series("http.requests", "/x", [2]int64{1, 1})}

	q, err := NewQueryable(f, "default").Querier(0, 10_000_000)
	require.NoError(t, err)
	t.Cleanup(func() { _ = q.Close() })
	ctx := context.Background()

	names, _, err := q.LabelNames(ctx, nil)
	require.NoError(t, err)
	assert.Equal(t, []string{"__name__", "route"}, names)

	vals, _, err := q.LabelValues(ctx, "__name__", nil)
	require.NoError(t, err)
	assert.Equal(t, []string{"cpu.seconds", "http.requests"}, vals)

	// The pushable matcher reaches the enumeration request; the negated one is re-checked here.
	vals, _, err = q.LabelValues(ctx, "route", nil, eq(t, "__name__", "http.requests"), neq(t, "route", "/b"))
	require.NoError(t, err)
	assert.Equal(t, []string{"/a"}, vals)
	require.Len(t, f.lastReq.Matchers, 1, "only the index-safe matcher is pushed")
	assert.Equal(t, []byte("__name__"), f.lastReq.Matchers[0].Name)
	assert.Equal(t, 3, f.calls, "every label call went through the enumeration seam")
	assert.Zero(t, f.last.End, "the sample path was never taken")
}

// TestCountSeriesRecheckStaysExact guards the semantic boundary of the enumeration seam: it is
// part-granular (a series in a window-overlapping part is listed even with no sample inside), which
// is fine for metadata but not for a count, which is a query result. The count rechecks must keep
// fetching — here the lister offers an extra series the fetch does not, and it must not be counted.
func TestCountSeriesRecheckStaysExact(t *testing.T) {
	t.Parallel()

	f := &listingFetcher{series: []signal.Series{
		series("http.requests", "/a").Series,
		series("http.requests", "/b").Series, // no sample in the window ⇒ not fetched
	}}
	f.batches = []*fetch.Batch{series("http.requests", "/a", [2]int64{1, 1})}

	q, err := NewQueryable(f, "default").Querier(0, 10_000_000)
	require.NoError(t, err)
	t.Cleanup(func() { _ = q.Close() })

	counter := q.(interface {
		CountSeries(ctx context.Context, startMs, endMs int64, matchers ...*labels.Matcher) (uint64, error)
		CountSeriesBy(ctx context.Context, startMs, endMs int64, label string, matchers ...*labels.Matcher) (map[string]uint64, error)
	})

	// A non-pushable matcher forces the recheck path (the exact one).
	ms := []*labels.Matcher{eq(t, "__name__", "http.requests"), neq(t, "route", "/zzz")}

	n, err := counter.CountSeries(context.Background(), 0, 10_000, ms...)
	require.NoError(t, err)
	assert.Equal(t, uint64(1), n, "only the series with an in-window sample counts")

	groups, err := counter.CountSeriesBy(context.Background(), 0, 10_000, "route", ms...)
	require.NoError(t, err)
	assert.Equal(t, map[string]uint64{"/a": 1}, groups)
	assert.Zero(t, f.calls, "the count rechecks never take the part-granular enumeration seam")
}

// TestSeriesListerDiscovery pins the capability discovery: a lister is found, a plain fetcher opts
// out (and the label endpoints then fall back to the fetch path).
func TestSeriesListerDiscovery(t *testing.T) {
	t.Parallel()

	assert.NotNil(t, fetch.SeriesListerOf(&listingFetcher{}))
	assert.Nil(t, fetch.SeriesListerOf(&fakeFetcher{}), "a plain fetcher opts out")

	// The fallback still answers the label endpoints exactly.
	f := &fakeFetcher{batches: []*fetch.Batch{
		series("http.requests", "/a", [2]int64{1, 1}),
		series("http.requests", "/b", [2]int64{1, 2}),
	}}
	q, err := NewQueryable(f, "default").Querier(0, 10_000_000)
	require.NoError(t, err)
	t.Cleanup(func() { _ = q.Close() })

	vals, _, err := q.LabelValues(context.Background(), "route", nil)
	require.NoError(t, err)
	assert.Equal(t, []string{"/a", "/b"}, vals)
}

// labelingFetcher implements fetch.LabelLister on top of the identity lister, so a test can tell
// which of the three paths (index metadata / identity enumeration / fetch) answered.
type labelingFetcher struct {
	listingFetcher

	names       []string
	values      []string
	lastReq     fetch.Request
	nameCalls   int
	valCalls    int
	unsupported bool
}

func (f *labelingFetcher) LabelNames(_ context.Context, r fetch.Request) ([]string, error) {
	if f.unsupported {
		return nil, fetch.ErrLabelsUnsupported
	}

	f.lastReq = r
	f.nameCalls++

	return f.names, nil
}

func (f *labelingFetcher) LabelValues(_ context.Context, r fetch.Request, _ []byte) ([]string, error) {
	if f.unsupported {
		return nil, fetch.ErrLabelsUnsupported
	}

	f.lastReq = r
	f.valCalls++

	return f.values, nil
}

// TestLabelMetadataUsesLabelLister pins the label-metadata pushdown: with the capability present the
// endpoints answer from the index — no identities, no samples — with the reserved internal labels
// and the empty value filtered out on the way through.
func TestLabelMetadataUsesLabelLister(t *testing.T) {
	t.Parallel()

	f := &labelingFetcher{
		names:  []string{"__name__", "__unit__", "route"},
		values: []string{"", "/a", "/b"},
	}
	f.series = []signal.Series{series("http.requests", "/x").Series} // the identity path, if taken

	q, err := NewQueryable(f, "default").Querier(0, 10_000_000)
	require.NoError(t, err)
	t.Cleanup(func() { _ = q.Close() })
	ctx := context.Background()

	names, _, err := q.LabelNames(ctx, nil, eq(t, "__name__", "http.requests"))
	require.NoError(t, err)
	assert.Equal(t, []string{"__name__", "route"}, names, "the reserved __unit__ is not a PromQL label")

	vals, _, err := q.LabelValues(ctx, "route", nil, eq(t, "__name__", "http.requests"))
	require.NoError(t, err)
	assert.Equal(t, []string{"/a", "/b"}, vals, "the empty value is an absent label, not a value")

	require.Len(t, f.lastReq.Matchers, 1, "the pushable matcher reaches the index")
	assert.Equal(t, 1, f.nameCalls)
	assert.Equal(t, 1, f.valCalls)
	assert.Zero(t, f.calls, "the identity seam was not needed")
}

// TestLabelMetadataFallsBackFromLabelLister covers the two ways the metadata pushdown steps aside:
// a matcher that cannot be pushed (nothing would re-check it, since the capability returns strings,
// not identities) and a chain that reports the capability unsupported.
func TestLabelMetadataFallsBackFromLabelLister(t *testing.T) {
	t.Parallel()

	f := &labelingFetcher{names: []string{"wrong"}, values: []string{"wrong"}}
	f.series = []signal.Series{
		series("http.requests", "/a").Series,
		series("http.requests", "/b").Series,
	}

	q, err := NewQueryable(f, "default").Querier(0, 10_000_000)
	require.NoError(t, err)
	t.Cleanup(func() { _ = q.Close() })
	ctx := context.Background()

	// Non-pushable matcher ⇒ the identity path, which re-checks it.
	vals, _, err := q.LabelValues(ctx, "route", nil, neq(t, "route", "/b"))
	require.NoError(t, err)
	assert.Equal(t, []string{"/a"}, vals)
	assert.Zero(t, f.valCalls, "the metadata capability cannot re-check a non-pushable matcher")
	assert.Equal(t, 1, f.calls, "the identity seam answered")

	// Capability present but unsupported for this chain ⇒ same fallback, no error surfaced.
	f.unsupported = true

	vals, _, err = q.LabelValues(ctx, "route", nil)
	require.NoError(t, err)
	assert.Equal(t, []string{"/a", "/b"}, vals)

	names, _, err := q.LabelNames(ctx, nil)
	require.NoError(t, err)
	assert.Equal(t, []string{"__name__", "route"}, names)
}

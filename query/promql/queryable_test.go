package promql

import (
	"context"
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

package promql

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/query"
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

func eval(t *testing.T, f fetch.Fetcher, text string, atSec int64) query.Result {
	t.Helper()
	res, err := NewEngine().Eval(context.Background(), f, "default", Params{Text: text, Start: atSec * sec, End: atSec * sec})
	require.NoError(t, err)

	return res
}

func TestEvalSelectorAndLabels(t *testing.T) {
	t.Parallel()

	f := &fakeFetcher{batches: []*fetch.Batch{
		series("m", "/a", [2]int64{100, 1}, [2]int64{110, 2}),
		series("m", "/b", [2]int64{100, 9}),
	}}

	res := eval(t, f, "m", 110)
	require.Equal(t, query.ResultVector, res.Type)
	require.Len(t, res.Series, 2)

	// The positive __name__ matcher is pushed into the fetch request.
	require.Len(t, f.last.Matchers, 1)
	assert.Equal(t, []byte("__name__"), f.last.Matchers[0].Name)
}

func TestEvalNegativeMatcherNotPushed(t *testing.T) {
	t.Parallel()

	f := &fakeFetcher{batches: []*fetch.Batch{
		series("m", "/a", [2]int64{100, 1}),
		series("m", "/b", [2]int64{100, 2}),
	}}

	res := eval(t, f, `m{route!="/a"}`, 100)
	require.Equal(t, query.ResultVector, res.Type)
	require.Len(t, res.Series, 1, "negative matcher filters to /b")
	assert.Equal(t, "/b", labelValue(res.Series[0], "route"))

	// Only the index-safe __name__ matcher is pushed down; the negative one is enforced in
	// the post-fetch re-check, not the postings index.
	require.Len(t, f.last.Matchers, 1)
	assert.Equal(t, []byte("__name__"), f.last.Matchers[0].Name)
}

func TestEvalTimeWindowFromQuery(t *testing.T) {
	t.Parallel()

	f := &fakeFetcher{batches: []*fetch.Batch{series("m", "/a", [2]int64{100, 1})}}
	_ = eval(t, f, "m", 100)

	// The querier window is finite (lookback-bounded), not the MinInt64/MaxInt64 sentinels.
	assert.Positive(t, f.last.End)
	assert.NotEqual(t, int64(-1<<63), f.last.Start)
}

func TestQuerierMetadataStubs(t *testing.T) {
	t.Parallel()

	q, err := NewQueryable(&fakeFetcher{}, "default").Querier(0, 1000)
	require.NoError(t, err)

	names, _, err := q.LabelNames(context.Background(), nil)
	require.NoError(t, err)
	assert.Nil(t, names)

	vals, _, err := q.LabelValues(context.Background(), "x", nil)
	require.NoError(t, err)
	assert.Nil(t, vals)

	require.NoError(t, q.Close())
}

func labelValue(s query.Series, name string) string {
	for _, l := range s.Metric {
		if l.Name == name {
			return l.Value
		}
	}

	return ""
}

package router_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/engine"
	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/signal"
)

func TestAbsentMatcherRechecksOwnerSuperset(t *testing.T) {
	t.Parallel()
	missing := signal.Series{}
	empty, api, web := svcSeries(""), svcSeries("api"), svcSeries("web")
	p := &peer{series: []signal.Series{missing, empty, api, web}}
	for _, s := range p.series {
		p.aggs = append(p.aggs, engine.NamedAgg{Series: s})
	}
	r := openRouter(t, p)
	ms := []fetch.Matcher{regexMatcher("service.name", "^(api|)$")}
	want := []signal.SeriesID{missing.Hash(), empty.Hash(), api.Hash()}
	it, err := r.Fetcher(signal.Metric, "acme").Fetch(t.Context(), fetch.Request{Start: 0, End: 10, Matchers: ms})
	require.NoError(t, err)
	batches, err := fetch.Drain(t.Context(), it)
	require.NoError(t, err)
	ids := make([]signal.SeriesID, 0, len(batches))
	for _, b := range batches {
		ids = append(ids, b.ID)
	}
	assert.ElementsMatch(t, want, ids)
	series, err := r.Series(t.Context(), signal.Metric, "acme", ms, 0, 10)
	require.NoError(t, err)
	ids = ids[:0]
	for _, s := range series {
		ids = append(ids, s.Hash())
	}
	assert.ElementsMatch(t, want, ids)
	aggs, err := r.Aggregate(t.Context(), "acme", 0, 10, 0, ms)
	require.NoError(t, err)
	ids = ids[:0]
	for _, a := range aggs {
		ids = append(ids, a.Series.Hash())
	}
	assert.ElementsMatch(t, want, ids)
	p.mu.Lock()
	defer p.mu.Unlock()
	assert.Empty(t, p.lastFetch.Matchers, "opaque predicates stay on the coordinator")
}

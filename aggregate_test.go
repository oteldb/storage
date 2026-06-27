package storage

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/query/fetch"
)

// TestAggregateMetricsEndToEnd drives AggregateMetrics through the public facade and checks it
// matches the aggregate of a raw fetch, exercising the sidecar pushdown over flushed parts.
func TestAggregateMetricsEndToEnd(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	s, err := Open(ctx, Options{}, WithBackend(backend.Memory()), WithAggregateStats())
	require.NoError(t, err)
	defer func() { require.NoError(t, s.Close(ctx)) }()

	if _, err := s.WriteMetrics(ctx, buildCorpus(corpusProfile{
		name: "c", series: 100, points: 50, interval: 15_000_000_000, pattern: patCounter,
	}, 1)); err != nil {
		t.Fatal(err)
	}
	eng := mustEngine(s.engineFor("default"))
	require.NoError(t, eng.Flush(ctx))

	req := fetch.Request{Start: 0, End: 1 << 62, Matchers: []fetch.Matcher{nameMatcher("bench.metric")}}

	got, err := s.AggregateMetrics(ctx, "default", req)
	require.NoError(t, err)
	require.Len(t, got, 100, "one aggregate per series")

	// Cross-check each series against the raw fetch.
	it, err := s.Fetcher("default").Fetch(ctx, req)
	require.NoError(t, err)
	batches, err := fetch.Drain(ctx, it)
	require.NoError(t, err)
	require.Len(t, batches, 100)

	for _, b := range batches {
		agg, ok := got[b.ID]
		require.Truef(t, ok, "series %v missing from aggregate", b.ID)

		var sum, mn, mx float64
		for i, v := range b.Values {
			sum += v
			if i == 0 || v < mn {
				mn = v
			}
			if i == 0 || v > mx {
				mx = v
			}
		}
		assert.Equal(t, int64(len(b.Values)), agg.Count)
		assert.InDelta(t, sum, agg.Sum, 0)
		assert.InDelta(t, mn, agg.Min, 0)
		assert.InDelta(t, mx, agg.Max, 0)
	}
}

package storage

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/engine"
	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/signal"
)

// sliceFetcher returns a fixed set of batches — stands in for the cluster fan-out so the coordinator
// fold can be unit-tested without a real cluster.
type sliceFetcher struct{ batches []*fetch.Batch }

func (f sliceFetcher) Fetch(context.Context, fetch.Request) (fetch.Iterator, error) {
	return fetch.NewSliceIterator(f.batches), nil
}

func aggBatch(lo uint64, ts []int64, vals []float64) *fetch.Batch {
	return &fetch.Batch{ID: signal.SeriesID{Lo: lo}, Timestamps: ts, Values: vals}
}

func TestAggregateFetchFolds(t *testing.T) {
	t.Parallel()

	f := sliceFetcher{batches: []*fetch.Batch{
		aggBatch(1, []int64{1, 2, 3}, []float64{10, 20, 6}),
		aggBatch(2, []int64{5}, []float64{7}),
	}}

	got, err := aggregateFetch(context.Background(), f, fetch.Request{})
	require.NoError(t, err)
	assert.Equal(t, engine.SeriesAgg{Count: 3, Sum: 36, Min: 6, Max: 20}, got[signal.SeriesID{Lo: 1}])
	assert.Equal(t, engine.SeriesAgg{Count: 1, Sum: 7, Min: 7, Max: 7}, got[signal.SeriesID{Lo: 2}])
}

func TestAggregateFetchStepFolds(t *testing.T) {
	t.Parallel()

	f := sliceFetcher{batches: []*fetch.Batch{aggBatch(1, []int64{1, 5, 105, 130}, []float64{1, 3, 9, 11})}}

	got, err := aggregateFetchStep(context.Background(), f, fetch.Request{}, 100)
	require.NoError(t, err)

	list := got[signal.SeriesID{Lo: 1}]
	require.Len(t, list, 2)
	assert.Equal(t, engine.BucketAgg{Start: 0, SeriesAgg: engine.SeriesAgg{Count: 2, Sum: 4, Min: 1, Max: 3}}, list[0])
	assert.Equal(t, engine.BucketAgg{Start: 100, SeriesAgg: engine.SeriesAgg{Count: 2, Sum: 20, Min: 9, Max: 11}}, list[1])
}

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

package storage

import (
	"bytes"
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/engine"
	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/query/promql"
	"github.com/oteldb/storage/signal"
)

func aggJobSeries(job string) signal.Series {
	return signal.Series{Attributes: signal.NewAttributes(
		signal.KeyValue{Key: []byte("job"), Value: signal.StringValue([]byte(job))})}
}

func aggJobMatcher(job string) fetch.Matcher {
	want := []byte(job)

	return fetch.Matcher{Name: []byte("job"), Match: func(v signal.Value) bool { return bytes.Equal(v.Str(), want) }}
}

// TestUnionNamed covers the coordinator side of the cluster aggregate fan-out: it re-checks the full
// matcher set against each shard's returned identities (a remote peer applied only the equality
// subset) and merges buckets for any series that surfaces from more than one shard.
func TestUnionNamed(t *testing.T) {
	t.Parallel()

	api, web := aggJobSeries("api"), aggJobSeries("web")
	matchers := []fetch.Matcher{aggJobMatcher("api")}

	out := map[signal.SeriesID][]engine.BucketAgg{}

	// Shard 1 returns a superset (api + web); only api survives the full matcher set.
	unionNamed(out, []engine.NamedAgg{
		{Series: api, Buckets: []engine.BucketAgg{{Start: 0, SeriesAgg: engine.SeriesAgg{Count: 2, Sum: 5, Min: 1, Max: 4}}}},
		{Series: web, Buckets: []engine.BucketAgg{{Start: 0, SeriesAgg: engine.SeriesAgg{Count: 1, Sum: 9, Min: 9, Max: 9}}}},
	}, matchers)

	require.Len(t, out, 1)
	require.Contains(t, out, api.Hash())
	assert.NotContains(t, out, web.Hash(), "web fails the full matcher set")

	// A second shard surfaces the same series in a different bucket ⇒ merge (defensive; series are
	// normally shard-partitioned).
	unionNamed(out, []engine.NamedAgg{
		{Series: api, Buckets: []engine.BucketAgg{{Start: 60, SeriesAgg: engine.SeriesAgg{Count: 1, Sum: 3, Min: 3, Max: 3}}}},
	}, matchers)

	list := out[api.Hash()]
	require.Len(t, list, 2)
	assert.Equal(t, int64(0), list[0].Start)
	assert.Equal(t, int64(60), list[1].Start)
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

// TestAggregateMetricsNamed drives the labeled aggregate facade and checks each result carries the
// series identity (renderable as labels) alongside the same aggregate the unlabeled facade returns.
func TestAggregateMetricsNamed(t *testing.T) {
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

	want, err := s.AggregateMetrics(ctx, "default", req)
	require.NoError(t, err)

	got, err := s.AggregateMetricsNamed(ctx, "default", req)
	require.NoError(t, err)
	require.Len(t, got, len(want), "one labeled aggregate per unlabeled entry")

	for _, la := range got {
		id := la.Series.Hash()
		agg, ok := want[id]
		require.Truef(t, ok, "labeled series %v absent from unlabeled map", id)
		assert.Equal(t, agg, la.SeriesAgg, "labeled aggregate matches the unlabeled facade")

		// The identity renders as a Prometheus label set carrying the metric name.
		lset := promql.PromLabels(la.Series)
		assert.Equal(t, "bench.metric", lset.Get("__name__"))
	}
}

package storage

import (
	"bytes"
	"context"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/engine"
	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/query/promql"
	"github.com/oteldb/storage/signal"
	"github.com/oteldb/storage/signal/metric"
)

// aggStep is a bucket width the corpus epoch (1.6e18 ns) divides evenly, so bucket starts on the
// absolute grid line up with the first sample and the assertions stay readable.
const aggStep = int64(100_000_000_000) // 100s

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

// TestAggregateMetricsStepNamed checks the stepped facade returns in one call exactly what the
// per-step calls it replaces would: every bucket must equal a whole-range aggregate over that
// bucket's window, and the buckets must fold back into the whole-range aggregate.
func TestAggregateMetricsStepNamed(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	s, err := Open(ctx, Options{}, WithBackend(backend.Memory()), WithAggregateStats())
	require.NoError(t, err)
	defer func() { require.NoError(t, s.Close(ctx)) }()

	const series = 20

	_, err = s.WriteMetrics(ctx, buildCorpus(corpusProfile{
		name: "c", series: series, points: 50, interval: 15_000_000_000, pattern: patCounter,
	}, 1))
	require.NoError(t, err)
	require.NoError(t, mustEngine(s.engineFor("default")).Flush(ctx))

	req := fetch.Request{Start: 0, End: 1 << 62, Matchers: []fetch.Matcher{nameMatcher("bench.metric")}}

	got, err := s.AggregateMetricsStepNamed(ctx, "default", req, aggStep)
	require.NoError(t, err)
	require.Len(t, got, series, "one entry per series")

	whole, err := s.AggregateMetricsNamed(ctx, "default", req)
	require.NoError(t, err)
	require.Len(t, whole, series)

	for _, na := range got {
		require.NotEmpty(t, na.Buckets)

		var total engine.SeriesAgg
		for i, b := range na.Buckets {
			if i > 0 {
				assert.Greater(t, b.Start, na.Buckets[i-1].Start, "buckets sorted ascending by start")
			}
			assert.Zero(t, b.Start%aggStep, "buckets align to the absolute grid")

			// The N-call form this replaces: one whole-range aggregate per bucket window.
			perStep, err := s.AggregateMetricsNamed(ctx, "default",
				fetch.Request{Start: b.Start, End: b.Start + aggStep - 1, Matchers: req.Matchers})
			require.NoError(t, err)

			want, ok := findAggregate(perStep, na.Series.Hash())
			require.Truef(t, ok, "series absent from the narrowed window at %d", b.Start)
			assert.Equalf(t, want, b.SeriesAgg, "bucket %d matches the narrowed whole-range call", b.Start)

			foldAgg(&total, b.SeriesAgg)
		}

		want, ok := findAggregate(whole, na.Series.Hash())
		require.True(t, ok)
		assert.Equal(t, want, total, "the buckets fold back into the whole-range aggregate")
	}
}

// TestAggregateMetricsStepNamedWholeRange checks step ≤ 0 collapses to the single whole-range
// bucket [Storage.AggregateMetricsNamed] is defined in terms of.
func TestAggregateMetricsStepNamedWholeRange(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	s, err := Open(ctx, Options{}, WithBackend(backend.Memory()), WithAggregateStats())
	require.NoError(t, err)
	defer func() { require.NoError(t, s.Close(ctx)) }()

	_, err = s.WriteMetrics(ctx, buildCorpus(corpusProfile{
		name: "c", series: 10, points: 50, interval: 15_000_000_000, pattern: patCounter,
	}, 1))
	require.NoError(t, err)
	require.NoError(t, mustEngine(s.engineFor("default")).Flush(ctx))

	req := fetch.Request{Start: 0, End: 1 << 62, Matchers: []fetch.Matcher{nameMatcher("bench.metric")}}

	whole, err := s.AggregateMetricsNamed(ctx, "default", req)
	require.NoError(t, err)
	require.Len(t, whole, 10)

	for _, step := range []int64{0, -1} {
		got, err := s.AggregateMetricsStepNamed(ctx, "default", req, step)
		require.NoError(t, err)
		require.Lenf(t, got, len(whole), "step=%d", step)

		for _, na := range got {
			require.Lenf(t, na.Buckets, 1, "step=%d ⇒ one bucket per series", step)

			want, ok := findAggregate(whole, na.Series.Hash())
			require.True(t, ok)
			assert.Equal(t, want, na.Buckets[0].SeriesAgg)
		}
	}
}

// TestAggregateMetricsStepNamedUnknownTenant checks an unknown tenant (and a closed store) yield no
// results rather than an error, matching the unstepped facades.
func TestAggregateMetricsStepNamedUnknownTenant(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	s, err := Open(ctx, Options{}, WithBackend(backend.Memory()), WithAggregateStats())
	require.NoError(t, err)

	req := fetch.Request{Start: 0, End: 1 << 62, Matchers: []fetch.Matcher{nameMatcher("bench.metric")}}

	got, err := s.AggregateMetricsStepNamed(ctx, "nosuch", req, aggStep)
	require.NoError(t, err)
	assert.Empty(t, got)

	require.NoError(t, s.Close(ctx))

	got, err = s.AggregateMetricsStepNamed(ctx, "default", req, aggStep)
	require.NoError(t, err)
	assert.Empty(t, got)
}

// TestAggregateMetricsWindowNamed checks the overlapping facade against the narrowed whole-range
// calls it replaces: each window must equal an aggregate over exactly (End-window, End], which is
// also what makes the half-open boundary visible.
func TestAggregateMetricsWindowNamed(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	s, err := Open(ctx, Options{}, WithBackend(backend.Memory()), WithAggregateStats())
	require.NoError(t, err)
	defer func() { require.NoError(t, s.Close(ctx)) }()

	const series = 8

	_, err = s.WriteMetrics(ctx, buildCorpus(corpusProfile{
		name: "c", series: series, points: 50, interval: 15_000_000_000, pattern: patCounter,
	}, 1))
	require.NoError(t, err)
	require.NoError(t, mustEngine(s.engineFor("default")).Flush(ctx))

	req := fetch.Request{Start: 0, End: 1 << 62, Matchers: []fetch.Matcher{nameMatcher("bench.metric")}}

	const window = 4 * aggStep // a 4x overlap at the step

	got, err := s.AggregateMetricsWindowNamed(ctx, "default", req, aggStep, window)
	require.NoError(t, err)
	require.Len(t, got, series, "one entry per series")

	for _, na := range got {
		require.NotEmpty(t, na.Windows)

		for i, w := range na.Windows {
			if i > 0 {
				assert.Greater(t, w.End, na.Windows[i-1].End, "windows sorted ascending by end")
			}

			assert.Zero(t, w.End%aggStep, "windows align to the absolute grid")

			// The N-call form this replaces: one whole-range aggregate over (End-window, End].
			perWindow, err := s.AggregateMetricsNamed(ctx, "default",
				fetch.Request{Start: w.End - window + 1, End: w.End, Matchers: req.Matchers})
			require.NoError(t, err)

			want, ok := findAggregate(perWindow, na.Series.Hash())
			require.Truef(t, ok, "series absent from the narrowed window ending at %d", w.End)
			assert.InDeltaf(t, want.Sum, w.Sum, 1e-6, "window ending %d sum", w.End)
			assert.Equalf(t, want.Count, w.Count, "window ending %d count", w.End)
			assert.InDeltaf(t, want.Min, w.Min, 0, "window ending %d min", w.End)
			assert.InDeltaf(t, want.Max, w.Max, 0, "window ending %d max", w.End)
		}
	}
}

// TestAggregateMetricsWindowNamedDegenerate checks window ≤ 0 reduces to the disjoint stepped form,
// and that a non-positive step is rejected rather than silently reinterpreted.
func TestAggregateMetricsWindowNamedDegenerate(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	s, err := Open(ctx, Options{}, WithBackend(backend.Memory()), WithAggregateStats())
	require.NoError(t, err)

	_, err = s.WriteMetrics(ctx, buildCorpus(corpusProfile{
		name: "c", series: 4, points: 50, interval: 15_000_000_000, pattern: patCounter,
	}, 1))
	require.NoError(t, err)
	require.NoError(t, mustEngine(s.engineFor("default")).Flush(ctx))

	req := fetch.Request{Start: 0, End: 1 << 62, Matchers: []fetch.Matcher{nameMatcher("bench.metric")}}

	stepped, err := s.AggregateMetricsStepNamed(ctx, "default", req, aggStep)
	require.NoError(t, err)

	windowed, err := s.AggregateMetricsWindowNamed(ctx, "default", req, aggStep, 0)
	require.NoError(t, err)
	require.Len(t, windowed, len(stepped))

	// Buckets are [b, b+step); windows at window == step are (t-step, t]. Both partition the samples
	// exactly once, so their folds agree — but they are not bucket-for-bucket identical: a sample
	// exactly on a grid multiple (the corpus epoch is one) closes the window ending there instead of
	// opening the bucket starting there, which can add one window at the head.
	folded := map[signal.SeriesID]engine.SeriesAgg{}
	count := map[signal.SeriesID]int{}

	for _, na := range windowed {
		var a engine.SeriesAgg
		for _, w := range na.Windows {
			foldAgg(&a, w.SeriesAgg)
		}

		folded[na.Series.Hash()] = a
		count[na.Series.Hash()] = len(na.Windows)
	}

	for _, na := range stepped {
		var a engine.SeriesAgg
		for _, b := range na.Buckets {
			foldAgg(&a, b.SeriesAgg)
		}

		got := folded[na.Series.Hash()]
		assert.Equal(t, a.Count, got.Count, "window == step partitions the samples exactly once")
		assert.InDelta(t, a.Sum, got.Sum, 1e-6)
		assert.InDelta(t, a.Min, got.Min, 0)
		assert.InDelta(t, a.Max, got.Max, 0)
		assert.LessOrEqual(t, count[na.Series.Hash()]-len(na.Buckets), 1, "at most one extra window at the head")
	}

	_, err = s.AggregateMetricsWindowNamed(ctx, "default", req, 0, aggStep)
	require.Error(t, err, "a stepped window has no evaluation grid without a step")

	// An unknown tenant, and a closed store, yield no results rather than an error.
	got, err := s.AggregateMetricsWindowNamed(ctx, "nosuch", req, aggStep, aggStep)
	require.NoError(t, err)
	assert.Empty(t, got)

	require.NoError(t, s.Close(ctx))

	got, err = s.AggregateMetricsWindowNamed(ctx, "default", req, aggStep, aggStep)
	require.NoError(t, err)
	assert.Empty(t, got)
}

// findAggregate looks a series' whole-range aggregate up by identity hash.
func findAggregate(list []SeriesAggregate, id signal.SeriesID) (engine.SeriesAgg, bool) {
	for i := range list {
		if list[i].Series.Hash() == id {
			return list[i].SeriesAgg, true
		}
	}

	return engine.SeriesAgg{}, false
}

// foldAgg merges one bucket's aggregate into a running total, mirroring what a caller summing the
// stepped result back to a whole-range answer would do.
func foldAgg(dst *engine.SeriesAgg, src engine.SeriesAgg) {
	if dst.Count == 0 {
		*dst = src

		return
	}

	dst.Count += src.Count
	dst.Sum += src.Sum
	dst.Min = min(dst.Min, src.Min)
	dst.Max = max(dst.Max, src.Max)
}

// BenchmarkAggregateMetricsStepNamed measures the stepped aggregate over a corpus laid out one part
// per bucket — the containment case the sidecar pushdown is built for. It fails if the run decodes
// a value column at all: with every part inside a single bucket, the answer comes from the
// per-part stats sidecars and the decode cache must stay untouched.
func BenchmarkAggregateMetricsStepNamed(b *testing.B) {
	ctx := context.Background()

	const (
		series  = 200
		buckets = 8
		points  = 5
	)

	s, err := Open(ctx, Options{}, WithBackend(backend.Memory()), WithAggregateStats(), WithDecodeCache(64<<20))
	require.NoError(b, err)
	defer func() { require.NoError(b, s.Close(ctx)) }()

	// One flush per bucket ⇒ each part lies wholly inside one step-aligned bucket.
	for k := range int64(buckets) {
		start := int64(1_600_000_000_000_000_000) + k*aggStep
		_, err := s.WriteMetrics(ctx, stepBucketCorpus(series, points, start, aggStep/(points+1)))
		require.NoError(b, err)
		require.NoError(b, mustEngine(s.engineFor("default")).Flush(ctx))
	}

	eng := mustEngine(s.engineFor("default"))
	req := fetch.Request{Start: 0, End: 1 << 62, Matchers: []fetch.Matcher{nameMatcher("bench.metric")}}

	got, err := s.AggregateMetricsStepNamed(ctx, "default", req, aggStep)
	require.NoError(b, err)
	require.Len(b, got, series)
	require.Len(b, got[0].Buckets, buckets, "one bucket per part")

	base, _ := eng.DecodeCacheStats()
	require.Zero(b, base.Hits+base.Misses, "contained parts answer from the sidecar, no value decode")

	b.ReportAllocs()

	for b.Loop() {
		if _, err := s.AggregateMetricsStepNamed(ctx, "default", req, aggStep); err != nil {
			b.Fatal(err)
		}
	}

	b.StopTimer()

	if st, _ := eng.DecodeCacheStats(); st.Hits+st.Misses != 0 {
		b.Fatalf("value column decoded: %d hits, %d misses", st.Hits, st.Misses)
	}
}

// stepBucketCorpus builds one gauge metric with `series` distinct label sets, each carrying
// `points` samples from start at interval — sized by the caller so the whole batch lands inside a
// single step bucket.
func stepBucketCorpus(series, points int, start, interval int64) metric.Metrics {
	var md metric.Metrics
	mt := md.AddResource().AddScope().AddMetric()
	mt.Name = []byte("bench.metric")
	mt.Kind = metric.KindGauge

	for s := range series {
		attrs := signal.NewAttributes(signal.KeyValue{
			Key: []byte("pod"), Value: signal.StringValue([]byte("pod-" + strconv.Itoa(s))),
		})

		for i := range points {
			p := mt.AddPoint()
			p.Ts = start + int64(i)*interval
			p.Attributes = attrs
			p.Value = float64(i)
		}
	}

	return md
}

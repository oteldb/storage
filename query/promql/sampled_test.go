package promql

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/util/annotations"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/query/fetch"
)

// sampledStream models what a tenant under a Sampling budget stores: of an original one-sample-per-
// second stream over 60s, the sampler kept every 4th point and stamped each with weight 4.
//
// values gives the kept point's value as a function of its second; deltaOnes yields a delta
// metric (each point an independent increment of 1), linearCounter a cumulative counter growing
// 10/s, so the unsampled truth is count=60, sum=60 and rate=10 respectively.
const (
	sampledEvery  = 4
	sampledWindow = 60
	sampledWeight = float64(sampledEvery)
)

func sampledStream(name string, weight float64, values func(sec int64) int64) *fetch.Batch {
	var samples [][2]int64
	for s := int64(sampledEvery); s <= sampledWindow; s += sampledEvery {
		samples = append(samples, [2]int64{s, values(s)})
	}

	b := series(name, "r1", samples...)
	b.ScaleFactors = make([]float64, len(b.Values))
	for i := range b.ScaleFactors {
		b.ScaleFactors[i] = weight
	}

	return b
}

func deltaOnes(int64) int64         { return 1 }
func linearCounter(sec int64) int64 { return 10 * sec }

func instantScalar(t *testing.T, q *Queryable, expr string, atSec int64) (float64, annotations.Annotations) {
	t.Helper()

	eng := promql.NewEngine(promql.EngineOpts{
		MaxSamples:    1_000_000,
		Timeout:       time.Minute,
		LookbackDelta: 5 * time.Minute,
	})
	pq, err := eng.NewInstantQuery(context.Background(), q, nil, expr, time.Unix(atSec, 0))
	require.NoError(t, err)
	t.Cleanup(pq.Close)

	res := pq.Exec(context.Background())
	require.NoError(t, res.Err)

	vec, err := res.Vector()
	require.NoError(t, err)
	require.Len(t, vec, 1, "%s: one series expected", expr)

	return vec[0].F, res.Warnings
}

func hasSampledWarning(w annotations.Annotations) bool {
	for _, err := range w.AsErrors() {
		if errors.Is(err, SampledWarning) {
			return true
		}
	}

	return false
}

// weightedFold is what a weight-aware embedder does with a [WeightedSeries]: Σw and Σw·v over the
// iterator's samples, weights read in parallel from ScaleFactors.
func weightedFold(t *testing.T, s storage.Series) (count, sum float64) {
	t.Helper()

	ws, ok := s.(WeightedSeries)
	require.True(t, ok, "every adapter series exposes its weights")
	sf := ws.ScaleFactors()

	it := s.Iterator(nil)
	for k := 0; it.Next() != chunkenc.ValNone; k++ {
		_, v := it.At()
		w := 1.0
		if sf != nil {
			w = sf[k]
		}
		count += w
		sum += w * v
	}
	require.NoError(t, it.Err())

	return count, sum
}

// TestSampledTenantOverTimeAggregates drives a real Prometheus engine over a tenant whose batches
// carry lossy-sampling weights (issue #474). A chunkenc.Iterator has no weight channel, so the
// adapter serves the kept rows as stored — count_over_time/sum_over_time see 15 rows, not the 60
// they stand for — and says so through [SampledWarning]; the weights themselves ride along on the
// series, where a weighted fold recovers the unsampled truth.
func TestSampledTenantOverTimeAggregates(t *testing.T) {
	t.Parallel()

	f := &fakeFetcher{batches: []*fetch.Batch{sampledStream("events", sampledWeight, deltaOnes)}}
	q := NewQueryable(f, "default")

	count, warns := instantScalar(t, q, `count_over_time(events[60s])`, sampledWindow)
	assert.InDelta(t, 15, count, 1e-9, "the kept rows are served unweighted, never fabricated")
	assert.True(t, hasSampledWarning(warns), "the engine surfaces the sampled warning: %v", warns.AsErrors())

	sum, warns := instantScalar(t, q, `sum_over_time(events[60s])`, sampledWindow)
	assert.InDelta(t, 15, sum, 1e-9, "the kept rows are served unweighted, never fabricated")
	assert.True(t, hasSampledWarning(warns), "the engine surfaces the sampled warning: %v", warns.AsErrors())

	ss := selectSeries(t, f)
	require.Len(t, ss, 1)
	wcount, wsum := weightedFold(t, ss[0])
	assert.InDelta(t, 60, wcount, 1e-9, "the weights recover the 60 original samples")
	assert.InDelta(t, 60, wsum, 1e-9, "the weights recover the 60 original increments")
}

// TestSampledTenantCumulativeRate checks the issue's rate claim: rate() over a cumulative counter
// reads the window's first/last points, so row sampling costs resolution, not mass — the slope is
// exact with or without the weights. The warning is still raised: the adapter cannot tell a
// cumulative counter from a delta sum at this layer.
func TestSampledTenantCumulativeRate(t *testing.T) {
	t.Parallel()

	f := &fakeFetcher{batches: []*fetch.Batch{sampledStream("requests_total", sampledWeight, linearCounter)}}
	q := NewQueryable(f, "default")

	rate, warns := instantScalar(t, q, `rate(requests_total[60s])`, sampledWindow)
	assert.InDelta(t, 10, rate, 1e-9, "a linear counter's rate is its slope regardless of which points survived")
	assert.True(t, hasSampledWarning(warns), "%v", warns.AsErrors())
}

// TestUnsampledSelectHasNoWarning pins the nil-means-weight-1 convention and its degenerate
// neighbors: no scale-factor column, an all-ones column and an all-zero column (the shape a
// producer bug emits for unsampled data) all serve raw values with no warning.
func TestUnsampledSelectHasNoWarning(t *testing.T) {
	t.Parallel()

	for name, b := range map[string]*fetch.Batch{
		"nil":   series("events", "r1", [2]int64{4, 1}, [2]int64{8, 1}),
		"ones":  sampledStream("events", 1, deltaOnes),
		"zeros": sampledStream("events", 0, deltaOnes),
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			f := &fakeFetcher{batches: []*fetch.Batch{b}}
			q, err := NewQueryable(f, "default").Querier(0, 10_000_000)
			require.NoError(t, err)
			t.Cleanup(func() { _ = q.Close() })

			ss := q.Select(context.Background(), false, nil)
			require.True(t, ss.Next())
			got, ok := ss.At().(WeightedSeries)
			require.True(t, ok)
			assert.Equal(t, b.ScaleFactors, got.ScaleFactors(), "weights pass through untouched")
			require.False(t, ss.Next())
			assert.Nil(t, ss.Warnings())
		})
	}

	sum, warns := instantScalar(t, NewQueryable(&fakeFetcher{batches: []*fetch.Batch{
		sampledStream("events", 0, deltaOnes),
	}}, "default"), `sum_over_time(events[60s])`, sampledWindow)
	assert.InDelta(t, 15, sum, 1e-9, "a zero weight is never multiplied in")
	assert.Empty(t, warns)
}

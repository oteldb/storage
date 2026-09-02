package cluster

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/signal"
	"github.com/oteldb/storage/signal/metric"
	"github.com/oteldb/storage/wal"
)

// buildMetrics makes one gauge per name, each with a single point at ts carrying a distinct
// attribute, so every name is its own series.
func buildMetrics(names ...string) metric.Metrics {
	var md metric.Metrics

	sm := md.AddResource().AddScope()
	for _, name := range names {
		mt := sm.AddMetric()
		mt.Name = []byte(name)
		mt.Kind = metric.KindGauge

		p := mt.AddPoint()
		p.Ts = 1_000
		p.Value = 1
	}

	return md
}

// replayed is what a framed payload decodes back to.
type replayed struct {
	series  map[signal.SeriesID]signal.Series
	samples int
}

func replay(t *testing.T, payload []byte) replayed {
	t.Helper()

	out := replayed{series: map[signal.SeriesID]signal.Series{}}
	require.NoError(t, wal.Replay(payload, wal.Handlers{
		OnSeries: func(id signal.SeriesID, s signal.Series) error {
			out.series[id] = s

			return nil
		},
		OnSamples: func(_ signal.SeriesID, ts []int64, _ []float64) error {
			out.samples += len(ts)

			return nil
		},
	}))

	return out
}

func TestFrameMetricsSingleShard(t *testing.T) {
	t.Parallel()

	f := FrameMetrics(buildMetrics("a", "b", "c"), 1, nil, nil)

	assert.Equal(t, 3, f.Emitted)
	assert.Zero(t, f.Shed)

	// One shard means the key is the bare tenant, and everything lands in it.
	require.Len(t, f.Shards, 1)
	require.Contains(t, f.Shards, DefaultTenant)

	got := replay(t, f.Shards[DefaultTenant])
	assert.Len(t, got.series, 3)
	assert.Equal(t, 3, got.samples)
}

func TestFrameMetricsRoutesEachSeriesToItsShard(t *testing.T) {
	t.Parallel()

	const shards = 4

	f := FrameMetrics(buildMetrics("a", "b", "c", "d", "e", "f", "g", "h"), shards, nil, nil)
	require.Equal(t, 8, f.Emitted)

	total := 0

	for key, payload := range f.Shards {
		assert.Equal(t, DefaultTenant, TenantOfShard(key))

		got := replay(t, payload)
		total += got.samples

		// Every series in a shard's payload must actually belong to that shard, or the frame is
		// routed to a primary that will not be asked for it on read.
		for id := range got.series {
			assert.Equal(t, key, ShardKeyOf(DefaultTenant, ShardOf(id, shards), shards))
		}
	}

	assert.Equal(t, 8, total, "no point is dropped or duplicated across shards")
}

func TestFrameMetricsTenantFunc(t *testing.T) {
	t.Parallel()

	// An empty id falls back to the default, so a partial routing function is safe.
	var calls int

	f := FrameMetrics(buildMetrics("a"), 1, func(signal.Resource, signal.Scope) signal.TenantID {
		calls++

		return ""
	}, nil)

	assert.Positive(t, calls)
	require.Contains(t, f.Shards, DefaultTenant)

	f = FrameMetrics(buildMetrics("a"), 1, func(signal.Resource, signal.Scope) signal.TenantID {
		return "acme"
	}, nil)
	require.Contains(t, f.Shards, signal.TenantID("acme"))
}

func TestFrameMetricsAdmitShedsWholeBatch(t *testing.T) {
	t.Parallel()

	f := FrameMetrics(buildMetrics("a", "b"), 1, nil, func(signal.TenantID, *metric.Batch) ([]float64, bool) {
		return nil, false
	})

	assert.Equal(t, 2, f.Emitted, "projection still saw them")
	assert.Equal(t, 2, f.Shed)
	assert.Empty(t, f.Shards, "a shed batch is never framed")
}

func TestFrameMetricsEmpty(t *testing.T) {
	t.Parallel()

	f := FrameMetrics(metric.Metrics{}, 4, nil, nil)

	assert.Zero(t, f.Emitted)
	assert.Zero(t, f.Shed)
	assert.Empty(t, f.Shards)
}

// TestFrameMetricsAdmitWeightsAreFramed pins that the sampler's verdict reaches the shard primary:
// a zero weight drops the point before framing, and a kept one is written as a scale-factor record
// (wal.WriteSamplesSF) rather than a plain one, because the primary is handed the encoded payload
// and cannot recover a weight that was never written.
func TestFrameMetricsAdmitWeightsAreFramed(t *testing.T) {
	t.Parallel()

	// Projection emits one single-point batch per metric, so the sampler is asked once per point:
	// keep the first and the last, drop the two in between.
	verdicts := [][]float64{{4}, {0}, {0}, {4}}
	call := 0

	f := FrameMetrics(buildMetrics("a", "b", "c", "d"), 1, nil,
		func(signal.TenantID, *metric.Batch) ([]float64, bool) {
			w := verdicts[call]
			call++

			return w, true
		})

	assert.Equal(t, 4, f.Emitted, "projection still saw them all")
	assert.Zero(t, f.Shed, "sampled out is not shed: the batch was admitted")
	assert.Equal(t, 2, f.Counts[DefaultTenant], "only the kept points are framed")

	var (
		sf   []float64
		ts   []int64
		sfID int
	)

	require.NoError(t, wal.Replay(f.Shards[DefaultTenant], wal.Handlers{
		OnSamples: func(signal.SeriesID, []int64, []float64) error {
			t.Fatal("a weighted point must not be framed as a plain samples record")

			return nil
		},
		OnSamplesSF: func(_ signal.SeriesID, rts []int64, _, rsf []float64) error {
			sfID++
			ts = append(ts, rts...)
			sf = append(sf, rsf...)

			return nil
		},
	}))

	assert.Equal(t, 2, sfID, "one record per kept point")
	assert.Len(t, ts, 2)
	assert.Equal(t, []float64{4, 4}, sf)
}

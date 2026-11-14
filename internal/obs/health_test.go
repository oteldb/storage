package obs_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/oteldb/storage/internal/obs"
)

func newMetered(t *testing.T) (*obs.Obs, func() metricdata.ResourceMetrics) {
	t.Helper()

	reader := sdkmetric.NewManualReader()

	o, err := obs.New(obs.Config{MeterProvider: sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))})
	require.NoError(t, err)

	return o, func() metricdata.ResourceMetrics {
		var rm metricdata.ResourceMetrics
		require.NoError(t, reader.Collect(context.Background(), &rm))

		return rm
	}
}

func TestNonReducingMerges(t *testing.T) {
	t.Parallel()
	o, collect := newMetered(t)
	o.Merge.NonReducing(t.Context(), "log")
	o.Merge.NonReducing(t.Context(), "log")
	o.Merge.NonReducing(t.Context(), "exemplar")
	rm := collect()
	assert.Equal(t, int64(2), counterSum(t, rm, "storage.merge.non_reducing", map[string]string{"signal": "log"}))
	assert.Equal(t, int64(1), counterSum(t, rm, "storage.merge.non_reducing", map[string]string{"signal": "exemplar"}))
}

func TestNumericMergePlanningMetrics(t *testing.T) {
	t.Parallel()
	o, collect := newMetered(t)
	o.Merge.Plan(t.Context(), "metric", "pool_busy", 32, 7, 100, 20, 30, 40, 50, 400)
	o.Merge.AdmissionRefused(t.Context(), "merge")
	o.Merge.AdmissionRefused(t.Context(), "build")
	o.Merge.Outputs(t.Context(), "metric", 2)
	rm := collect()
	require.EqualValues(t, 1, counterSum(t, rm, "storage.merge.plans", map[string]string{"signal": "metric", "outcome": "pool_busy"}))
	for _, pool := range []string{"merge", "build"} {
		require.EqualValues(t, 1, counterSum(t, rm, "storage.merge.admission_refused", map[string]string{"pool": pool}))
	}
	seen := make(map[string]int64)
	for _, scope := range rm.ScopeMetrics {
		for _, m := range scope.Metrics {
			if m.Name != "storage.merge.plan.bytes" && m.Name != "storage.merge.plan.parts" && m.Name != "storage.merge.parts_out" {
				continue
			}
			h, ok := m.Data.(metricdata.Histogram[int64])
			require.True(t, ok)
			for _, point := range h.DataPoints {
				component, _ := point.Attributes.Value("component")
				seen[m.Name+"/"+component.AsString()] = point.Sum
			}
		}
	}
	require.Equal(t, map[string]int64{
		"storage.merge.plan.parts/candidates": 32, "storage.merge.plan.parts/selected": 7,
		"storage.merge.plan.bytes/input_resident": 100, "storage.merge.plan.bytes/open_transient": 20,
		"storage.merge.plan.bytes/series_scratch": 30, "storage.merge.plan.bytes/metadata": 40,
		"storage.merge.plan.bytes/output_floor": 50, "storage.merge.plan.bytes/allowance": 400,
		"storage.merge.parts_out/": 2,
	}, seen)
}

func TestCanonicalVerificationCacheMetrics(t *testing.T) {
	t.Parallel()
	o, collect := newMetered(t)
	o.Flush.VerificationCache(t.Context(), 3, 7)
	rm := collect()
	require.EqualValues(t, 3, counterSum(t, rm, "storage.series.verification_cache.requests", map[string]string{"result": "hit"}))
	require.EqualValues(t, 7, counterSum(t, rm, "storage.series.verification_cache.requests", map[string]string{"result": "miss"}))
}

func TestFlushReservationMetrics(t *testing.T) {
	t.Parallel()
	o, collect := newMetered(t)
	o.Flush.Reservation(t.Context(), "shape", 16<<20, 448<<20, time.Millisecond)
	o.Flush.OutputSeries(t.Context(), 8192)
	rm := collect()
	values := make(map[string]int64)
	for _, scope := range rm.ScopeMetrics {
		for _, m := range scope.Metrics {
			if m.Name != "storage.flush.reservation.bytes" && m.Name != "storage.flush.output.series" {
				continue
			}
			h, ok := m.Data.(metricdata.Histogram[int64])
			require.True(t, ok)
			for _, p := range h.DataPoints {
				component, _ := p.Attributes.Value("component")
				values[m.Name+"/"+component.AsString()] = p.Sum
			}
		}
	}
	require.Equal(t, map[string]int64{"storage.flush.reservation.bytes/reserved": 16 << 20, "storage.flush.reservation.bytes/target": 448 << 20, "storage.flush.output.series/": 8192}, values)
}

func TestFlushBuildMetrics(t *testing.T) {
	t.Parallel()
	o, collect := newMetered(t)
	o.Flush.BuildPlan(t.Context(), 2, 4)
	o.Flush.BuildFallback(t.Context(), "workspace")
	rm := collect()
	require.EqualValues(t, 1, counterSum(t, rm, "storage.flush.build.fallbacks", map[string]string{"reason": "workspace"}))
	values := make(map[string]int64)
	for _, scope := range rm.ScopeMetrics {
		for _, m := range scope.Metrics {
			if m.Name != "storage.flush.build.workers" && m.Name != "storage.flush.build.jobs" {
				continue
			}
			h, ok := m.Data.(metricdata.Histogram[int64])
			require.True(t, ok)
			for _, p := range h.DataPoints {
				values[m.Name] += p.Sum
			}
		}
	}
	require.Equal(t, map[string]int64{"storage.flush.build.workers": 2, "storage.flush.build.jobs": 4}, values)
}

func TestCorruptionDetected(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	o, collect := newMetered(t)

	o.Corruption.Detected(ctx, "bloom", obs.CorruptTolerated)
	o.Corruption.Detected(ctx, "bloom", obs.CorruptTolerated)
	o.Corruption.Detected(ctx, "wal", obs.CorruptFatal)

	rm := collect()
	assert.Equal(t, int64(2), counterSum(t, rm, "storage.corruption.detected",
		map[string]string{"component": "bloom", "disposition": "tolerated"}))
	assert.Equal(t, int64(1), counterSum(t, rm, "storage.corruption.detected",
		map[string]string{"component": "wal", "disposition": "fatal"}))
}

func TestClusterFencing(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	o, collect := newMetered(t)

	o.Cluster.Record(ctx, false, true, 0)
	o.Cluster.PrimaryRefused(ctx, "log")
	o.Cluster.PrimaryRefused(ctx, "log")

	rm := collect()
	assert.Equal(t, int64(1), gaugeValue(t, rm, "storage.cluster.fenced", nil))
	assert.Zero(t, gaugeValue(t, rm, "storage.cluster.self_absent", nil))
	assert.Equal(t, int64(2), counterSum(t, rm, "storage.cluster.primary_refusals", map[string]string{"signal": "log"}))
}

func TestWALSyncFailed(t *testing.T) {
	t.Parallel()
	o, collect := newMetered(t)

	o.WAL.SyncFailed(t.Context(), 3, true)
	rm := collect()
	assert.Equal(t, int64(3), counterSum(t, rm, "storage.wal.sync_failures", nil))
	assert.Equal(t, int64(1), gaugeValue(t, rm, "storage.wal.sync_failing", nil))

	o.WAL.SyncFailed(t.Context(), 0, false)
	rm = collect()
	assert.Equal(t, int64(3), counterSum(t, rm, "storage.wal.sync_failures", nil))
	assert.Zero(t, gaugeValue(t, rm, "storage.wal.sync_failing", nil))
}

func TestPartsOrphansSwept(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	o, collect := newMetered(t)

	o.Parts.OrphansSwept(ctx, "trace", 4)
	o.Parts.OrphansSwept(ctx, "trace", 0)

	assert.Equal(t, int64(4), counterSum(t, collect(), "storage.parts.orphans_swept", map[string]string{"signal": "trace"}))
}

func TestPartsOrphansDeferred(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	o, collect := newMetered(t)

	o.Parts.OrphansDeferred(ctx, "log", 3)
	o.Parts.OrphansDeferred(ctx, "log", 0)

	assert.Equal(t, int64(3), counterSum(t, collect(), "storage.parts.orphans_deferred", map[string]string{"signal": "log"}))
}

func TestHealthNop(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	o := obs.NewNop()

	o.Corruption.Detected(ctx, "part", obs.CorruptFatal)
	o.Cluster.Record(ctx, true, true, 1)
	o.Cluster.PrimaryRefused(ctx, "metric")
	o.WAL.SyncFailed(ctx, 1, true)
	o.Parts.OrphansSwept(ctx, "metric", 1)
	o.Parts.OrphansDeferred(ctx, "metric", 1)
}

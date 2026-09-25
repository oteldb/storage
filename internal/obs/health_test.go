package obs_test

import (
	"context"
	"testing"

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

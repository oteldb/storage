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

// TestDiskRecords pins the disk-pressure surface: the gauge an alert stands on, and the counter
// that says what the state is costing.
func TestDiskRecords(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))

	o, err := obs.New(obs.Config{MeterProvider: mp})
	require.NoError(t, err)

	o.Disk.Record(ctx, "metric", true, 7)
	o.Disk.Record(ctx, "log", false, 0)

	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(ctx, &rm))

	assert.Equal(t, int64(1), gaugeValue(t, rm, "storage.disk.out_of_space", map[string]string{"signal": "metric"}))
	assert.Zero(t, gaugeValue(t, rm, "storage.disk.out_of_space", map[string]string{"signal": "log"}))
	assert.Equal(t, int64(7),
		counterSum(t, rm, "storage.disk.rejected_writes", map[string]string{"signal": "metric"}))
}

// TestDiskNop confirms the unconfigured handle records without a meter.
func TestDiskNop(t *testing.T) {
	t.Parallel()

	obs.NewNop().Disk.Record(context.Background(), "metric", true, 1)
}

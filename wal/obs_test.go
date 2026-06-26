package wal_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/oteldb/storage/internal/obs"
	"github.com/oteldb/storage/signal"
	"github.com/oteldb/storage/wal"
)

func counterSum(t *testing.T, rm metricdata.ResourceMetrics, name string) int64 {
	t.Helper()
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != name {
				continue
			}
			sum, ok := m.Data.(metricdata.Sum[int64])
			require.True(t, ok)
			var total int64
			for _, dp := range sum.DataPoints {
				total += dp.Value
			}
			return total
		}
	}
	return 0
}

func TestWALMetricsEmitted(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	o, err := obs.New(obs.Config{MeterProvider: mp})
	require.NoError(t, err)

	w, err := wal.Create(t.TempDir(), 0)
	require.NoError(t, err)
	t.Cleanup(func() { _ = w.Close() })
	w.SetObs(o.WAL)
	w.SetSync(true) // fsync per write

	require.NoError(t, w.WriteSamples(signal.SeriesID{Lo: 1}, []int64{1}, []float64{1}))
	require.NoError(t, w.Sync())

	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(ctx, &rm))

	assert.Positive(t, counterSum(t, rm, "storage.wal.appends"))
	assert.Positive(t, counterSum(t, rm, "storage.wal.fsyncs"))
}

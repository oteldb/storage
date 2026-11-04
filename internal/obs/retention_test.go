package obs_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/oteldb/storage/internal/obs"
)

func TestRetentionInstruments(t *testing.T) {
	t.Parallel()
	reader := sdkmetric.NewManualReader()
	o, err := obs.New(obs.Config{MeterProvider: sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))})
	require.NoError(t, err)
	ctx := t.Context()
	o.Retention.Dropped(ctx, "log", 2, 123)
	o.Retention.Dropped(ctx, "log", 0, 500)
	o.Retention.Rewritten(ctx, "log")
	o.Retention.Lag(ctx, "log", 1.5)
	o.Retention.Lag(ctx, "trace", -1)
	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(ctx, &rm))
	attrs := map[string]string{"signal": "log"}
	require.EqualValues(t, 2, counterSum(t, rm, "storage.retention.dropped_parts", attrs))
	require.EqualValues(t, 123, counterSum(t, rm, "storage.retention.dropped_bytes", attrs))
	require.EqualValues(t, 1, counterSum(t, rm, "storage.retention.rewrites", attrs))
	var found int
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != "storage.retention.lag_seconds" {
				continue
			}
			gauge, ok := m.Data.(metricdata.Gauge[float64])
			require.True(t, ok)
			for _, dp := range gauge.DataPoints {
				require.Len(t, dp.Attributes.ToSlice(), 1)
				if attrsContain(dp.Attributes.ToSlice(), attrs) {
					require.InDelta(t, 1.5, dp.Value, 1e-12)
				} else {
					require.Zero(t, dp.Value)
				}
				found++
			}
		}
	}
	require.Equal(t, 2, found)
	nop := obs.NewNop()
	nop.Retention.Dropped(ctx, "log", 1, 20)
	nop.Retention.Rewritten(ctx, "log")
	nop.Retention.Lag(ctx, "log", 10)
}

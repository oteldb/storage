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

// f64GaugeValue returns the value of float64 gauge `name` at the data point whose attributes
// include every (key=value) in want.
func f64GaugeValue(t *testing.T, rm metricdata.ResourceMetrics, name string, want map[string]string) float64 {
	t.Helper()

	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != name {
				continue
			}

			g, ok := m.Data.(metricdata.Gauge[float64])
			require.Truef(t, ok, "%s is not a float64 gauge", name)

			for _, dp := range g.DataPoints {
				if attrsContain(dp.Attributes.ToSlice(), want) {
					return dp.Value
				}
			}
		}
	}

	t.Fatalf("gauge %q not found", name)

	return 0
}

// TestHeadGaugesRecord pins the unflushed-side surface: the state a stalled flush lives in, which
// no storage.parts.* gauge reaches.
func TestHeadGaugesRecord(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))

	o, err := obs.New(obs.Config{MeterProvider: mp})
	require.NoError(t, err)

	o.Head.Record(ctx, "metric", 32<<20, 1_000, 40, 5<<20, 90*time.Second)
	o.Head.Record(ctx, "log", 1<<20, 12, 3, 4096, 0)

	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(ctx, &rm))

	metricSig := map[string]string{"signal": "metric"}
	assert.Equal(t, int64(32<<20), gaugeValue(t, rm, "storage.head.bytes", metricSig))
	assert.Equal(t, int64(1_000), gaugeValue(t, rm, "storage.head.items", metricSig))
	assert.Equal(t, int64(40), gaugeValue(t, rm, "storage.head.series", metricSig))
	assert.Equal(t, int64(5<<20), gaugeValue(t, rm, "storage.head.identity_bytes", metricSig))
	assert.InDelta(t, 90.0, f64GaugeValue(t, rm, "storage.head.age", metricSig), 0.001)

	logSig := map[string]string{"signal": "log"}
	assert.Equal(t, int64(1<<20), gaugeValue(t, rm, "storage.head.bytes", logSig))
	assert.Zero(t, f64GaugeValue(t, rm, "storage.head.age", logSig))
}

// TestWALGaugesRecord pins the durability backlog: segments accumulate exactly while flushes are
// not retiring them.
func TestWALGaugesRecord(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))

	o, err := obs.New(obs.Config{MeterProvider: mp})
	require.NoError(t, err)

	o.WAL.Record(ctx, "trace", 6, 48<<20)

	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(ctx, &rm))

	traceSig := map[string]string{"signal": "trace"}
	assert.Equal(t, int64(6), gaugeValue(t, rm, "storage.wal.segments", traceSig))
	assert.Equal(t, int64(48<<20), gaugeValue(t, rm, "storage.wal.bytes", traceSig))
}

// TestHeadNop confirms the unconfigured handle records without a meter.
func TestHeadNop(t *testing.T) {
	t.Parallel()

	o := obs.NewNop()
	o.Head.Record(context.Background(), "metric", 1, 1, 1, 1, time.Second)
	o.WAL.Record(context.Background(), "metric", 1, 1)
}

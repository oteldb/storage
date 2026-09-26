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

// gaugeValue returns the value of gauge `name` at the data point whose attributes include every
// (key=value) in want.
func gaugeValue(t *testing.T, rm metricdata.ResourceMetrics, name string, want map[string]string) int64 {
	t.Helper()

	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != name {
				continue
			}

			g, ok := m.Data.(metricdata.Gauge[int64])
			require.Truef(t, ok, "%s is not an int64 gauge", name)

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

func TestPartsGaugesRecord(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))

	o, err := obs.New(obs.Config{MeterProvider: mp})
	require.NoError(t, err)

	o.Parts.Record(ctx, "metric", obs.PartShape{
		Total: 9, Sealed: 4, Backlog: 5, Candidates: 2, ForceCandidates: 3, CapBytes: 64 << 20, Bytes: 700 << 20,
	})
	o.Parts.Record(ctx, "log", obs.PartShape{Total: 3, Backlog: 3, CapBytes: 8 << 20, Bytes: 12 << 20})

	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(ctx, &rm))

	metricSig := map[string]string{"signal": "metric"}
	assert.Equal(t, int64(9), gaugeValue(t, rm, "storage.parts.total", metricSig))
	assert.Equal(t, int64(4), gaugeValue(t, rm, "storage.parts.sealed", metricSig))
	assert.Equal(t, int64(5), gaugeValue(t, rm, "storage.parts.merge_backlog", metricSig))
	assert.Equal(t, int64(2), gaugeValue(t, rm, "storage.parts.merge_candidates", metricSig))
	assert.Equal(t, int64(3), gaugeValue(t, rm, "storage.parts.merge_force_candidates", metricSig))
	assert.Equal(t, int64(64<<20), gaugeValue(t, rm, "storage.merge.cap_bytes", metricSig))
	assert.Equal(t, int64(700<<20), gaugeValue(t, rm, "storage.parts.bytes", metricSig))

	logSig := map[string]string{"signal": "log"}
	assert.Equal(t, int64(3), gaugeValue(t, rm, "storage.parts.total", logSig))
	assert.Zero(t, gaugeValue(t, rm, "storage.parts.merge_candidates", logSig))
}

// TestPartsGaugesNop confirms the unconfigured handle records without a meter.
func TestPartsGaugesNop(t *testing.T) {
	t.Parallel()

	obs.NewNop().Parts.Record(context.Background(), "metric", obs.PartShape{Total: 1, Backlog: 1})
}

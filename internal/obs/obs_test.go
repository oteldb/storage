package obs_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/oteldb/storage/internal/obs"
)

// TestNopIsUsable confirms the default (unconfigured) handle is fully usable and a no-op: a span
// starts and ends, and the admission metrics record without panicking or allocating a meter.
func TestNopIsUsable(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	o := obs.NewNop()
	require.NotNil(t, o.Log)

	_, span := o.Tracer.Start(ctx, "noop")
	span.End()

	// No meter configured ⇒ these are no-ops, but must not panic.
	o.Admission.Accepted(ctx, 10, "metric")
	o.Admission.Rejected(ctx, 3, "metric", "max_series")
	o.Admission.SampledDropped(ctx, 2, "metric")
}

// counterSum returns the total value of counter `name` across data points whose attributes include
// every (key=value) in want.
func counterSum(t *testing.T, rm metricdata.ResourceMetrics, name string, want map[string]string) int64 {
	t.Helper()

	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != name {
				continue
			}

			sum, ok := m.Data.(metricdata.Sum[int64])
			require.Truef(t, ok, "%s is not an int64 sum", name)

			var total int64

			for _, dp := range sum.DataPoints {
				if attrsContain(dp.Attributes.ToSlice(), want) {
					total += dp.Value
				}
			}

			return total
		}
	}

	t.Fatalf("counter %q not found", name)

	return 0
}

func attrsContain(set []attribute.KeyValue, want map[string]string) bool {
	for k, v := range want {
		found := false

		for _, a := range set {
			if string(a.Key) == k && a.Value.AsString() == v {
				found = true

				break
			}
		}

		if !found {
			return false
		}
	}

	return true
}

func TestAdmissionCountersRecord(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))

	o, err := obs.New(obs.Config{MeterProvider: mp})
	require.NoError(t, err)

	o.Admission.Accepted(ctx, 100, "metric")
	o.Admission.Accepted(ctx, 5, "log")
	o.Admission.Rejected(ctx, 7, "metric", "max_series")
	o.Admission.Rejected(ctx, 3, "metric", "rate_limit")
	o.Admission.SampledDropped(ctx, 12, "metric")
	o.Admission.Accepted(ctx, 0, "metric") // ignored

	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(ctx, &rm))

	assert.Equal(t, int64(100), counterSum(t, rm, "storage.ingest.accepted", map[string]string{"signal": "metric"}))
	assert.Equal(t, int64(5), counterSum(t, rm, "storage.ingest.accepted", map[string]string{"signal": "log"}))
	assert.Equal(t, int64(7), counterSum(t, rm, "storage.ingest.rejected", map[string]string{"signal": "metric", "reason": "max_series"}))
	assert.Equal(t, int64(3), counterSum(t, rm, "storage.ingest.rejected", map[string]string{"signal": "metric", "reason": "rate_limit"}))
	assert.Equal(t, int64(12), counterSum(t, rm, "storage.ingest.sampled_dropped", map[string]string{"signal": "metric"}))
}

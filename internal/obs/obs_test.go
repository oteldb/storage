package obs_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"

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

// TestLoggerTraceCorrelation confirms Obs.Logger routes through zctx: when a valid span is in ctx,
// the returned logger stamps trace_id and span_id onto every line (the embedder's correlation key).
func TestLoggerTraceCorrelation(t *testing.T) {
	t.Parallel()

	core, logs := observer.New(zap.DebugLevel)
	o, err := obs.New(obs.Config{Logger: zap.New(core)})
	require.NoError(t, err)

	traceID := trace.TraceID{0x1, 0x2, 0x3, 0x4, 0x5, 0x6, 0x7, 0x8, 0x9, 0xa, 0xb, 0xc, 0xd, 0xe, 0xf, 0x10}
	spanID := trace.SpanID{0x1, 0x2, 0x3, 0x4, 0x5, 0x6, 0x7, 0x8}
	sc := trace.NewSpanContext(trace.SpanContextConfig{TraceID: traceID, SpanID: spanID, TraceFlags: trace.FlagsSampled})
	ctx := trace.ContextWithSpanContext(context.Background(), sc)

	o.Logger(ctx).Info("hello")

	require.Equal(t, 1, logs.Len())
	fields := logs.All()[0].ContextMap()
	assert.Equal(t, traceID.String(), fields["trace_id"], "trace_id stamped from the active span")
	assert.Equal(t, spanID.String(), fields["span_id"], "span_id stamped from the active span")

	// Without a span, no correlation fields are added (base logger passes through).
	core2, logs2 := observer.New(zap.DebugLevel)
	o2, err := obs.New(obs.Config{Logger: zap.New(core2)})
	require.NoError(t, err)
	o2.Logger(context.Background()).Info("plain")
	require.Equal(t, 1, logs2.Len())
	assert.NotContains(t, logs2.All()[0].ContextMap(), "trace_id")
}

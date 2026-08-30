package storage

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/oteldb/storage/signal"
)

// TestMaintainPublishesHeadGauges pins the unflushed side of the cycle: the head is measured before
// the flushes drain it, so the gauges report what accumulated between cycles rather than the empty
// head a healthy flush leaves behind.
func TestMaintainPublishesHeadGauges(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	reader := sdkmetric.NewManualReader()
	s, err := InMemory(WithMeterProvider(sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))))
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close(ctx) })

	_, err = s.WriteMetrics(ctx, gaugeBatch("api", "m1", []int64{1, 2, 3}, []float64{1, 2, 3}))
	require.NoError(t, err)
	_, err = s.WriteLogs(ctx, logBatch("api", [3]any{1, 9, "hello"}))
	require.NoError(t, err)

	require.NoError(t, s.Admin().MaintainNow(ctx))

	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(ctx, &rm))

	metricSig := map[string]string{"signal": "metric"}
	assert.Positive(t, partGauge(t, rm, "storage.head.bytes", metricSig),
		"the head is measured before the cycle flushes it")
	assert.Equal(t, int64(3), partGauge(t, rm, "storage.head.items", metricSig))
	assert.Equal(t, int64(1), partGauge(t, rm, "storage.head.series", metricSig))
	assert.Positive(t, partGauge(t, rm, "storage.head.identity_bytes", metricSig))

	logSig := map[string]string{"signal": "log"}
	assert.Equal(t, int64(1), partGauge(t, rm, "storage.head.items", logSig))

	// The ephemeral in-memory engine has no WAL, so its backlog is zero rather than absent: the
	// gauge must still be published, or a dashboard cannot tell "no WAL" from "not instrumented".
	assert.Zero(t, partGauge(t, rm, "storage.wal.segments", metricSig))
}

// TestMaintainPublishesPartBytes covers the part-size gauge that makes a rising part count
// readable: whether the parts themselves grew, or a merge stopped taking them.
func TestMaintainPublishesPartBytes(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	reader := sdkmetric.NewManualReader()
	s, err := InMemory(WithMeterProvider(sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))))
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close(ctx) })

	_, err = s.WriteMetrics(ctx, gaugeBatch("api", "m1", []int64{1, 2, 3}, []float64{1, 2, 3}))
	require.NoError(t, err)
	require.NoError(t, s.Admin().Flush(ctx, "default", signal.Metric))
	require.NoError(t, s.Admin().MaintainNow(ctx))

	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(ctx, &rm))

	want := map[string]string{"signal": "metric"}
	assert.Positive(t, partGauge(t, rm, "storage.parts.total", want))
	assert.Positive(t, partGauge(t, rm, "storage.parts.bytes", want),
		"the flushed parts report what they occupy, not just how many they are")
	assert.Positive(t, sumCounter(t, rm, "storage.flush.bytes", want),
		"the flush reports the bytes it wrote — the denominator of write amplification")
}

// TestMergeReportsBytes pins the other half of write amplification: what a merge read and rewrote,
// which parts_in alone never shows.
func TestMergeReportsBytes(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	reader := sdkmetric.NewManualReader()
	s, err := InMemory(WithMeterProvider(sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))))
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close(ctx) })

	for i := range 2 {
		_, err = s.WriteMetrics(ctx, gaugeBatch("api", "m1", []int64{int64(i) + 1}, []float64{1}))
		require.NoError(t, err)
		require.NoError(t, s.Admin().Flush(ctx, "default", signal.Metric))
	}

	require.NoError(t, s.Admin().CompactNow(ctx, "default", signal.Metric))

	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(ctx, &rm))

	want := map[string]string{"signal": "metric"}
	assert.Positive(t, sumCounter(t, rm, "storage.merge.bytes_in", want))
	assert.Positive(t, sumCounter(t, rm, "storage.merge.bytes_out", want))
}

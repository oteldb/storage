package storage

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/engine"
	"github.com/oteldb/storage/signal"
)

// TestEfficiencyStats checks the data-shape drill-down on a single in-memory node: point
// counts, stored bytes, bytes-per-point, and the metric compression ratio (logical = 16 B per
// sample) after a flush.
func TestEfficiencyStats(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	s, err := Open(ctx, Options{})
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close(ctx) })

	// A compressible ramp: 4096 samples, one series.
	n := 4096
	ts := make([]int64, n)
	vals := make([]float64, n)
	for i := range ts {
		ts[i] = int64(i + 1)
		vals[i] = float64(i)
	}

	_, err = s.WriteMetrics(ctx, gaugeBatch("api", "http.requests", ts, vals))
	require.NoError(t, err)

	_, err = s.WriteLogs(ctx, logBatch("api", [3]any{100, 9, "first"}, [3]any{200, 17, "second"}))
	require.NoError(t, err)

	s.maintain(ctx) // flush both signals

	stats, err := s.EfficiencyStats(ctx)
	require.NoError(t, err)
	require.Len(t, stats, 1, "one tenant")
	te := stats[0]
	assert.Equal(t, signal.TenantID("default"), te.Tenant)
	require.Len(t, te.Signals, 2, "metrics + logs")

	var m, l *SignalEfficiency
	for i := range te.Signals {
		switch te.Signals[i].Signal {
		case signal.Metric:
			m = &te.Signals[i]
		case signal.Log:
			l = &te.Signals[i]
		default: // no trace/profile data written in this test
		}
	}
	require.NotNil(t, m)
	require.NotNil(t, l)

	// Metrics: exact logical size, sane derived ratios.
	assert.Equal(t, int64(n), m.Points)
	assert.Equal(t, int64(1), m.Series)
	assert.Equal(t, 1, m.Parts)
	assert.Positive(t, m.StoredBytes)
	assert.Equal(t, int64(n)*engine.SampleBytes, m.LogicalBytes)
	assert.InDelta(t, float64(m.StoredBytes)/float64(n), m.BytesPerPoint, 1e-9)
	assert.Greater(t, m.CompressionRatio, 1.0, "a ramp compresses well below 16 B/sample")

	// Logs: counts and bytes-per-record present; no logical size ⇒ no ratio.
	assert.Equal(t, int64(2), l.Points)
	assert.Positive(t, l.StoredBytes)
	assert.Positive(t, l.BytesPerPoint)
	assert.Zero(t, l.LogicalBytes)
	assert.Zero(t, l.CompressionRatio)
}

// TestInspectMaintenanceStats pins the maintenance-loop section: cycles count up and the
// last-cycle fields are stamped.
func TestInspectMaintenanceStats(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	s, err := Open(ctx, Options{})
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close(ctx) })

	_, err = s.WriteMetrics(ctx, gaugeBatch("api", "m", []int64{1, 2}, []float64{1, 2}))
	require.NoError(t, err)

	before := s.Inspect().Maintenance.Cycles
	s.maintain(ctx)
	ms := s.Inspect().Maintenance
	assert.Equal(t, before+1, ms.Cycles, "a cycle completed")
	assert.Positive(t, ms.LastCycleStartUnixNano)
	assert.Positive(t, ms.LastCycleTasks, "the metric engine produced a task")
}

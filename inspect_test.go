package storage

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/signal"
)

// findTenant returns the named tenant's stats (and whether present).
func findTenant(ss StoreStats, tid signal.TenantID) (TenantStats, bool) {
	for _, t := range ss.Tenants {
		if t.Tenant == tid {
			return t, true
		}
	}

	return TenantStats{}, false
}

func findSignal(ts TenantStats, sig signal.Signal) (SignalStats, bool) {
	for _, s := range ts.Signals {
		if s.Signal == sig {
			return s, true
		}
	}

	return SignalStats{}, false
}

func TestInspectInMemory(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s, err := InMemory()
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close(ctx) })

	// Empty store: a usable, empty snapshot.
	empty := s.Inspect()
	assert.Empty(t, empty.Tenants)
	assert.Nil(t, empty.Cluster, "single-node ⇒ no cluster section")

	// Ingest metrics (two series) and logs (one stream) for the default tenant.
	_, err = s.WriteMetrics(ctx, gaugeBatch("api", "m1", []int64{100, 200}, []float64{1, 2}))
	require.NoError(t, err)
	_, err = s.WriteMetrics(ctx, gaugeBatch("api", "m2", []int64{300}, []float64{3}))
	require.NoError(t, err)
	_, err = s.WriteLogs(ctx, logBatch("api", [3]any{150, 9, "hello"}))
	require.NoError(t, err)

	ss := s.Inspect()
	require.Len(t, ss.Tenants, 1)

	ts, ok := findTenant(ss, "default")
	require.True(t, ok)

	mstat, ok := findSignal(ts, signal.Metric)
	require.True(t, ok, "metrics signal present")
	assert.Equal(t, int64(2), mstat.Series, "two distinct metric series")
	assert.Equal(t, int64(3), mstat.HeadItems, "three samples buffered in the head")
	assert.Positive(t, mstat.HeadBytes)
	assert.Equal(t, int64(0), mstat.MinTimeUnixNano, "no flushed parts yet ⇒ no part-based min")
	assert.Equal(t, int64(300), mstat.MaxTimeUnixNano, "newest sample (from the head)")

	lstat, ok := findSignal(ts, signal.Log)
	require.True(t, ok, "logs signal present")
	assert.Equal(t, int64(1), lstat.Series, "one log stream")
	assert.Equal(t, int64(1), lstat.HeadItems, "one log record buffered")

	// Admission reflects the accepted writes (no rejections here).
	assert.Equal(t, int64(0), ts.Admission.Rejected())
	assert.Positive(t, ts.Admission.Accepted)
}

func TestInspectReflectsFlush(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s, err := InMemory()
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close(ctx) })

	_, err = s.WriteMetrics(ctx, gaugeBatch("api", "m1", []int64{100, 200}, []float64{1, 2}))
	require.NoError(t, err)

	before, _ := findSignal(mustTenant(t, s.Inspect(), "default"), signal.Metric)
	assert.Equal(t, 0, before.Parts, "no parts before flush")
	assert.Equal(t, int64(2), before.HeadItems)

	// Flush the engine; the head drains into a part, which Inspect reflects.
	eng, err := s.engineFor("default")
	require.NoError(t, err)
	require.NoError(t, eng.Flush(ctx))

	after, _ := findSignal(mustTenant(t, s.Inspect(), "default"), signal.Metric)
	assert.Equal(t, 1, after.Parts, "one part after flush")
	assert.Equal(t, int64(0), after.HeadItems, "head drained")
	assert.Equal(t, int64(100), after.MinTimeUnixNano, "part time bounds")
	assert.Equal(t, int64(200), after.MaxTimeUnixNano)
	assert.Equal(t, int64(1), after.Series, "series index spans flushed data (one series, two samples)")
}

func mustTenant(t *testing.T, ss StoreStats, tid signal.TenantID) TenantStats {
	t.Helper()

	ts, ok := findTenant(ss, tid)
	require.True(t, ok)

	return ts
}

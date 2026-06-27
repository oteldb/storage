package storage

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/signal"
)

func TestAdminFlushAndCompact(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s, err := InMemory()
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close(ctx) })

	_, err = s.WriteMetrics(ctx, gaugeBatch("api", "m1", []int64{100, 200}, []float64{1, 2}))
	require.NoError(t, err)

	// Flush drains the head into a part.
	require.NoError(t, s.Admin().Flush(ctx, "default", signal.Metric))

	st, _ := findSignal(mustTenant(t, s.Inspect(), "default"), signal.Metric)
	assert.Equal(t, 1, st.Parts, "flush produced a part")
	assert.Equal(t, int64(0), st.HeadItems, "head drained")

	// A second part, then Compact merges them into one.
	_, err = s.WriteMetrics(ctx, gaugeBatch("api", "m1", []int64{300}, []float64{3}))
	require.NoError(t, err)
	require.NoError(t, s.Admin().Flush(ctx, "default", signal.Metric))

	st, _ = findSignal(mustTenant(t, s.Inspect(), "default"), signal.Metric)
	require.Equal(t, 2, st.Parts, "two parts before compaction")

	require.NoError(t, s.Admin().Compact(ctx, "default", signal.Metric))

	st, _ = findSignal(mustTenant(t, s.Inspect(), "default"), signal.Metric)
	assert.Equal(t, 1, st.Parts, "compaction merged the parts")
}

func TestAdminFlushUnknownIsNoOp(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s, err := InMemory()
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close(ctx) })

	// No data ingested for this tenant/signal: a no-op, not an error.
	require.NoError(t, s.Admin().Flush(ctx, "nobody", signal.Metric))
	require.NoError(t, s.Admin().Compact(ctx, "nobody", signal.Log))
}

func TestAdminMaintainNowFlushesAllSignals(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s, err := InMemory()
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close(ctx) })

	_, err = s.WriteMetrics(ctx, gaugeBatch("api", "m1", []int64{1}, []float64{1}))
	require.NoError(t, err)
	_, err = s.WriteLogs(ctx, logBatch("api", [3]any{2, 9, "x"}))
	require.NoError(t, err)

	require.NoError(t, s.Admin().MaintainNow(ctx))

	ts := mustTenant(t, s.Inspect(), "default")
	for _, sig := range []signal.Signal{signal.Metric, signal.Log} {
		st, ok := findSignal(ts, sig)
		require.True(t, ok)
		assert.Equalf(t, 1, st.Parts, "%s flushed by MaintainNow", sig)
		assert.Equalf(t, int64(0), st.HeadItems, "%s head drained", sig)
	}
}

// TestMaintainFairSchedulingFlushesAllTenants confirms the pressure-ordered maintenance work-list
// still flushes every tenant in a cycle (a skewed big/small tenant mix), so fair ordering never
// starves a tenant — order is a within-cycle optimization, not a cap on coverage.
func TestMaintainFairSchedulingFlushesAllTenants(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s, err := InMemory(WithTenant(func(r signal.Resource, _ signal.Scope) signal.TenantID {
		v, _ := r.Attributes.Get([]byte("service.name"))

		return signal.TenantID(v.Str())
	}))
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close(ctx) })

	// "big" gets many samples (high head pressure ⇒ scheduled first); "small" gets one.
	bigTs := make([]int64, 50)
	bigVals := make([]float64, 50)
	for i := range bigTs {
		bigTs[i], bigVals[i] = int64(i+1), float64(i)
	}

	_, err = s.WriteMetrics(ctx, gaugeBatch("big", "m", bigTs, bigVals))
	require.NoError(t, err)
	_, err = s.WriteMetrics(ctx, gaugeBatch("small", "m", []int64{1}, []float64{1}))
	require.NoError(t, err)

	require.NoError(t, s.Admin().MaintainNow(ctx))

	for _, tenant := range []signal.TenantID{"big", "small"} {
		st, ok := findSignal(mustTenant(t, s.Inspect(), tenant), signal.Metric)
		require.Truef(t, ok, "%s present", tenant)
		assert.Equalf(t, 1, st.Parts, "%s flushed", tenant)
		assert.Equalf(t, int64(0), st.HeadItems, "%s head drained", tenant)
	}
}

func TestAdminRebalanceSingleNodeNoOp(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s, err := InMemory()
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close(ctx) })

	require.NoError(t, s.Admin().Rebalance(ctx), "rebalance is a no-op without a cluster")
}

func TestAdminRejectsAfterClose(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s, err := InMemory()
	require.NoError(t, err)
	require.NoError(t, s.Close(ctx))

	require.ErrorIs(t, s.Admin().Flush(ctx, "default", signal.Metric), ErrClosed)
	require.ErrorIs(t, s.Admin().Compact(ctx, "default", signal.Metric), ErrClosed)
	require.ErrorIs(t, s.Admin().MaintainNow(ctx), ErrClosed)
}

package storage

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/signal"
	"github.com/oteldb/storage/tenant"
)

func TestTokenBucket(t *testing.T) {
	t.Parallel()

	const t0 = int64(1_000_000_000)

	var b tokenBucket
	b.reconfigure(100, 100, t0) // 100 bytes/sec, burst 100

	assert.True(t, b.allow(100, t0, false), "full burst available")
	assert.False(t, b.allow(1, t0, false), "drained")

	// One second later, the bucket has refilled to burst.
	assert.True(t, b.allow(100, t0+1_000_000_000, false), "refilled after 1s")

	// Half a second ⇒ 50 tokens.
	assert.False(t, b.allow(100, t0+1_500_000_000, false), "only 50 refilled")
	assert.True(t, b.allow(50, t0+1_500_000_000, false))

	// Unlimited always admits regardless of tokens.
	assert.True(t, b.allow(1e9, t0, true))
}

func TestWriteMetricsRateLimit(t *testing.T) {
	t.Parallel()

	// 16 bytes/sec ⇒ one SampleBytes-sized point per second of budget.
	s, err := InMemory(WithTenancy(tenant.ResolverFunc(func(signal.TenantID) tenant.Policy {
		return tenant.Policy{Limits: tenant.Limits{IngestBytesPerSecond: 16}}
	})))
	require.NoError(t, err)

	s.now = func() int64 { return 1000 } // freeze the clock: no refill between writes

	ctx := context.Background()
	a, err := s.WriteMetrics(ctx, gaugeBatch("api", "m", []int64{1}, []float64{1}))
	require.NoError(t, err)
	assert.Equal(t, int64(1), a.Accepted)
	assert.Zero(t, a.Rejected)

	// Budget exhausted (no refill at the frozen clock): the next point is shed.
	b, err := s.WriteMetrics(ctx, gaugeBatch("api", "m", []int64{2}, []float64{2}))
	require.NoError(t, err)
	assert.Zero(t, b.Accepted)
	assert.Equal(t, int64(1), b.Rejected)
	assert.Equal(t, "rate_limit", b.RejectedReason)

	st := s.AdmissionStats("default")
	assert.Equal(t, int64(1), st.Accepted)
	assert.Equal(t, int64(1), st.RejectedRate)
}

func TestWriteMetricsMaxSeries(t *testing.T) {
	t.Parallel()

	s, err := InMemory(WithTenancy(tenant.ResolverFunc(func(signal.TenantID) tenant.Policy {
		return tenant.Policy{Limits: tenant.Limits{MaxSeries: 1}}
	})))
	require.NoError(t, err)

	ctx := context.Background()
	a, err := s.WriteMetrics(ctx, gaugeBatch("api", "m1", []int64{1}, []float64{1}))
	require.NoError(t, err)
	assert.Equal(t, int64(1), a.Accepted)

	// A second, distinct series exceeds the cardinality cap and is shed.
	b, err := s.WriteMetrics(ctx, gaugeBatch("api", "m2", []int64{1}, []float64{1}))
	require.NoError(t, err)
	assert.Zero(t, b.Accepted)
	assert.Equal(t, int64(1), b.Rejected)
	assert.Equal(t, "max_series", b.RejectedReason)

	assert.Equal(t, int64(1), s.AdmissionStats("default").RejectedCardinality)
}

func TestWriteMetricsMaxInFlightBytes(t *testing.T) {
	t.Parallel()

	// Cap at two samples' worth of in-flight bytes.
	s, err := InMemory(WithTenancy(tenant.ResolverFunc(func(signal.TenantID) tenant.Policy {
		return tenant.Policy{Limits: tenant.Limits{MaxInFlightBytes: 32}}
	})))
	require.NoError(t, err)

	ctx := context.Background()
	a, err := s.WriteMetrics(ctx, gaugeBatch("api", "m", []int64{1, 2, 3}, []float64{1, 2, 3}))
	require.NoError(t, err)
	assert.Equal(t, int64(2), a.Accepted, "two samples fit the in-flight cap")
	assert.Equal(t, int64(1), a.Rejected)
	assert.Equal(t, "max_in_flight_bytes", a.RejectedReason)

	assert.Equal(t, int64(1), s.AdmissionStats("default").RejectedInFlight)
}

func TestAdmissionStatsUnknownTenant(t *testing.T) {
	t.Parallel()

	s, err := InMemory()
	require.NoError(t, err)

	assert.Equal(t, AdmissionStats{}, s.AdmissionStats("never-written"))
}

func TestWriteMetricsNoLimitsUnaffected(t *testing.T) {
	t.Parallel()

	s, err := InMemory() // default resolver ⇒ no limits
	require.NoError(t, err)

	ctx := context.Background()
	a, err := s.WriteMetrics(ctx, gaugeBatch("api", "m", []int64{1, 2, 3}, []float64{1, 2, 3}))
	require.NoError(t, err)
	assert.Equal(t, int64(3), a.Accepted)
	assert.Zero(t, a.Rejected)
	assert.Empty(t, a.RejectedReason)
	assert.Equal(t, int64(3), s.AdmissionStats("default").Accepted)
}

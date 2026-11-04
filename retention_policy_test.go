package storage

import (
	"context"
	"math/rand/v2"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/internal/reproduce"
	"github.com/oteldb/storage/signal"
	"github.com/oteldb/storage/tenant"
)

func TestRetentionPerSignalAgeIsolation(t *testing.T) {
	t.Parallel()
	for target := signal.Metric; target <= signal.Exemplar; target++ {
		t.Run(target.String(), func(t *testing.T) {
			t.Parallel()
			checkRetentionAgeIsolation(t, target, WithMergeMemory(512<<20), WithMaintenanceConcurrency(1), WithFlushInterval(-1))
		})
	}
}

func checkRetentionAgeIsolation(t *testing.T, target signal.Signal, opts ...Option) {
	t.Helper()
	s, err := InMemory(append(opts, WithTenancy(tenant.ResolverFunc(func(signal.TenantID) tenant.Policy {
		return tenant.Policy{Retention: tenant.Retention{MaxAge: 24 * time.Hour, PerSignal: map[signal.Signal]tenant.SignalRetention{target: {MaxAge: time.Hour}}}}
	})))...)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, s.Close(context.Background())) })
	now := time.Now()
	old, fresh := now.Add(-6*time.Hour).UnixNano(), now.Add(-time.Minute).UnixNano()
	for _, ts := range []int64{old, fresh} {
		_, err = s.WriteMetrics(t.Context(), exemplarBatch("api", "m", ts, map[string][]exemplarSpec{"/": {{ts: ts, value: 1, traceID: "trace"}}}))
		require.NoError(t, err)
		_, err = s.WriteLogs(t.Context(), logBatch("api", [3]any{ts, 9, "body"}))
		require.NoError(t, err)
		_, err = s.WriteTraces(t.Context(), traceBatch("api", spanSpec{traceID: "trace", spanID: "span", name: "op", start: ts, end: ts + 1}))
		require.NoError(t, err)
		_, err = s.WriteProfiles(t.Context(), profileBatch("api", ts, sampleSpec{"cpu", "ns", 1}))
		require.NoError(t, err)
	}
	metric, ok := s.lookupEngine("default")
	require.True(t, ok)
	require.NoError(t, metric.Flush(t.Context()))
	require.Equal(t, 1, metric.PartCount(), "the fixture retains a straddling old/fresh metric part")

	s.maintain(t.Context())
	s.maintain(t.Context())
	stats := s.Inspect()
	require.Len(t, stats.Tenants, 1)
	require.Len(t, stats.Tenants[0].Signals, 5)
	for i := range stats.Tenants[0].Signals {
		got := &stats.Tenants[0].Signals[i]
		require.Positive(t, got.Parts)
		if got.Signal == target {
			require.GreaterOrEqual(t, got.MinTimeUnixNano, now.Add(-time.Hour).UnixNano())
		} else {
			require.Equal(t, old, got.MinTimeUnixNano, got.Signal.String())
		}
	}
}

func TestMetricRetentionFitsFallbackBudget(t *testing.T) {
	reproduce.Unfixed(t, 659, "a buffered single-part metric retention rewrite needs more than the fallback per-engine share")
	t.Parallel()
	checkRetentionAgeIsolation(t, signal.Metric, WithMergeMemory(512<<20), WithMaintenanceConcurrency(5), WithFlushInterval(-1))
}

func TestRetentionPolicyIsolationProperty(t *testing.T) {
	t.Parallel()
	rng := rand.New(rand.NewPCG(667, 1))
	const now = int64(1 << 50)
	for range 1024 {
		def := time.Duration(rng.IntN(1000) + 1)
		r := tenant.Retention{MaxAge: def, PerSignal: make(map[signal.Signal]tenant.SignalRetention)}
		for sig := signal.Metric; sig <= signal.Exemplar; sig++ {
			r.PerSignal[sig] = tenant.SignalRetention{MaxAge: time.Duration(rng.IntN(1000) + 1), MaxBytes: int64(rng.IntN(1000) + 1)}
		}
		before := budgetsOf(r)
		for sig := signal.Metric; sig <= signal.Exemplar; sig++ {
			age := r.PerSignal[sig].MaxAge
			require.Equal(t, now-int64(age), retentionCutoff(r, sig, now))
			r.MaxAge++
			require.Equal(t, now-int64(age), retentionCutoff(r, sig, now))
		}
		target := signal.Signal(1 + rng.IntN(5))
		bounds := r.PerSignal[target]
		bounds.MaxAge = max(bounds.MaxAge/2, 1)
		bounds.MaxBytes++
		r.PerSignal[target] = bounds
		after := budgetsOf(r)
		for sig := signal.Metric; sig <= signal.Exemplar; sig++ {
			if sig != target {
				require.Equal(t, before.perSignal.at(sig), after.perSignal.at(sig))
			}
		}
	}
}

func TestRetentionPerTenantRuntimeAge(t *testing.T) {
	t.Parallel()
	var policy atomic.Pointer[tenant.Retention]
	policy.Store(&tenant.Retention{MaxAge: 24 * time.Hour})
	s, err := InMemory(WithTenant(tenantByService), WithTenancy(tenant.ResolverFunc(func(id signal.TenantID) tenant.Policy {
		r := tenant.Retention{MaxAge: 24 * time.Hour}
		if id == "left" {
			r = *policy.Load()
		}
		return tenant.Policy{Retention: r}
	})))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, s.Close(context.Background())) })
	now := time.Now()
	old, fresh := now.Add(-6*time.Hour).UnixNano(), now.Add(-time.Minute).UnixNano()
	for _, id := range []string{"left", "right"} {
		_, err = s.WriteLogs(t.Context(), logBatch(id, [3]any{old, 9, "old"}, [3]any{fresh, 9, "fresh"}))
		require.NoError(t, err)
	}
	s.maintain(t.Context())
	check := func(left int64) {
		t.Helper()
		for _, ts := range s.Inspect().Tenants {
			require.Len(t, ts.Signals, 1)
			want := old
			if ts.Tenant == "left" {
				want = left
			}
			require.Equal(t, want, ts.Signals[0].MinTimeUnixNano)
		}
	}
	check(old)
	policy.Store(&tenant.Retention{PerSignal: map[signal.Signal]tenant.SignalRetention{signal.Log: {MaxAge: time.Hour}}})
	s.maintain(t.Context())
	s.maintain(t.Context())
	check(fresh)
	policy.Store(&tenant.Retention{MaxAge: 24 * time.Hour})
	s.maintain(t.Context())
	check(fresh)
}

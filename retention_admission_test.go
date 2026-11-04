package storage

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/oteldb/storage/signal"
	"github.com/oteldb/storage/tenant"
)

func writeRetentionPair(t *testing.T, s *Storage, sig signal.Signal, old, fresh int64) Accepted {
	t.Helper()
	var acc Accepted
	var err error
	switch sig {
	case signal.Metric:
		acc, err = s.WriteMetrics(t.Context(), gaugeBatch("api", "m", []int64{old, fresh}, []float64{1, 2}))
	case signal.Log:
		acc, err = s.WriteLogs(t.Context(), logBatch("api", [3]any{int(old), 9, "old"}, [3]any{int(fresh), 9, "fresh"}))
	case signal.Trace:
		acc, err = s.WriteTraces(t.Context(), traceBatch("api", spanSpec{traceID: "a", spanID: "a", name: "old", start: old, end: old + 1}, spanSpec{traceID: "b", spanID: "b", name: "fresh", start: fresh, end: fresh + 1}))
	case signal.Profile:
		pd := profileBatch("api", old, sampleSpec{"cpu", "ns", 1})
		pr := pd.Resources[0].Scopes[0].Profiles[0]
		pr.TimeNanos = fresh
		pd.Resources[0].Scopes[0].Profiles = append(pd.Resources[0].Scopes[0].Profiles, pr)
		acc, err = s.WriteProfiles(t.Context(), pd)
	case signal.Exemplar:
		acc, err = s.WriteMetrics(t.Context(), exemplarBatch("api", "exemplar-parent", fresh, map[string][]exemplarSpec{"/": {{ts: old, value: 1, traceID: "old"}, {ts: fresh, value: 2, traceID: "fresh"}}}))
	default:
		t.Fatal("unknown signal")
	}
	require.NoError(t, err)
	return acc
}

func TestRetentionAdmissionOptInAndSignalAge(t *testing.T) {
	t.Parallel()
	for sig := signal.Metric; sig <= signal.Exemplar; sig++ {
		t.Run(sig.String(), func(t *testing.T) {
			t.Parallel()
			for _, enabled := range []bool{false, true} {
				name := "off"
				if enabled {
					name = "on"
				}
				t.Run(name, func(t *testing.T) {
					t.Parallel()
					reader := sdkmetric.NewManualReader()
					mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
					t.Cleanup(func() { require.NoError(t, mp.Shutdown(context.Background())) })
					opts := []Option{WithMeterProvider(mp), WithTenancy(tenant.ResolverFunc(func(signal.TenantID) tenant.Policy {
						return tenant.Policy{Retention: tenant.Retention{PerSignal: map[signal.Signal]tenant.SignalRetention{sig: {MaxAge: time.Hour}}}}
					}))}
					if enabled {
						opts = append(opts, WithRejectOutOfRetention())
					}
					s, err := InMemory(opts...)
					require.NoError(t, err)
					t.Cleanup(func() { require.NoError(t, s.Close(context.Background())) })
					const now = int64(10 * time.Hour)
					s.now = func() int64 { return now }
					acc := writeRetentionPair(t, s, sig, now-int64(2*time.Hour), now-int64(time.Minute))
					expired := 0
					if enabled {
						expired = 1
					}
					if sig == signal.Exemplar {
						require.EqualValues(t, 1, acc.Accepted)
						require.Zero(t, acc.Rejected)
					} else {
						require.EqualValues(t, 2-expired, acc.Accepted)
						require.EqualValues(t, expired, acc.Rejected)
						if enabled {
							require.Equal(t, "out_of_retention", acc.RejectedReason)
						}
					}
					stats := s.AdmissionStats("default")
					require.EqualValues(t, expired, stats.RejectedRetention)
					if sig == signal.Metric {
						eng, ok := s.lookupEngine("default")
						require.True(t, ok)
						require.Equal(t, 2-expired, eng.HeadSampleCount())
					} else {
						eng, ok := s.lookupRecordEngine(sig, "default")
						require.True(t, ok)
						require.Equal(t, 2-expired, eng.HeadRecordCount())
					}
					var rm metricdata.ResourceMetrics
					require.NoError(t, reader.Collect(t.Context(), &rm))
					require.EqualValues(t, expired, sumCounter(t, rm, "storage.ingest.rejected", map[string]string{"signal": sig.String(), "reason": "out_of_retention"}))
				})
			}
		})
	}
}

func TestRetentionAdmissionZeroAgeAndBoundary(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name     string
		age      time.Duration
		ts       []int64
		accepted int
	}{
		{"zero age", 0, []int64{-1, 0, 1}, 3},
		{"zero cutoff", time.Hour, []int64{-1, 0, 1}, 2},
		{"negative cutoff", 2 * time.Hour, []int64{-int64(time.Hour) - 1, -int64(time.Hour), 0}, 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			s, err := InMemory(WithRejectOutOfRetention(), WithTenancy(tenant.ResolverFunc(func(signal.TenantID) tenant.Policy { return tenant.Policy{Retention: tenant.Retention{MaxAge: tc.age}} })))
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, s.Close(context.Background())) })
			s.now = func() int64 { return int64(time.Hour) }
			vals := make([]float64, len(tc.ts))
			acc, err := s.WriteMetrics(t.Context(), gaugeBatch("api", "m", tc.ts, vals))
			require.NoError(t, err)
			require.EqualValues(t, tc.accepted, acc.Accepted)
			require.EqualValues(t, len(tc.ts)-tc.accepted, acc.Rejected)
		})
	}
}

func TestRetentionAdmissionDoesNotSampleExpiredAsAccepted(t *testing.T) {
	t.Parallel()
	s, err := InMemory(WithRejectOutOfRetention(), WithTenancy(tenant.ResolverFunc(func(signal.TenantID) tenant.Policy {
		return tenant.Policy{Retention: tenant.Retention{MaxAge: time.Hour}, Sampling: tenant.Sampling{MaxRowsPerSecond: 1}}
	})))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, s.Close(context.Background())) })
	now := int64(10 * time.Hour)
	s.now = func() int64 { return now }
	_, err = s.WriteMetrics(t.Context(), gaugeBatch("api", "m", []int64{now, now + 1, now + 2, now + 3}, []float64{1, 1, 1, 1}))
	require.NoError(t, err)
	now += int64(time.Second)
	acc, err := s.WriteMetrics(t.Context(), gaugeBatch("api", "m", []int64{0, 0, 0, now, now + 1, now + 2, now + 3}, []float64{1, 1, 1, 1, 1, 1, 1}))
	require.NoError(t, err)
	require.EqualValues(t, 4, acc.Accepted)
	require.EqualValues(t, 3, acc.Rejected)
	require.EqualValues(t, 3, s.AdmissionStats("default").RejectedRetention)
}

func TestRetentionAdmissionMetricAndExemplarAreIndependent(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name                   string
		metricAge, exemplarAge time.Duration
		metricExpired          bool
	}{
		{"metric expires first", time.Hour, 24 * time.Hour, true},
		{"exemplar expires first", 24 * time.Hour, time.Hour, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			s, err := InMemory(WithRejectOutOfRetention(), WithTenancy(tenant.ResolverFunc(func(signal.TenantID) tenant.Policy {
				return tenant.Policy{Retention: tenant.Retention{PerSignal: map[signal.Signal]tenant.SignalRetention{signal.Metric: {MaxAge: tc.metricAge}, signal.Exemplar: {MaxAge: tc.exemplarAge}}}}
			})))
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, s.Close(context.Background())) })
			const now = int64(10 * time.Hour)
			s.now = func() int64 { return now }
			old := now - int64(2*time.Hour)
			acc, err := s.WriteMetrics(t.Context(), exemplarBatch("api", "m", old, map[string][]exemplarSpec{"/": {{ts: old, value: 1, traceID: "trace"}}}))
			require.NoError(t, err)
			if tc.metricExpired {
				require.Zero(t, acc.Accepted)
				require.EqualValues(t, 1, acc.Rejected)
			} else {
				require.EqualValues(t, 1, acc.Accepted)
				require.Zero(t, acc.Rejected)
			}
			exemplar, exists := s.lookupRecordEngine(signal.Exemplar, "default")
			require.True(t, exists)
			if tc.metricExpired {
				require.Equal(t, 1, exemplar.HeadRecordCount())
			} else {
				require.Zero(t, exemplar.HeadRecordCount())
			}
			require.EqualValues(t, 1, s.AdmissionStats("default").RejectedRetention)
		})
	}
}

func TestRetentionAdmissionUsesCurrentTenantPolicy(t *testing.T) {
	t.Parallel()
	var current atomic.Pointer[tenant.Retention]
	current.Store(&tenant.Retention{})
	s, err := InMemory(WithRejectOutOfRetention(), WithTenant(tenantByService), WithTenancy(tenant.ResolverFunc(func(id signal.TenantID) tenant.Policy {
		r := tenant.Retention{}
		if id == "api" {
			r = *current.Load()
		}
		return tenant.Policy{Retention: r}
	})))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, s.Close(context.Background())) })
	const now = int64(10 * time.Hour)
	s.now = func() int64 { return now }
	old := now - int64(2*time.Hour)
	acc, err := s.WriteLogs(t.Context(), logBatch("api", [3]any{int(old), 9, "old"}))
	require.NoError(t, err)
	require.EqualValues(t, 1, acc.Accepted)
	current.Store(&tenant.Retention{PerSignal: map[signal.Signal]tenant.SignalRetention{signal.Log: {MaxAge: time.Hour}}})
	acc, err = s.WriteLogs(t.Context(), logBatch("api", [3]any{int(old), 9, "old"}))
	require.NoError(t, err)
	require.EqualValues(t, 1, acc.Rejected)
	acc, err = s.WriteLogs(t.Context(), logBatch("other", [3]any{int(old), 9, "old"}))
	require.NoError(t, err)
	require.EqualValues(t, 1, acc.Accepted)
	require.Zero(t, s.AdmissionStats("other").RejectedRetention)
	current.Store(&tenant.Retention{MaxAge: 24 * time.Hour})
	acc, err = s.WriteLogs(t.Context(), logBatch("api", [3]any{int(old), 9, "old"}))
	require.NoError(t, err)
	require.EqualValues(t, 1, acc.Accepted)
}

func TestRetentionAdmissionPreservesRatePrecedence(t *testing.T) {
	t.Parallel()
	for _, sig := range []signal.Signal{signal.Metric, signal.Log, signal.Trace, signal.Profile} {
		t.Run(sig.String(), func(t *testing.T) {
			t.Parallel()
			s, err := InMemory(WithRejectOutOfRetention(), WithTenancy(tenant.ResolverFunc(func(signal.TenantID) tenant.Policy {
				return tenant.Policy{Limits: tenant.Limits{IngestBytesPerSecond: 1}, Retention: tenant.Retention{MaxAge: time.Hour}}
			})))
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, s.Close(context.Background())) })
			const now = int64(10 * time.Hour)
			s.now = func() int64 { return now }
			acc := writeRetentionPair(t, s, sig, now-int64(2*time.Hour), now-int64(time.Minute))
			require.Zero(t, acc.Accepted)
			require.EqualValues(t, 2, acc.Rejected)
			require.Equal(t, "rate_limit", acc.RejectedReason)
			require.EqualValues(t, 2, s.AdmissionStats("default").RejectedRate)
			require.Zero(t, s.AdmissionStats("default").RejectedRetention)
		})
	}
}

func TestRetentionAdmissionChargesMetricRateOnce(t *testing.T) {
	t.Parallel()
	s, err := InMemory(WithRejectOutOfRetention(), WithTenancy(tenant.ResolverFunc(func(signal.TenantID) tenant.Policy {
		return tenant.Policy{Limits: tenant.Limits{IngestBytesPerSecond: 32}, Retention: tenant.Retention{MaxAge: time.Hour}}
	})))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, s.Close(context.Background())) })
	const now = int64(10 * time.Hour)
	s.now = func() int64 { return now }
	acc := writeRetentionPair(t, s, signal.Metric, now-int64(2*time.Hour), now-int64(time.Minute))
	require.EqualValues(t, 1, acc.Accepted)
	require.EqualValues(t, 1, acc.Rejected)
	require.Equal(t, "out_of_retention", acc.RejectedReason)
	require.Zero(t, s.AdmissionStats("default").RejectedRate)
}

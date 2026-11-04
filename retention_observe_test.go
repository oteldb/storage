package storage

import (
	"context"
	"math"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-faster/errors"
	"github.com/stretchr/testify/require"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/backend/bucketindex"
	"github.com/oteldb/storage/cluster/etcd/etcdtest"
	"github.com/oteldb/storage/engine"
	"github.com/oteldb/storage/internal/obs/obstest"
	"github.com/oteldb/storage/recordengine"
	"github.com/oteldb/storage/signal"
	"github.com/oteldb/storage/tenant"
)

func TestRetentionDecisionBoundOrder(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name   string
		policy tenant.Retention
		size   retentionDecision
		want   retentionDecision
	}{
		{"none", tenant.Retention{}, retentionDecision{}, retentionDecision{}},
		{"age", tenant.Retention{MaxAge: 10}, retentionDecision{}, retentionDecision{90, RetentionBoundAge}},
		{"signal bytes", tenant.Retention{MaxAge: 20}, retentionDecision{90, RetentionBoundSignalBytes}, retentionDecision{90, RetentionBoundSignalBytes}},
		{"pooled bytes", tenant.Retention{MaxAge: 20}, retentionDecision{90, RetentionBoundPooledBytes}, retentionDecision{90, RetentionBoundPooledBytes}},
		{"age wins tie", tenant.Retention{MaxAge: 10}, retentionDecision{90, RetentionBoundPooledBytes}, retentionDecision{90, RetentionBoundAge}},
		{"per-signal age", tenant.Retention{MaxAge: 10, PerSignal: map[signal.Signal]tenant.SignalRetention{signal.Log: {MaxAge: 20}}}, retentionDecision{}, retentionDecision{80, RetentionBoundAge}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, tc.want, retentionDecisionFor(tc.policy, signal.Log, 100, tc.size))
		})
	}
	for _, tc := range []struct {
		name           string
		cutoff, oldest int64
		parts          int
		want           float64
	}{
		{"no policy", 0, 1, 1, 0}, {"no parts", 10, 0, 0, 0}, {"converged", 10, 10, 1, 0}, {"lag", 10e9, 5e9, 1, 5},
		{"epoch zero", 10e9, 0, 1, 10}, {"negative data time", 10e9, -5e9, 1, 15}, {"saturating subtraction", math.MaxInt64, math.MinInt64, 1, float64(math.MaxInt64)/1e9 - float64(math.MinInt64)/1e9},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			require.InDelta(t, tc.want, retentionLag(tc.cutoff, tc.oldest, tc.parts), 1e-6)
		})
	}
}

func TestRetentionInspectWinningSizeBounds(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name   string
		policy tenant.Retention
		bound  RetentionBound
	}{
		{"signal", tenant.Retention{PerSignal: map[signal.Signal]tenant.SignalRetention{signal.Log: {MaxBytes: 1}}}, RetentionBoundSignalBytes},
		{"pooled", tenant.Retention{MaxBytes: 1}, RetentionBoundPooledBytes},
		{"specific wins tie", tenant.Retention{MaxBytes: 1, PerSignal: map[signal.Signal]tenant.SignalRetention{signal.Log: {MaxBytes: 1}}}, RetentionBoundSignalBytes},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			s, err := InMemory(WithTenancy(tenant.ResolverFunc(func(signal.TenantID) tenant.Policy { return tenant.Policy{Retention: tc.policy} })))
			require.NoError(t, err)
			defer func() { require.NoError(t, s.Close(context.Background())) }()
			for _, ts := range []int{100, 200} {
				_, err = s.WriteLogs(t.Context(), logBatch("api", [3]any{ts, 9, "body"}))
				require.NoError(t, err)
				require.NoError(t, s.Admin().Flush(t.Context(), "default", signal.Log))
			}
			d, ok := s.measureSizeDecision(t.Context(), "default")
			require.True(t, ok)
			require.Equal(t, tc.bound, d.at(signal.Log).bound)
			require.NoError(t, s.Admin().CompactNow(t.Context(), "default", signal.Log))
			stats := s.Inspect()
			tenantStats, ok := findTenant(stats, "default")
			require.True(t, ok)
			got, ok := findSignal(tenantStats, signal.Log)
			require.True(t, ok)
			require.Equal(t, d.at(signal.Log).cutoff, got.RetainFromUnixNano)
			require.Equal(t, tc.bound, got.RetentionBoundBy)
		})
	}
}

func TestRetentionInspectSignalAgesAndDynamicPolicy(t *testing.T) {
	t.Parallel()
	var ret atomic.Pointer[tenant.Retention]
	ret.Store(&tenant.Retention{MaxAge: 24 * time.Hour})
	s, err := InMemory(WithTenancy(tenant.ResolverFunc(func(signal.TenantID) tenant.Policy { return tenant.Policy{Retention: *ret.Load()} })))
	require.NoError(t, err)
	defer func() { require.NoError(t, s.Close(context.Background())) }()
	now := time.Now()
	old, fresh := now.Add(-6*time.Hour).UnixNano(), now.Add(-time.Minute).UnixNano()
	for _, ts := range []int64{old, fresh} {
		_, err = s.WriteMetrics(t.Context(), exemplarBatch("api", "m", ts, map[string][]exemplarSpec{"/": {{ts: ts, value: 1, traceID: "trace"}}}))
		require.NoError(t, err)
		_, err = s.WriteLogs(t.Context(), logBatch("api", [3]any{int(ts), 9, "body"}))
		require.NoError(t, err)
		_, err = s.WriteTraces(t.Context(), traceBatch("api", spanSpec{traceID: "trace", spanID: "span", name: "op", start: ts, end: ts + 1}))
		require.NoError(t, err)
		_, err = s.WriteProfiles(t.Context(), profileBatch("api", ts, sampleSpec{"cpu", "ns", 1}))
		require.NoError(t, err)
	}
	require.NoError(t, s.Admin().MaintainNow(t.Context()))
	before := s.Inspect().Tenants[0].Signals
	require.Len(t, before, 5)
	for _, got := range before {
		require.Equal(t, RetentionBoundAge, got.RetentionBoundBy)
		require.Less(t, got.RetainFromUnixNano, old)
	}
	ret.Store(&tenant.Retention{MaxAge: 24 * time.Hour, PerSignal: map[signal.Signal]tenant.SignalRetention{signal.Log: {MaxAge: time.Hour}}})
	require.NoError(t, s.Admin().CompactNow(t.Context(), "default", signal.Log))
	after := s.Inspect().Tenants[0].Signals
	for _, got := range after {
		if got.Signal == signal.Log {
			require.Greater(t, got.RetainFromUnixNano, old)
			require.Equal(t, RetentionBoundAge, got.RetentionBoundBy)
		} else {
			for _, prior := range before {
				if prior.Signal == got.Signal {
					require.Equal(t, prior.RetainFromUnixNano, got.RetainFromUnixNano)
				}
			}
		}
	}
}

func TestRetentionLagGaugeClearsWithoutParts(t *testing.T) {
	t.Parallel()
	reader := sdkmetric.NewManualReader()
	s, err := Open(t.Context(), Options{Backend: backend.Memory(), MergeMemoryBytes: 1 << 20}, WithFlushInterval(-1), WithMeterProvider(sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))))
	require.NoError(t, err)
	defer func() { require.NoError(t, s.Close(context.Background())) }()
	_, err = s.WriteLogs(t.Context(), logBatch("api", [3]any{int(1e9), 9, "old"}, [3]any{int(3e9), 9, "new"}))
	require.NoError(t, err)
	require.NoError(t, s.Admin().Flush(t.Context(), "default", signal.Log))
	e, ok := s.lookupRecordEngine(signal.Log, "default")
	require.True(t, ok)
	release, err := s.mergePool.Acquire(t.Context(), s.opts.MergeMemoryBytes)
	require.NoError(t, err)
	defer func() {
		if release != nil {
			release()
		}
	}()
	eCfg := recordengine.MergeOptions{RetainFrom: 2e9, RetentionBoundBy: RetentionBoundAge, Background: true}
	require.NoError(t, e.MergeWith(t.Context(), eCfg))
	require.True(t, e.MergeDeferred())
	s.recordPartShape(t.Context())
	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(t.Context(), &rm))
	require.InDelta(t, 1.0, retentionGauge(t, rm, "log"), 1e-12)
	release()
	release = nil
	eCfg.Background = false
	require.NoError(t, e.MergeWith(t.Context(), eCfg))
	s.recordPartShape(t.Context())
	require.NoError(t, reader.Collect(t.Context(), &rm))
	require.Zero(t, retentionGauge(t, rm, "log"))
	require.NoError(t, e.Merge(t.Context(), int64(10e9)))
	s.recordPartShape(t.Context())
	require.NoError(t, reader.Collect(t.Context(), &rm))
	require.Zero(t, retentionGauge(t, rm, "log"))
}

func retentionGauge(t *testing.T, rm metricdata.ResourceMetrics, sig string) float64 {
	t.Helper()
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != "storage.retention.lag_seconds" {
				continue
			}
			g, ok := m.Data.(metricdata.Gauge[float64])
			require.True(t, ok)
			for _, dp := range g.DataPoints {
				v, _ := dp.Attributes.Value("signal")
				if v.AsString() == sig {
					return dp.Value
				}
			}
		}
	}
	t.Fatal("missing retention lag gauge")
	return 0
}

func writeRetentionObserved(t *testing.T, s *Storage, sig signal.Signal, ts int64) {
	t.Helper()
	var err error
	switch sig {
	case signal.Metric:
		_, err = s.WriteMetrics(t.Context(), gaugeBatch("api", "retention", []int64{ts}, []float64{1}))
	case signal.Log:
		_, err = s.WriteLogs(t.Context(), logBatch("api", [3]any{int(ts), 9, "body"}))
	case signal.Trace:
		_, err = s.WriteTraces(t.Context(), traceBatch("api", spanSpec{traceID: "trace", spanID: "span", name: "op", start: ts, end: ts + 1}))
	case signal.Profile:
		_, err = s.WriteProfiles(t.Context(), profileBatch("api", ts, sampleSpec{"cpu", "ns", 1}))
	case signal.Exemplar:
		_, err = s.WriteMetrics(t.Context(), exemplarBatch("api", "retention", ts, map[string][]exemplarSpec{"/": {{ts: ts, value: 1, traceID: "trace"}}}))
	default:
		t.Fatal("unsupported signal")
	}
	require.NoError(t, err)
}

func mergeRetentionObserved(t *testing.T, s *Storage, sig signal.Signal, cutoff int64) error {
	t.Helper()
	if sig == signal.Metric {
		e, ok := s.lookupEngine("default")
		require.True(t, ok)
		return e.MergeWith(t.Context(), engine.MergeOptions{RetainFrom: cutoff, RetentionBoundBy: RetentionBoundAge, Force: true})
	}
	e, ok := s.lookupRecordEngine(sig, "default")
	require.True(t, ok)
	return e.MergeWith(t.Context(), recordengine.MergeOptions{RetainFrom: cutoff, RetentionBoundBy: RetentionBoundAge, Force: true})
}

func TestRetentionCountersAcrossSignals(t *testing.T) {
	t.Parallel()
	for sig := signal.Metric; sig <= signal.Exemplar; sig++ {
		t.Run(sig.String(), func(t *testing.T) {
			t.Parallel()
			for _, mode := range []string{"drop", "rewrite", "plain"} {
				t.Run(mode, func(t *testing.T) {
					t.Parallel()
					mp, metrics := obstest.Provider(t)
					s, err := InMemory(WithMeterProvider(mp))
					require.NoError(t, err)
					defer func() { require.NoError(t, s.Close(context.Background())) }()
					writeRetentionObserved(t, s, sig, 100)
					if mode != "rewrite" {
						require.NoError(t, s.Admin().Flush(t.Context(), "default", sig))
					}
					writeRetentionObserved(t, s, sig, 300)
					require.NoError(t, s.Admin().Flush(t.Context(), "default", sig))
					cutoff := int64(200)
					if mode == "plain" {
						cutoff = 0
					}
					require.NoError(t, mergeRetentionObserved(t, s, sig, cutoff))
					attrs := []string{"signal", sig.String()}
					drop, rewrite := int64(0), int64(0)
					if mode == "drop" {
						drop = 1
					}
					if mode == "rewrite" {
						rewrite = 1
					}
					require.Equal(t, drop, metrics.Counter("storage.retention.dropped_parts", attrs...))
					require.Equal(t, rewrite, metrics.Counter("storage.retention.rewrites", attrs...))
					if drop > 0 {
						require.Positive(t, metrics.Counter("storage.retention.dropped_bytes", attrs...))
					}
					require.NoError(t, mergeRetentionObserved(t, s, sig, cutoff))
					require.Equal(t, drop, metrics.Counter("storage.retention.dropped_parts", attrs...))
					require.Equal(t, rewrite, metrics.Counter("storage.retention.rewrites", attrs...))
				})
			}
		})
	}
}

type retentionReplyBackend struct {
	backend.Backend

	fail        bool
	land        bool
	unconfirmed bool
}

var errRetentionPublish = errors.New("retention publication failed")

func (b *retentionReplyBackend) CompareAndSwap(ctx context.Context, key string, expected backend.Version, data []byte) (backend.Version, bool, error) {
	if b.fail && strings.HasSuffix(key, bucketindex.Object) {
		if b.land {
			_, _, err := b.Backend.CompareAndSwap(ctx, key, expected, data)
			if err != nil {
				return backend.VersionAbsent, false, err
			}
		}
		return backend.VersionAbsent, false, errRetentionPublish
	}
	return b.Backend.CompareAndSwap(ctx, key, expected, data)
}

func (b *retentionReplyBackend) ReadVersioned(ctx context.Context, key string) ([]byte, backend.Version, error) {
	if b.fail && b.unconfirmed && strings.HasSuffix(key, bucketindex.Object) {
		return nil, backend.VersionAbsent, errRetentionPublish
	}
	return b.Backend.ReadVersioned(ctx, key)
}

func TestRetentionCountersFollowConfirmedPublication(t *testing.T) {
	t.Parallel()
	for _, sig := range []signal.Signal{signal.Metric, signal.Log} {
		t.Run(sig.String(), func(t *testing.T) {
			t.Parallel()
			for _, mode := range []string{"failed", "confirmed lost response", "unconfirmed"} {
				t.Run(mode, func(t *testing.T) {
					t.Parallel()
					b := &retentionReplyBackend{Backend: backend.Memory(), land: mode != "failed", unconfirmed: mode == "unconfirmed"}
					mp, metrics := obstest.Provider(t)
					s, err := Open(t.Context(), Options{Backend: b}, WithFlushInterval(-1), WithMeterProvider(mp))
					require.NoError(t, err)
					defer func() { b.fail = false; require.NoError(t, s.Close(context.Background())) }()
					for _, ts := range []int64{100, 300} {
						writeRetentionObserved(t, s, sig, ts)
						require.NoError(t, s.Admin().Flush(t.Context(), "default", sig))
					}
					b.fail = true
					err = mergeRetentionObserved(t, s, sig, 200)
					attrs := []string{"signal", sig.String()}
					if mode == "confirmed lost response" {
						require.NoError(t, err)
						require.EqualValues(t, 1, metrics.Counter("storage.retention.dropped_parts", attrs...))
					} else {
						require.ErrorIs(t, err, errRetentionPublish)
						require.Zero(t, metrics.Counter("storage.retention.dropped_parts", attrs...))
					}
					b.fail = false
					if mode == "unconfirmed" {
						return
					}
					require.NoError(t, mergeRetentionObserved(t, s, sig, 200))
					require.EqualValues(t, 1, metrics.Counter("storage.retention.dropped_parts", attrs...))
					require.Zero(t, metrics.Counter("storage.retention.rewrites", attrs...))
				})
			}
		})
	}
}

//nolint:paralleltest // owns embedded etcd and drives claims explicitly.
func TestRetentionInspectDisclaimsReplicasAndReleasedOwners(t *testing.T) {
	endpoint := etcdtest.Start(t)
	policy := WithTenancy(tenant.ResolverFunc(func(signal.TenantID) tenant.Policy {
		return tenant.Policy{Retention: tenant.Retention{MaxAge: time.Hour}}
	}))
	nodes := map[string]*Storage{
		"node-a": openClusterNodeWith(t, endpoint, "node-a", backend.Memory(), policy, WithFlushInterval(-1)),
		"node-b": openClusterNodeWith(t, endpoint, "node-b", backend.Memory(), policy, WithFlushInterval(-1)),
	}
	awaitMembership(t, nodes)
	primary, ok := nodes["node-a"].cluster.membership.Ring().Primary([]byte("default"))
	require.True(t, ok)
	owner := nodes[primary.ID]
	replica := nodes["node-a"]
	if replica == owner {
		replica = nodes["node-b"]
	}
	now := time.Now().UnixNano()
	_, err := owner.WriteMetrics(t.Context(), gaugeBatch("api", "retention", []int64{now}, []float64{1}))
	require.NoError(t, err)
	require.NoError(t, owner.Admin().MaintainNow(t.Context()))
	require.NoError(t, replica.Admin().MaintainNow(t.Context()))
	owned, ok := findTenant(owner.Inspect(), "default")
	require.True(t, ok)
	m, ok := findSignal(owned, signal.Metric)
	require.True(t, ok)
	require.Positive(t, m.RetainFromUnixNano)
	require.Equal(t, RetentionBoundAge, m.RetentionBoundBy)
	mirrored, ok := findTenant(replica.Inspect(), "default")
	require.True(t, ok)
	m, ok = findSignal(mirrored, signal.Metric)
	require.True(t, ok)
	require.Zero(t, m.RetainFromUnixNano)
	require.Equal(t, RetentionBoundNone, m.RetentionBoundBy)
	owner.cluster.ownership.SetFence(func() bool { return true })
	owned, _ = findTenant(owner.Inspect(), "default")
	m, _ = findSignal(owned, signal.Metric)
	require.Zero(t, m.RetainFromUnixNano)
	owner.cluster.ownership.SetFence(owner.cluster.membership.Fenced)
	require.NoError(t, owner.cluster.ownership.Release(t.Context(), "default"))
	owned, _ = findTenant(owner.Inspect(), "default")
	m, _ = findSignal(owned, signal.Metric)
	require.Zero(t, m.RetainFromUnixNano)
}

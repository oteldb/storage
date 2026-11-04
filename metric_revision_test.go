package storage

import (
	"context"
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/signal"
	"github.com/oteldb/storage/tenant"
)

//nolint:paralleltest,tparallel // Exercises revision continuity across reset.
func TestMetricReadRevisionMutations(t *testing.T) {
	t.Parallel()
	s, err := InMemory()
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, s.Close(context.Background())) })
	before, ok := s.MetricReadRevision("", 0)
	require.True(t, ok)
	for _, tt := range []struct {
		name   string
		mutate func() error
	}{
		{"write", func() error {
			_, err := s.WriteMetrics(t.Context(), gaugeBatch("a", "counter", []int64{100}, []float64{1}))
			return err
		}},
		{"late-write", func() error {
			_, err := s.WriteMetrics(t.Context(), gaugeBatch("a", "counter", []int64{90}, []float64{2}))
			return err
		}},
		{"flush", func() error { return s.Admin().Flush(t.Context(), "", signal.Metric) }},
		{"reset", func() error { return s.Reset(t.Context()) }},
	} {
		t.Run(tt.name, func(t *testing.T) {
			require.NoError(t, tt.mutate())
			after, ok := s.MetricReadRevision("default", 0)
			require.True(t, ok)
			require.NotEqual(t, before, after)
			again, ok := s.MetricReadRevision("", 0)
			require.True(t, ok)
			require.Equal(t, after, again)
			before = after
		})
	}
	require.NoError(t, s.Close(t.Context()))
	_, ok = s.MetricReadRevision("", 0)
	require.False(t, ok)
}

func TestMetricReadRevisionAge(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		name     string
		min, now int64
		age      time.Duration
		valid    bool
		until    int64
	}{
		{"unbounded", math.MinInt64, 1000, 0, true, 0},
		{"fresh", 901, 1000, 100, true, 1001},
		{"deadline", 900, 1000, 100, false, 1000},
		{"expired", 899, 1000, 100, false, 999},
		{"overflow", math.MaxInt64, 1000, 100, false, 0},
		{"zero-sentinel", -100, -1, 100, false, 0},
		{"negative-time", -150, -100, 100, true, -50},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			s, err := InMemory(WithTenancy(tenant.ResolverFunc(func(signal.TenantID) tenant.Policy { return tenant.Policy{Retention: tenant.Retention{MaxAge: tt.age}} })))
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, s.Close(context.Background())) })
			s.now = func() int64 { return tt.now }
			revision, ok := s.MetricReadRevision("", tt.min)
			require.Equal(t, tt.valid, ok)
			require.Equal(t, tt.until, revision.ValidUntil)
		})
	}
}

func TestMetricReadRevisionPolicyAndClock(t *testing.T) {
	t.Parallel()
	var age time.Duration = 100
	s, err := InMemory(WithTenancy(tenant.ResolverFunc(func(signal.TenantID) tenant.Policy { return tenant.Policy{Retention: tenant.Retention{MaxAge: age}} })))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, s.Close(context.Background())) })
	var now int64 = 1000
	s.now = func() int64 { return now }
	first, ok := s.MetricReadRevision("", 901)
	require.True(t, ok)
	age = 200
	second, ok := s.MetricReadRevision("", 901)
	require.True(t, ok)
	require.NotEqual(t, first, second, "a policy age change changes the key even without a data mutation")
	now = second.ValidUntil
	_, ok = s.MetricReadRevision("", 901)
	require.False(t, ok, "the deadline is checked on every lookup")
}

func TestMetricReadRevisionClusterUnsupported(t *testing.T) {
	t.Parallel()
	s := &Storage{cluster: &clusterNode{}}
	_, ok := s.MetricReadRevision("", 0)
	require.False(t, ok)
}

func TestMetricReadRevisionLocalShardPolicy(t *testing.T) {
	t.Parallel()
	s, err := InMemory(WithTenant(func(signal.Resource, signal.Scope) signal.TenantID { return "team/_s3" }),
		WithTenancy(tenant.ResolverFunc(func(id signal.TenantID) tenant.Policy {
			if id == "team" {
				return tenant.Policy{Retention: tenant.Retention{MaxAge: 100}}
			}
			return tenant.Policy{}
		})))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, s.Close(context.Background())) })
	var now int64 = 1000
	s.now = func() int64 { return now }
	empty, ok := s.MetricReadRevision("team/_s3", 901)
	require.True(t, ok)
	require.Equal(t, int64(1001), empty.ValidUntil, "a local shard inherits its parent tenant's age")
	_, err = s.WriteMetrics(t.Context(), gaugeBatch("a", "counter", []int64{950}, []float64{1}))
	require.NoError(t, err)
	first, ok := s.MetricReadRevision("team/_s3", 901)
	require.True(t, ok)
	require.NotEqual(t, empty.Revision, first.Revision)
	parent, ok := s.MetricReadRevision("team", 901)
	require.True(t, ok)
	require.NotEqual(t, first.Revision, parent.Revision, "the revision identifies the shard engine, not its parent policy")
	_, err = s.WriteMetrics(t.Context(), gaugeBatch("a", "counter", []int64{960}, []float64{2}))
	require.NoError(t, err)
	second, ok := s.MetricReadRevision("team/_s3", 901)
	require.True(t, ok)
	require.NotEqual(t, first.Revision, second.Revision)
	now = empty.ValidUntil
	_, ok = s.MetricReadRevision("team/_s3", 901)
	require.False(t, ok, "the parent age expires the shard window without a write")
}

package storage

import (
	"context"
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/engine"
	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/signal"
)

// dropMetricEngine replaces a node's engine for the shard with a freshly built one over the same
// (part-less) backend and records the recovery gap that startup would: the engine a restarted owner
// comes back with, holding the shard's flushed parts and nothing of its head.
func dropMetricEngine(t *testing.T, s *Storage, shard signal.TenantID) {
	t.Helper()

	s.tmu.Lock()
	delete(s.tenants, shard)
	s.tmu.Unlock()

	_, err := s.engineFor(shard)
	require.NoError(t, err)

	s.noteReadGap(signal.Metric, shard, math.MinInt64)
}

// dropProfileEngine is [dropMetricEngine] for the profile signal, whose symbol store rides the head.
func dropProfileEngine(t *testing.T, s *Storage, shard signal.TenantID) {
	t.Helper()

	s.tmu.Lock()
	delete(s.profileTenants, shard)
	s.tmu.Unlock()

	_, err := s.profileEngineFor(shard)
	require.NoError(t, err)

	s.noteReadGap(signal.Profile, shard, math.MinInt64)
}

// TestAggregateDoesNotDivergeAcrossCoordinators: a node coordinating a metric aggregate over its own
// shards resolved placement and called the engine directly, skipping the completeness guard the
// peer-serving path applies. A restarted owner then aggregated from its parts alone while a peer —
// and its own [Storage.Fetcher], which is gap-guarded — answered in full, so the aggregate's value
// depended on which node was asked. Silent, not an error, so the assertion is equality across
// coordinators, not non-emptiness.
//
//nolint:paralleltest // owns an embedded etcd; runs serially
func TestAggregateDoesNotDivergeAcrossCoordinators(t *testing.T) {
	endpoint := startEtcd(t)
	ctx := context.Background()

	nodes := map[string]*Storage{
		"node-a": openClusterNode(t, endpoint, "node-a"),
		"node-b": openClusterNode(t, endpoint, "node-b"),
	}
	a, b := nodes["node-a"], nodes["node-b"]

	awaitMembership(t, nodes)

	_, err := a.WriteMetrics(ctx, gaugeBatch("api", "http.requests", []int64{100, 200}, []float64{1, 2}))
	require.NoError(t, err)

	req := fetch.Request{Start: 0, End: 1 << 60, Matchers: []fetch.Matcher{nameMatcher("http.requests")}}

	want, err := b.AggregateMetricsStepNamed(ctx, "default", req, 0)
	require.NoError(t, err)
	require.Len(t, want, 1, "the peer coordinator sees the replicated series")

	dropMetricEngine(t, a, shardKeyOf("default", 0, a.cluster.shardCount()))

	// The gap-guarded fetch on the same node stays complete: that is the asymmetry the aggregate
	// must not have.
	it, err := a.Fetcher("default").Fetch(ctx, req)
	require.NoError(t, err)
	batches, err := fetch.Drain(ctx, it)
	require.NoError(t, err)
	require.Len(t, batches, 1)
	assert.Equal(t, []float64{1, 2}, batches[0].Values, "Fetch on the restarted node is complete")

	got, err := a.AggregateMetricsStepNamed(ctx, "default", req, 0)
	require.NoError(t, err)
	assert.Equal(t, want, got, "the restarted coordinator must answer what a peer answers")

	spec := engine.WindowSpec{Step: 100, Window: 1000}

	wantWin, err := b.AggregateMetricsWindowNamed(ctx, "default", req, spec)
	require.NoError(t, err)
	require.NotEmpty(t, wantWin)

	gotWin, err := a.AggregateMetricsWindowNamed(ctx, "default", req, spec)
	require.NoError(t, err)
	assert.Equal(t, wantWin, gotWin, "the window aggregate must not diverge either")
}

// TestProfileSymbolsDoNotDivergeAcrossCoordinators: profile symbols are interned into the head as
// samples arrive, so a node that lost its head lost them too — yet the symbol RPC consulted no gap
// and served the truncated store as authoritative, resolving a flamegraph's stacks to nothing while
// the samples themselves failed over correctly. The store has no time domain, so the guard disclaims
// it outright.
//
//nolint:paralleltest // owns an embedded etcd; runs serially
func TestProfileSymbolsDoNotDivergeAcrossCoordinators(t *testing.T) {
	endpoint := startEtcd(t)
	ctx := context.Background()

	nodes := map[string]*Storage{
		"node-a": openClusterNode(t, endpoint, "node-a"),
		"node-b": openClusterNode(t, endpoint, "node-b"),
	}
	a, b := nodes["node-a"], nodes["node-b"]

	awaitMembership(t, nodes)

	_, err := a.WriteProfiles(ctx, profileBatch("api", 1000, sampleSpec{"cpu", "nanoseconds", 50}))
	require.NoError(t, err)

	want, err := b.clusterProfileSymbols(ctx, "default")
	require.NoError(t, err)
	require.NotEmpty(t, want, "the peer coordinator sees the replicated symbol store")

	dropProfileEngine(t, a, shardKeyOf("default", 0, a.cluster.shardCount()))

	got, err := a.clusterProfileSymbols(ctx, "default")
	require.NoError(t, err)
	assert.Equal(t, want, got, "the restarted coordinator must serve the peer's symbol store")

	res, err := a.ProfileResolver(ctx, "default")
	require.NoError(t, err)
	assert.NotNil(t, res)
}

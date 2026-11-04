package storage

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/cluster/etcd/etcdtest"
	"github.com/oteldb/storage/signal"
	"github.com/oteldb/storage/tenant"
)

//nolint:paralleltest // owns an embedded etcd; runs serially.
func TestPerSignalRetentionSurvivesClusterHandoff(t *testing.T) {
	endpoint := etcdtest.Start(t)
	ctx := t.Context()
	policy := WithTenancy(tenant.ResolverFunc(func(signal.TenantID) tenant.Policy {
		return tenant.Policy{Retention: tenant.Retention{MaxAge: 24 * time.Hour,
			PerSignal: map[signal.Signal]tenant.SignalRetention{signal.Exemplar: {MaxAge: time.Hour}}}}
	}))
	nodes := map[string]*Storage{
		"node-a": openClusterNodeWith(t, endpoint, "node-a", backend.Memory(), WithFlushInterval(-1), policy),
		"node-b": openClusterNodeWith(t, endpoint, "node-b", backend.Memory(), WithFlushInterval(-1), policy),
	}
	awaitMembership(t, nodes)
	primary, ok := nodes["node-a"].cluster.membership.Ring().Primary([]byte("default"))
	require.True(t, ok)
	owner := nodes[primary.ID]
	var replica *Storage
	for id, s := range nodes {
		if id != primary.ID {
			replica = s
		}
	}
	require.NotNil(t, replica)
	now := time.Now()
	old, fresh := now.Add(-6*time.Hour).UnixNano(), now.Add(-time.Minute).UnixNano()
	for _, ts := range []int64{old, fresh} {
		_, err := owner.WriteMetrics(ctx, exemplarBatch("api", "m", ts, map[string][]exemplarSpec{"/": {{ts: ts, value: 1, traceID: "trace"}}}))
		require.NoError(t, err)
	}
	for range 3 {
		owner.maintain(ctx)
		replica.maintain(ctx)
	}
	for _, sig := range []signal.Signal{signal.Metric, signal.Exemplar} {
		want, err := owner.PartsDetailed(ctx, "default", sig)
		require.NoError(t, err)
		require.NotEmpty(t, want)
		got, err := replica.PartsDetailed(ctx, "default", sig)
		require.NoError(t, err)
		require.Equal(t, want, got, "replica mirrors retained %s parts", sig)
		minTime := want[0].MinTime
		for _, p := range want {
			minTime = min(minTime, p.MinTime)
		}
		if sig == signal.Exemplar {
			require.GreaterOrEqual(t, minTime, now.Add(-time.Hour).UnixNano())
		} else {
			require.Equal(t, old, minTime)
		}
	}
	require.NoError(t, owner.Close(context.Background()))
	delete(nodes, primary.ID)
	awaitMembership(t, nodes)
	replica.maintain(ctx)
	require.True(t, replica.claimsShard("default"))
	for _, sig := range []signal.Signal{signal.Metric, signal.Exemplar} {
		parts, err := replica.PartsDetailed(ctx, "default", sig)
		require.NoError(t, err)
		require.NotEmpty(t, parts)
		minTime := parts[0].MinTime
		for _, p := range parts {
			minTime = min(minTime, p.MinTime)
		}
		if sig == signal.Exemplar {
			require.GreaterOrEqual(t, minTime, now.Add(-time.Hour).UnixNano())
		} else {
			require.Equal(t, old, minTime)
		}
	}
}

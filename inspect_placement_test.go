package storage

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/cluster/etcd/etcdtest"
	"github.com/oteldb/storage/signal"
	"github.com/oteldb/storage/tenant"
)

//nolint:paralleltest // Owns an embedded etcd.
func TestInspectPlacementsFollowRouting(t *testing.T) {
	endpoint := etcdtest.Start(t)
	policy := WithTenancy(tenant.ResolverFunc(func(id signal.TenantID) tenant.Policy {
		switch id {
		case "single":
			return tenant.Policy{Durability: tenant.Durability{RF: 1}}
		case "coded":
			return tenant.Policy{Durability: tenant.Durability{EC: &tenant.ECScheme{Data: 2, Parity: 1}}}
		default:
			return tenant.Policy{}
		}
	}))
	nodes := []*Storage{
		openClusterNodeSharded(t, endpoint, "a", 4, policy),
		openClusterNodeSharded(t, endpoint, "b", 4, policy),
		openClusterNodeSharded(t, endpoint, "c", 4, policy),
	}
	require.Eventually(t, func() bool {
		for _, node := range nodes {
			if ringSize(node) != len(nodes) {
				return false
			}
		}
		return true
	}, 10*time.Second, 10*time.Millisecond)

	shard := signal.TenantID("claim-only")
	primary := nodes[0].ownerLookup(shard)[0].ID
	var holder *Storage
	for i, node := range nodes {
		if []string{"a", "b", "c"}[i] == primary {
			holder = node
			break
		}
	}
	require.NotNil(t, holder)
	_, err := holder.cluster.ownership.Reconcile(context.Background(), holder.cluster.membership.Ring(), []string{string(shard)})
	require.NoError(t, err)

	stats := holder.clusterStats([]TenantStats{{Tenant: "single/_s0"}, {Tenant: "coded/_s2"}, {Tenant: "regular/_s1"}})
	require.Equal(t, []string{"claim-only"}, stats.Owned)
	require.Len(t, stats.Placements, 4)
	counts := map[string]int{"claim-only": 2, "coded/_s2": 3, "regular/_s1": 2, "single/_s0": 1}
	previous := ""
	for _, placement := range stats.Placements {
		require.Greater(t, placement.Shard, previous)
		previous = placement.Shard
		require.Len(t, placement.Owners, counts[placement.Shard])
		var want []string
		for _, owner := range holder.ownerLookup(signal.TenantID(placement.Shard)) {
			want = append(want, owner.ID)
		}
		require.Equal(t, want, placement.Owners)
	}
}

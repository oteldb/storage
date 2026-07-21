package storage

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestClusterECRoutedWritePreservesValues is a regression test for the interaction between a
// routed (non-primary origin) write, the full erasure-coding lifecycle, and the owner-prune /
// slot-filter prune. It pins two things:
//
//   - A write routed from a non-primary owner reaches the primary's head with its values intact
//     (the "routed-write value collapse" once suspected here was never a write-path fault — the
//     head is correct after the route).
//   - Driving the full EC settle (flush → convert → distribute → owner-prune) over many rounds
//     keeps every node's value column reconstructible. This is the scenario that surfaced the
//     prune bug where a replica dropped its own authoritative shard right after the owner pruned
//     its staged copy; without that protection reconstruction fails with "0 of Data shards
//     available".
//
//nolint:paralleltest // owns an embedded etcd; runs serially
func TestClusterECRoutedWritePreservesValues(t *testing.T) {
	endpoint := startEtcd(t)
	ctx := context.Background()

	nodes := map[string]*Storage{
		"n1": openClusterNodeECDomains(t, endpoint, "n1", []string{"rack1"}, 2, 1),
		"n2": openClusterNodeECDomains(t, endpoint, "n2", []string{"rack2"}, 2, 1),
		"n3": openClusterNodeECDomains(t, endpoint, "n3", []string{"rack3"}, 2, 1),
	}
	n1 := nodes["n1"]

	// Every node must see the full membership before we route: a routed write uses the origin
	// node's own ring view to pick the primary, so if the origin has not converged it routes
	// elsewhere and the primary we compute never receives it.
	require.Eventually(t, func() bool {
		for _, s := range nodes {
			if len(s.cluster.membership.Members()) != 3 {
				return false
			}
		}

		return true
	}, 15*time.Second, 50*time.Millisecond)

	primary, _ := n1.cluster.membership.Ring().Primary([]byte("default"))

	var nonPrimary *Storage
	for id, s := range nodes {
		if id != primary.ID {
			nonPrimary = s
		}
	}
	require.NotNil(t, nonPrimary)

	ts, vals := ecPayload(4096, 9)

	// Route the write from a non-primary origin.
	_, err := nonPrimary.WriteMetrics(ctx, gaugeBatch("api", "http.requests", ts, vals))
	require.NoError(t, err)

	// The primary's head has the values intact after the route (before any flush).
	pe, ok := nodes[primary.ID].lookupEngine("default")
	require.True(t, ok)
	head := queryEngine(t, pe, nameMatcher("http.requests"))
	require.Len(t, head, 1, "primary head has the routed series")
	require.Equal(t, vals, head[0].Values, "routed write reaches the head with values intact")

	// Drive the full EC lifecycle across many rounds, then every node must still reconstruct.
	owners := n1.cluster.membership.Ring().LookupBalanced([]byte("default"), 3)
	for range 8 {
		for _, o := range owners {
			nodes[o.ID].maintain(ctx)
		}
	}

	for id, s := range nodes {
		eng, ok := s.lookupEngine("default")
		require.Truef(t, ok, "%s engine", id)
		g := queryEngine(t, eng, nameMatcher("http.requests"))
		require.Lenf(t, g, 1, "%s serves the series after the full EC settle", id)
		assert.Equalf(t, vals, g[0].Values, "%s values survive routed write + EC settle", id)
	}
}

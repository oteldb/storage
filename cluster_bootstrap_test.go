package storage

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/query/fetch"
)

// TestClusterGainedOwnerBootstrap proves the spare-promotion path: a node that was never in a
// tenant's owner set (no engine, no data) is promoted after an owner is lost, discovers the
// tenant from the etcd compaction claims, mirrors its parts from the surviving owner, creates
// the engine, and serves the flushed data locally.
//
//nolint:paralleltest // owns an embedded etcd; runs serially
func TestClusterGainedOwnerBootstrap(t *testing.T) {
	endpoint := startEtcd(t)
	ctx := context.Background()

	nodes := map[string]*Storage{
		"n1": openClusterNodePrivate(t, endpoint, "n1", 2),
		"n2": openClusterNodePrivate(t, endpoint, "n2", 2),
		"n3": openClusterNodePrivate(t, endpoint, "n3", 2),
	}

	require.Eventually(t, func() bool {
		for _, s := range nodes {
			if len(s.cluster.membership.Members()) != 3 {
				return false
			}
		}

		return true
	}, 15*time.Second, 50*time.Millisecond)

	anyNode := nodes["n1"]
	owners := anyNode.cluster.membership.Ring().Lookup([]byte("default"), 2)
	require.Len(t, owners, 2)

	var spareID string
	for id := range nodes {
		if id != owners[0].ID && id != owners[1].ID {
			spareID = id
		}
	}
	require.NotEmpty(t, spareID)
	primary, secondary, spare := nodes[owners[0].ID], nodes[owners[1].ID], nodes[spareID]

	// Write through the primary, flush (claims the shard), and let the secondary mirror.
	ts := []int64{100, 200, 300}
	vals := []float64{1, 2, 3}
	_, err := primary.WriteMetrics(ctx, gaugeBatch("api", "http.requests", ts, vals))
	require.NoError(t, err)
	primary.maintain(ctx)
	secondary.maintain(ctx)

	// The spare has no engine for the tenant (it is not an owner).
	_, ok := spare.lookupEngine("default")
	require.False(t, ok, "the spare has no engine before promotion")

	// The secondary is permanently lost; the ring promotes the spare into the owner set.
	require.NoError(t, secondary.Close(ctx))
	delete(nodes, owners[1].ID)

	require.Eventually(t, func() bool {
		return len(spare.cluster.membership.Members()) == 2
	}, 15*time.Second, 100*time.Millisecond, "membership drops the lost owner")

	newOwners := spare.cluster.membership.Ring().Lookup([]byte("default"), 2)
	ownerIDs := map[string]bool{}
	for _, o := range newOwners {
		ownerIDs[o.ID] = true
	}
	require.True(t, ownerIDs[spareID], "the spare is now an owner")

	// The spare's maintenance bootstraps the shard: claims discovery → peer mirror → engine.
	require.Eventually(t, func() bool {
		primary.maintain(ctx) // keeps the claim alive and the parts served
		spare.maintain(ctx)

		eng, ok := spare.lookupEngine("default")

		return ok && eng.PartCount() == 1
	}, 15*time.Second, 200*time.Millisecond, "the spare bootstraps the engine and mirrors the part")

	// The spare serves the flushed data from its OWN backend (localFetch does not fan out).
	got, err := spare.localFetch(ctx, "default", 0, 1<<60, []fetch.Matcher{nameMatcher("http.requests")})
	require.NoError(t, err)
	require.Len(t, got, 1, "the spare serves the series locally")
	assert.Equal(t, ts, got[0].Timestamps)
	assert.Equal(t, vals, got[0].Values)
}

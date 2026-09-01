package storage

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/signal"
)

// TestClusterMirrorSkipsNonOwner pins that part mirroring is bounded by RF and not by membership
// (issue #455).
//
// The bug it guards is quiet and compounding: the mirror used to run for any node holding an engine
// for the prefix, so every member converged on a full copy — a 3-node RF=2 cluster stored 3 copies,
// and a 4th node would have stored 4. Nothing failed, the footprint was simply membership-sized, and
// startup recovery rebuilt the engine from whatever the last pass copied, so it sustained itself
// across restarts.
//
// RF=2 over three nodes is the shape that exposes it: exactly one node is outside the owner set, and
// it is the one that must copy nothing.
//
//nolint:paralleltest // owns an embedded etcd; runs serially
func TestClusterMirrorSkipsNonOwner(t *testing.T) {
	endpoint := startEtcd(t)
	ctx := context.Background()

	nodes := map[string]*Storage{
		"node-a": openClusterNodeWith(t, endpoint, "node-a", backend.Memory()),
		"node-b": openClusterNodeWith(t, endpoint, "node-b", backend.Memory()),
		"node-c": openClusterNodeWith(t, endpoint, "node-c", backend.Memory()),
	}
	awaitMembership(t, nodes)

	var owners, outsiders []*Storage

	for _, s := range nodes {
		if s.ownsShard("default") {
			owners = append(owners, s)
		} else {
			outsiders = append(outsiders, s)
		}
	}

	require.Len(t, owners, 2, "RF=2 places the tenant on two of the three nodes")
	require.Len(t, outsiders, 1, "leaving exactly one node outside the owner set")

	outsider := outsiders[0]

	_, err := owners[0].WriteMetrics(ctx, gaugeBatch("api", "m", []int64{100, 200, 300}, []float64{1, 2, 3}))
	require.NoError(t, err)

	// Flushing is the shard primary's to do, and either owner may hold that claim, so the one that
	// refuses is not a failure — what the test needs is that some owner produced parts.
	for _, o := range owners {
		if err := o.Admin().Flush(ctx, "default", signal.Metric); err == nil {
			break
		}
	}

	// Without this the test would pass on an empty cluster, proving only that nothing was there to
	// copy rather than that the outsider declined to copy it.
	var copyable int

	for _, o := range owners {
		keys, err := o.backend.List(ctx, "default"+metricsPrefix+"/")
		require.NoError(t, err)

		copyable += len(keys)
	}

	require.Positive(t, copyable, "an owner holds flushed objects for the outsider to have mirrored")

	assert.False(t, outsider.syncParts(ctx, "default", metricsPrefix, false),
		"a node outside the owner set reports nothing mirrored")

	got, err := outsider.backend.List(ctx, "default"+metricsPrefix+"/")
	require.NoError(t, err)
	assert.Empty(t, got, "and copies nothing into its private backend")
}

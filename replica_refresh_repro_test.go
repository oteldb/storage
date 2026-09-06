package storage

import (
	"context"
	"path"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend/bucketindex"
	"github.com/oteldb/storage/signal"
)

// TestRepro557ReplicaRefreshRecordsGonePart is the case #555's reconcile cannot heal: the part's
// objects are gone from every owner, so the replica's sync finds nothing to copy and its refresh
// opens a part that is not there. The refresh must drop the stale handle and leave an observable
// obligation; keeping the part in Parts() with nothing on disk is the defect.
//
//nolint:paralleltest // owns an embedded etcd; runs serially
func TestRepro557ReplicaRefreshRecordsGonePart(t *testing.T) {
	endpoint := startEtcd(t)
	ctx := context.Background()

	c := newReadPolicyCluster(t, endpoint)
	c.write(t)

	owner, replica := c.compactionOwnerOf(t)
	t.Logf("compaction owner=%s replica=%s", owner, replica)

	ownerEng, _ := c.nodes[owner].lookupRecordEngine(signal.Log, c.shard)
	partPrefix := ownerEng.Parts()[0].ID
	indexKey := path.Dir(partPrefix) + "/" + bucketindex.Object

	for range 2 {
		require.NoError(t, c.nodes[owner].Admin().MaintainNow(ctx))
		require.NoError(t, c.nodes[replica].Admin().MaintainNow(ctx))
	}

	ownerIndex := c.readIndex(t, owner, indexKey)
	require.Equal(t, ownerIndex, c.readIndex(t, replica, indexKey), "replica mirrors the owner's index")

	for _, id := range []string{owner, replica} {
		keys, err := c.backends[id].List(ctx, partPrefix)
		require.NoError(t, err)
		require.NotEmpty(t, keys, "%s holds the part", id)

		for _, k := range keys {
			require.NoError(t, c.backends[id].Delete(ctx, k))
		}
	}

	for i := range 3 {
		require.NoError(t, c.nodes[replica].Admin().MaintainNow(ctx))

		rp, rw, rh, rl := c.logStats(replica)

		keys, err := c.backends[replica].List(ctx, partPrefix)
		require.NoError(t, err)
		t.Logf("round %d: replica parts=%d wanted=%d holes=%d lost=%d, %d objects of the part on disk",
			i+1, rp, rw, rh, rl, len(keys))
	}

	keys, err := c.backends[replica].List(ctx, partPrefix)
	require.NoError(t, err)
	require.Empty(t, keys, "no owner holds the objects, so sync had nothing to mirror")

	require.Equal(t, ownerIndex, c.readIndex(t, replica, indexKey),
		"a replica never commits: its index is still the owner's")

	rp, rw, _, _ := c.logStats(replica)
	assert.Zero(t, rp, "a part with no objects is not served as present")
	assert.Equal(t, 1, rw, "the loss is visible as an outstanding want")

	replicaEng, _ := c.nodes[replica].lookupRecordEngine(signal.Log, c.shard)
	assert.True(t, replicaEng.WantOverlaps(0, 1<<62), "reads over the lost window disclaim")
}

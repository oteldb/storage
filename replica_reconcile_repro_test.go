package storage

import (
	"context"
	"path"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/backend/bucketindex"
	"github.com/oteldb/storage/signal"
)

// readIndex returns the raw bucket index a node's backend holds for the shard's log engine.
func (c *readPolicyCluster) readIndex(t *testing.T, id, indexKey string) []byte {
	t.Helper()

	raw, err := c.backends[id].Read(context.Background(), indexKey)
	require.NoError(t, err)

	return raw
}

// TestRepro551ReplicaLossUnderStableOwnerIndex is the replica-side mirror of #387/#544: the replica's
// disk loses a part's objects while the owner's index does not move, and the replica has to notice on
// its own. The loss is a bit-rot shape — no reload, no commit — so nothing on either side changes an
// index; TestRepro387ReplicaLossPropagatesToOwner recovers only because the replica's reload commits
// a want that the owner's repair then discharges with a commit, which is the coincidence removed here.
//
//nolint:paralleltest // owns an embedded etcd; runs serially
func TestRepro551ReplicaLossUnderStableOwnerIndex(t *testing.T) {
	endpoint := startEtcd(t)
	ctx := context.Background()

	c := newReadPolicyCluster(t, endpoint)
	c.write(t)

	owner, replica := c.compactionOwnerOf(t)
	t.Logf("compaction owner=%s replica=%s", owner, replica)

	ownerEng, _ := c.nodes[owner].lookupRecordEngine(signal.Log, c.shard)
	partPrefix := ownerEng.Parts()[0].ID
	indexKey := path.Dir(partPrefix) + "/" + bucketindex.Object

	// Let both sides settle so the indexes are byte-identical before the loss.
	for range 2 {
		require.NoError(t, c.nodes[owner].Admin().MaintainNow(ctx))
		require.NoError(t, c.nodes[replica].Admin().MaintainNow(ctx))
	}

	ownerIndex := c.readIndex(t, owner, indexKey)
	require.Equal(t, ownerIndex, c.readIndex(t, replica, indexKey), "replica mirrors the owner's index")

	keys, err := c.backends[replica].List(ctx, partPrefix)
	require.NoError(t, err)
	require.NotEmpty(t, keys, "replica mirrored the part")

	for _, k := range keys {
		require.NoError(t, c.backends[replica].Delete(ctx, k))
	}

	for i := range 5 {
		require.NoError(t, c.nodes[owner].Admin().MaintainNow(ctx))
		require.NoError(t, c.nodes[replica].Admin().MaintainNow(ctx))

		rp, rw, rh, rl := c.logStats(replica)

		keys, err = c.backends[replica].List(ctx, partPrefix)
		require.NoError(t, err)
		t.Logf("round %d: replica parts=%d wanted=%d holes=%d lost=%d, %d objects of the part on disk",
			i+1, rp, rw, rh, rl, len(keys))
	}

	require.Equal(t, ownerIndex, c.readIndex(t, owner, indexKey),
		"the owner's index never moved: only the replica's own reconcile can bring the part back")

	keys, err = c.backends[replica].List(ctx, partPrefix)
	require.NoError(t, err)
	require.NotEmpty(t, keys, "the replica re-copies the objects its index names")

	rp, rw, rh, _ := c.logStats(replica)
	require.Equal(t, 1, rp, "the replica serves the part again")
	require.Zero(t, rw+rh, "with no want and no hole for a part the owner holds")

	c.requireBothRecords(t, replica)
	c.requireBothRecords(t, owner)

	_, err = backend.ReadView(ctx, c.backends[replica], partPrefix+"/manifest")
	require.NoError(t, err, "the copy is complete")
}

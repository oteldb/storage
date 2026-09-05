package storage

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/signal"
)

// compactionOwnerOf returns the shard's compaction owner and the other ring owner.
func (c *readPolicyCluster) compactionOwnerOf(t *testing.T) (owner, replica string) {
	t.Helper()
	ctx := context.Background()

	holders := c.owners(t)
	for _, id := range holders {
		owned := c.nodes[id].ownedTenants(ctx, map[signal.TenantID]struct{}{c.shard: {}})
		if _, ok := owned[c.shard]; ok {
			require.Empty(t, owner, "two compaction owners")
			owner = id
		} else {
			replica = id
		}
	}

	require.NotEmpty(t, owner)
	require.NotEmpty(t, replica)

	return owner, replica
}

func (c *readPolicyCluster) logStats(id string) (parts int, wanted int, holes int, lost uint64) {
	eng, ok := c.nodes[id].lookupRecordEngine(signal.Log, c.shard)
	if !ok {
		return 0, 0, 0, 0
	}

	st := eng.Stats()

	return len(eng.Parts()), st.WantedParts, st.Holes, st.LostParts
}

//nolint:paralleltest // owns an embedded etcd; runs serially
func TestRepro387ReplicaInstallsDamagedIndexThenOwnerHoles(t *testing.T) {
	endpoint := startEtcd(t)
	ctx := context.Background()

	c := newReadPolicyCluster(t, endpoint)
	c.write(t)

	owner, replica := c.compactionOwnerOf(t)
	t.Logf("compaction owner=%s replica=%s", owner, replica)

	ownerEng, _ := c.nodes[owner].lookupRecordEngine(signal.Log, c.shard)
	lostPrefix := ownerEng.Parts()[0].ID

	// Replica really holds the part's objects before anything happens.
	keys, err := c.backends[replica].List(ctx, lostPrefix)
	require.NoError(t, err)
	require.NotEmpty(t, keys, "replica mirrored the part")

	require.True(t, c.loseOldestPart(t, owner))

	p, w, h, l := c.logStats(owner)
	t.Logf("owner after loss:   parts=%d wanted=%d holes=%d lost=%d", p, w, h, l)

	// Replica's maintenance tick runs before the owner's: it pulls the owner's newer index.
	require.NoError(t, c.nodes[replica].Admin().MaintainNow(ctx))

	p, w, h, l = c.logStats(replica)
	t.Logf("replica after sync: parts=%d wanted=%d holes=%d lost=%d", p, w, h, l)

	keys, err = c.backends[replica].List(ctx, lostPrefix)
	require.NoError(t, err)
	t.Logf("replica still holds %d objects of the lost part on disk", len(keys))

	// Owner runs repair passes.
	for i := range 5 {
		require.NoError(t, c.nodes[owner].Admin().MaintainNow(ctx))

		p, w, h, l = c.logStats(owner)
		rs := ownerEng.RepairStats()
		t.Logf("owner pass %d: parts=%d wanted=%d holes=%d lost=%d repair=%+v", i+1, p, w, h, l, rs)
	}

	keys, err = c.backends[replica].List(ctx, lostPrefix)
	require.NoError(t, err)
	t.Logf("replica STILL holds %d objects of the lost part on disk", len(keys))

	p, w, h, l = c.logStats(owner)
	require.Zero(t, h, "DESIGN EXPECTATION: the part is on the replica's disk, so no hole should be committed")
	require.Zero(t, l, "DESIGN EXPECTATION: no data was lost cluster-wide")
	require.Equal(t, 1, p, "DESIGN EXPECTATION: the part is repaired back")
	require.Zero(t, w, "DESIGN EXPECTATION: the repair discharges the want")

	require.NoError(t, c.nodes[replica].Admin().MaintainNow(ctx))

	rp, rw, rh, _ := c.logStats(replica)
	require.Equal(t, 1, rp, "DESIGN EXPECTATION: the replica goes on serving the part")
	require.Zero(t, rw+rh, "DESIGN EXPECTATION: and adopts the owner's repaired index")
}

//nolint:paralleltest // owns an embedded etcd; runs serially
func TestRepro387HoleFreezesReplicaIndexInstall(t *testing.T) {
	endpoint := startEtcd(t)
	ctx := context.Background()

	c := newReadPolicyCluster(t, endpoint)
	c.write(t)

	owner, replica := c.compactionOwnerOf(t)
	t.Logf("compaction owner=%s replica=%s", owner, replica)

	// Lose the part on BOTH owners so the hole is legitimately earned.
	require.True(t, c.loseOldestPart(t, owner))
	require.True(t, c.loseOldestPart(t, replica))

	ownerEng, _ := c.nodes[owner].lookupRecordEngine(signal.Log, c.shard)

	require.Eventually(t, func() bool {
		_ = c.nodes[owner].Admin().MaintainNow(ctx)
		_ = c.nodes[replica].Admin().MaintainNow(ctx)

		return ownerEng.Stats().Holes == 1
	}, 20*time.Second, 100*time.Millisecond, "owner commits a hole")

	p, w, h, l := c.logStats(owner)
	t.Logf("owner with hole:    parts=%d wanted=%d holes=%d lost=%d", p, w, h, l)

	// Now the owner flushes new data; the replica should mirror it.
	_, err := c.nodes["node-a"].WriteLogs(ctx,
		logBatch("api", [3]any{1000, 9, "third"}, [3]any{1100, 17, "fourth"}))
	require.NoError(t, err)

	ok := false

	for i := range 8 {
		for _, s := range c.nodes {
			_ = s.Admin().MaintainNow(ctx)
		}

		op, _, oh, _ := c.logStats(owner)
		rp, rw, rh, rl := c.logStats(replica)
		t.Logf("round %d: owner parts=%d holes=%d | replica parts=%d wanted=%d holes=%d lost=%d",
			i+1, op, oh, rp, rw, rh, rl)

		if rp >= op && rh == oh {
			ok = true

			break
		}
	}

	require.True(t, ok, "DESIGN EXPECTATION: the replica converges on the owner's index (parts and hole) after a hole exists")
}

//nolint:paralleltest // owns an embedded etcd; runs serially
func TestRepro387ReplicaLossPropagatesToOwner(t *testing.T) {
	endpoint := startEtcd(t)
	ctx := context.Background()

	c := newReadPolicyCluster(t, endpoint)
	c.write(t)

	owner, replica := c.compactionOwnerOf(t)
	t.Logf("compaction owner=%s replica=%s", owner, replica)

	ownerEng, _ := c.nodes[owner].lookupRecordEngine(signal.Log, c.shard)
	prefix := ownerEng.Parts()[0].ID

	// Only the REPLICA loses the part and restarts (recovery = LoadParts with sweep).
	require.True(t, c.loseOldestPart(t, replica))

	p, w, h, l := c.logStats(replica)
	t.Logf("replica after loss: parts=%d wanted=%d holes=%d lost=%d", p, w, h, l)

	for i := range 5 {
		require.NoError(t, c.nodes[owner].Admin().MaintainNow(ctx))
		require.NoError(t, c.nodes[replica].Admin().MaintainNow(ctx))

		op, ow, oh, ol := c.logStats(owner)
		rp, rw, rh, rl := c.logStats(replica)
		t.Logf("round %d: owner parts=%d wanted=%d holes=%d lost=%d | replica parts=%d wanted=%d holes=%d lost=%d",
			i+1, op, ow, oh, ol, rp, rw, rh, rl)
	}

	keys, err := c.backends[owner].List(ctx, prefix)
	require.NoError(t, err)
	t.Logf("owner holds %d objects of the part on disk", len(keys))

	op, ow, oh, ol := c.logStats(owner)
	require.Equal(t, 1, op, "DESIGN EXPECTATION: owner still serves its own intact part")
	require.Zero(t, ow, "DESIGN EXPECTATION: owner owes nothing")
	require.Zero(t, oh+int(ol), "DESIGN EXPECTATION: no hole, no loss")

	rp, _, rh, _ := c.logStats(replica)
	require.Equal(t, 1, rp, "DESIGN EXPECTATION: replica re-mirrors the part from the owner")
	require.Zero(t, rh, "DESIGN EXPECTATION: and adopts no hole for a part that exists")
}

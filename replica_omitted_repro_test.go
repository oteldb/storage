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

// TestRepro556OwnerSilentlyOmitsAPartTheReplicaHolds is the omission half of #387/#544: the owner's
// index stops naming a part without saying anything about it — a stale-snapshot restore, a partial
// rm — and goes on committing, so it supersedes. The replica holds the only surviving copy, and
// installing that index verbatim would make the rows unreachable while the conservative deletion
// rule keeps the bytes, so nothing reclaims them either.
//
//nolint:paralleltest // owns an embedded etcd; runs serially
func TestRepro556OwnerSilentlyOmitsAPartTheReplicaHolds(t *testing.T) {
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

	require.Equal(t, c.readIndex(t, owner, indexKey), c.readIndex(t, replica, indexKey),
		"replica mirrors the owner's index")

	keys, err := c.backends[replica].List(ctx, partPrefix)
	require.NoError(t, err)
	require.NotEmpty(t, keys, "replica mirrored the part")

	// The owner's index forgets the part: no tombstone, no want, no hole, and one generation on, so
	// it supersedes everything the replica has. Only the replica's maintenance runs from here, so
	// the owner's own next commit cannot undo the forgery.
	forgetPart(t, c.backends[owner], indexKey, partPrefix)

	for i := range 3 {
		require.NoError(t, c.nodes[replica].Admin().MaintainNow(ctx))

		rp, rw, rh, rl := c.logStats(replica)
		keys, err = c.backends[replica].List(ctx, partPrefix)
		require.NoError(t, err)
		t.Logf("round %d: replica parts=%d wanted=%d holes=%d lost=%d, %d objects of the part on disk",
			i+1, rp, rw, rh, rl, len(keys))
	}

	keys, err = c.backends[replica].List(ctx, partPrefix)
	require.NoError(t, err)
	require.NotEmpty(t, keys, "the deletion rule withholds the objects, as it must")

	got, err := bucketindex.Load(ctx, c.backends[replica], indexKey)
	require.NoError(t, err)
	require.Contains(t, partPrefixesOf(got), partPrefix,
		"so the installed index has to go on naming them: unreachable and unreclaimable is not an outcome")

	rp, rw, rh, rl := c.logStats(replica)
	require.Equal(t, 1, rp, "the replica serves the part it holds")
	require.Zero(t, rw+rh, "and invents no obligation for a part it can read")
	require.Zero(t, rl, "nor a cluster-wide loss the peer never claimed")

	c.requireBothRecords(t, replica)

	_, err = backend.ReadView(ctx, c.backends[replica], partPrefix+"/manifest")
	require.NoError(t, err, "the copy is complete")
}

// forgetPart rewrites the node's bucket index without partPrefix and one generation on, stating
// nothing about where the part went.
func forgetPart(t *testing.T, be backend.Backend, indexKey, partPrefix string) {
	t.Helper()
	ctx := context.Background()

	ix, version, err := bucketindex.LoadVersioned(ctx, be, indexKey)
	require.NoError(t, err)

	out := &bucketindex.Index{
		Generation:   ix.Generation.Next(1),
		FlushedEpoch: ix.FlushedEpoch,
		Epochs:       ix.Epochs,
		Removed:      ix.Removed,
		Wanted:       ix.Wanted,
		LostParts:    ix.LostParts,
	}

	for i := range ix.Entries {
		if ix.Entries[i].Prefix != partPrefix {
			out.Add(ix.Entries[i])
		}
	}

	require.NotEqual(t, len(ix.Entries), len(out.Entries), "the part was in the index to begin with")

	_, err = out.Save(ctx, be, indexKey, version)
	require.NoError(t, err)
}

func partPrefixesOf(ix *bucketindex.Index) []string {
	out := make([]string, 0, len(ix.Entries))
	for i := range ix.Entries {
		out = append(out, ix.Entries[i].Prefix)
	}

	return out
}

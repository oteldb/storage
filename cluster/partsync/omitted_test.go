package partsync_test

// A superseding peer index that simply *omits* a part makes no claim about it: unlike a want or a
// hole, an omission is not a statement. It is either a part the owner legitimately dropped — a
// merge consumed it, retention outlived it — or one the owner silently lost. These tests pin the
// two apart: an omission the peer explains is obeyed, one it cannot explain is not.

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/backend/bucketindex"
	"github.com/oteldb/storage/cluster/partsync"
)

const omitPrefix = "t/omit"

func omitPart(seq int) string { return omitPrefix + "/000000000" + string(rune('0'+seq)) }

// mirrorOmitParts stages an owner holding parts 1 and 2 at gen(1,5) and a replica that mirrored
// both, and returns the owner's index, its address and the syncer.
func mirrorOmitParts(t *testing.T, owner, replica backend.Backend) (*bucketindex.Index, string, *partsync.Syncer) {
	t.Helper()
	ctx := context.Background()

	ix := writeParts(t, owner, omitPrefix, gen(1, 5), 1, 2)
	addr := serve(t, owner)
	s := partsync.New(replica, &partsync.Client{})

	_, err := s.Sync(ctx, omitPrefix, []string{addr}, false, nil)
	require.NoError(t, err)
	_, err = replica.Read(ctx, omitPart(1)+"/manifest")
	require.NoError(t, err, "the replica mirrored both parts")

	return ix, addr, s
}

// omitIndex is ix without part seq: the entry gone, no tombstone, no want, no hole — the shape a
// stale-snapshot restore or a partial rm leaves behind on a writer that goes on committing.
func omitIndex(ix *bucketindex.Index, seq int, g bucketindex.Generation) *bucketindex.Index {
	out := &bucketindex.Index{Generation: g, Removed: ix.Removed}
	for _, e := range ix.Entries {
		if e.Prefix != omitPart(seq) {
			out.Add(e)
		}
	}

	return out
}

func replicaOmitIndex(t *testing.T, replica backend.Backend) *bucketindex.Index {
	t.Helper()

	ix, err := bucketindex.Load(context.Background(), replica, omitPrefix+"/"+bucketindex.Object)
	require.NoError(t, err)

	return ix
}

// TestRepro556SupersedingPeerOmitsAHeldPart pins the pair that has to move together. Dropping the
// entry while the deletion rule withholds the objects is the worst of both: nothing names the rows
// and nothing reclaims the bytes.
func TestRepro556SupersedingPeerOmitsAHeldPart(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	owner, replica := backend.Memory(), backend.Memory()
	ix, addr, s := mirrorOmitParts(t, owner, replica)

	for _, suffix := range []string{"/c/0", "/marks", "/manifest"} {
		require.NoError(t, owner.Delete(ctx, omitPart(1)+suffix))
	}

	// The owner goes on committing after the loss, so every one of its writes supersedes.
	shrunk := omitIndex(ix, 1, gen(1, 6))
	saveIndex(t, owner, omitPrefix, shrunk)

	for pass := range 3 {
		st, err := s.Sync(ctx, omitPrefix, []string{addr}, false, nil)
		require.NoErrorf(t, err, "pass %d", pass)
		assert.Zerof(t, st.Pruned, "pass %d pruned a part the peer never said it removed", pass)

		writePart(t, owner, shrunk, omitPrefix, 3+pass, int64(pass)*1000, int64(pass)*1000+100)
		shrunk.Generation = shrunk.Generation.Next(1)
		saveIndex(t, owner, omitPrefix, shrunk)
	}

	_, err := replica.Read(ctx, omitPart(1)+"/manifest")
	require.NoError(t, err, "the replica's only good copy is still on disk")

	got := replicaOmitIndex(t, replica)
	assert.Contains(t, partPrefixes(got), omitPart(1),
		"the objects are here and nothing authorized deleting them, so the index must go on naming them")
	assert.Contains(t, partPrefixes(got), omitPart(2), "the parts the peer does name are mirrored")
}

// A merge is an explanation the index carries on its own: the successor's identity says it covers
// the omitted part's blocks at a higher level, so the rows are inside it. Nothing is resurrected,
// and compaction is not defeated by the rule above — which the tombstone the merge also writes then
// turns into a reclaim.
func TestSyncObeysAnOmissionAMergeExplains(t *testing.T) {
	t.Parallel()
	t.Run("tombstoned", func(t *testing.T) { t.Parallel(); syncOmissionAfterMerge(t, true) })
	t.Run("tombstone aged out", func(t *testing.T) { t.Parallel(); syncOmissionAfterMerge(t, false) })
}

func syncOmissionAfterMerge(t *testing.T, tombstone bool) {
	t.Helper()
	ctx := context.Background()

	owner, replica := backend.Memory(), backend.Memory()

	ix := &bucketindex.Index{Generation: gen(1, 5)}
	writePart(t, owner, ix, omitPrefix, 1, 100, 150)
	writePart(t, owner, ix, omitPrefix, 2, 200, 250)
	ix.Entries[0].Blocks, ix.Entries[0].Level = bucketindex.Interval{Min: 1, Max: 1}, 0
	ix.Entries[1].Blocks, ix.Entries[1].Level = bucketindex.Interval{Min: 2, Max: 2}, 0
	saveIndex(t, owner, omitPrefix, ix)

	addr := serve(t, owner)
	s := partsync.New(replica, &partsync.Client{})

	_, err := s.Sync(ctx, omitPrefix, []string{addr}, false, nil)
	require.NoError(t, err)

	// Parts 1 and 2 merge into part 3, covering blocks [1,2] at level 1.
	for _, seq := range []int{1, 2} {
		for _, suffix := range []string{"/c/0", "/marks", "/manifest"} {
			require.NoError(t, owner.Delete(ctx, omitPart(seq)+suffix))
		}
	}

	merged := &bucketindex.Index{Generation: gen(1, 6)}
	writePart(t, owner, merged, omitPrefix, 3, 100, 250)
	merged.Entries[0].Blocks, merged.Entries[0].Level = bucketindex.Interval{Min: 1, Max: 2}, 1

	if tombstone {
		for _, seq := range []int{1, 2} {
			merged.Tombstone(bucketindex.Removal{Prefix: omitPart(seq), Generation: merged.Generation})
		}
	}

	saveIndex(t, owner, omitPrefix, merged)

	var retained int

	for range 3 {
		st, err := s.Sync(ctx, omitPrefix, []string{addr}, false, nil)
		require.NoError(t, err)
		retained += st.Retained

		merged.Generation = merged.Generation.Next(1)
		saveIndex(t, owner, omitPrefix, merged)
	}

	assert.Zero(t, retained, "a merge accounts for its inputs, so nothing is held back")

	got := replicaOmitIndex(t, replica)
	assert.Equal(t, []string{omitPart(3)}, partPrefixes(got), "the merge output replaces its inputs")

	for _, seq := range []int{1, 2} {
		_, err = replica.Read(ctx, omitPart(seq)+"/manifest")
		if tombstone {
			require.ErrorIsf(t, err, backend.ErrNotExist, "part %d is still reclaimed", seq)
		} else {
			require.NoErrorf(t, err, "an aged-out tombstone leaves bounded garbage, not a deletion")
		}
	}
}

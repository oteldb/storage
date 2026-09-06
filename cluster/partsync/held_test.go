package partsync_test

// A peer's index is a copy of whichever owner's index last superseded it: it says nothing about the
// peer's disk, and even less about this one. So a peer index that reports a part lost — as a want
// or as a hole — does not remove a part this node still holds; the part stays in Entries with the
// claim carried beside it as a want, for the owner's repair to reconcile with a commit.

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/backend/bucketindex"
	"github.com/oteldb/storage/cluster/partsync"
)

const heldPrefix = "t/logs"

// mirrorTwoParts stages an owner holding parts 1 and 2 at gen(1,5) and a replica that mirrored
// both, and returns the owner's index, its address and the syncer.
func mirrorTwoParts(t *testing.T, owner, replica backend.Backend) (*bucketindex.Index, string, *partsync.Syncer) {
	t.Helper()

	ix := writeParts(t, owner, heldPrefix, gen(1, 5), 1, 2)
	addr := serve(t, owner)

	s := partsync.New(replica, &partsync.Client{})

	_, err := s.Sync(context.Background(), heldPrefix, []string{addr}, false, nil)
	require.NoError(t, err)
	_, err = replica.Read(context.Background(), heldPrefix+"/0000000001/manifest")
	require.NoError(t, err, "the replica mirrored the part")

	return ix, addr, s
}

// loseOnOwner destroys part 1 on the owner and commits the index the owner's own reload would: the
// entry gone, the want (or the hole acknowledging it) in its place, one generation on.
func loseOnOwner(t *testing.T, owner backend.Backend, ix *bucketindex.Index, hole bool) (*bucketindex.Index, bucketindex.Entry) {
	t.Helper()
	ctx := context.Background()

	part := heldPrefix + "/0000000001"
	for _, suffix := range []string{"/c/0", "/marks", "/manifest"} {
		require.NoError(t, owner.Delete(ctx, part+suffix))
	}

	var lost bucketindex.Entry

	damaged := &bucketindex.Index{Generation: gen(1, 6)}
	for i := range ix.Entries {
		e := &ix.Entries[i]
		if e.Prefix == part {
			lost = *e

			continue
		}

		damaged.Add(*e)
	}

	w := bucketindex.WantOf(lost, damaged.Generation)
	if hole {
		damaged.RecordHole(w)
	} else {
		damaged.RecordWant(w)
	}

	saveIndex(t, owner, heldPrefix, damaged)

	return damaged, lost
}

func replicaIndex(t *testing.T, replica backend.Backend) *bucketindex.Index {
	t.Helper()

	ix, err := bucketindex.Load(context.Background(), replica, heldPrefix+"/"+bucketindex.Object)
	require.NoError(t, err)

	return ix
}

func TestSyncKeepsHeldPartThePeerWants(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	owner, replica := backend.Memory(), backend.Memory()
	ix, addr, s := mirrorTwoParts(t, owner, replica)
	damaged, lost := loseOnOwner(t, owner, ix, false)

	st, err := s.Sync(ctx, heldPrefix, []string{addr}, false, nil)
	require.NoError(t, err)
	require.True(t, st.Synced, "the owner's index is newer and is installed")

	got := replicaIndex(t, replica)
	assert.Equal(t, damaged.Generation, got.Generation)
	assert.Equal(t, partPrefixes(ix), partPrefixes(got), "the held part stays in Entries")

	ent, ok := got.Satisfying(bucketindex.WantOf(lost, gen(0, 0)))
	require.True(t, ok)
	assert.Equal(t, lost, ent)

	require.Len(t, got.Wanted, 1)
	assert.Equal(t, lost.Prefix, got.Wanted[0].Prefix, "the peer's claim rides beside the entry for the owner's repair to discharge")

	_, err = replica.Read(ctx, lost.Prefix+"/manifest")
	require.NoError(t, err, "the objects are protected, not pruned")

	st, err = s.Sync(ctx, heldPrefix, []string{addr}, false, nil)
	require.NoError(t, err)
	assert.False(t, st.Synced, "the kept entry is what the local index says now, so nothing is newer")
	assert.Zero(t, st.Pruned)
}

func TestSyncKeepsHeldPartThePeerHoled(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	owner, replica := backend.Memory(), backend.Memory()
	ix, addr, s := mirrorTwoParts(t, owner, replica)
	_, lost := loseOnOwner(t, owner, ix, true)

	st, err := s.Sync(ctx, heldPrefix, []string{addr}, false, nil)
	require.NoError(t, err)
	require.True(t, st.Synced)

	got := replicaIndex(t, replica)
	assert.Empty(t, got.Holes(), "an entry and a hole cannot share a prefix; the data wins")
	assert.EqualValues(t, 1, got.LostParts, "the loss counter never falls")
	assert.Equal(t, partPrefixes(ix), partPrefixes(got))

	require.Len(t, got.Wanted, 1)
	assert.Equal(t, lost.Prefix, got.Wanted[0].Prefix, "the hole becomes the want it discharged")
}

func TestSyncAdoptsPeerWantForAPartNotHeld(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	owner, replica := backend.Memory(), backend.Memory()
	ix, addr, s := mirrorTwoParts(t, owner, replica)
	damaged, lost := loseOnOwner(t, owner, ix, false)

	// This replica's copy is incomplete too: without the manifest there is no readable part to keep.
	require.NoError(t, replica.Delete(ctx, lost.Prefix+"/manifest"))

	st, err := s.Sync(ctx, heldPrefix, []string{addr}, false, nil)
	require.NoError(t, err)
	require.True(t, st.Synced)

	got := replicaIndex(t, replica)
	assert.Equal(t, partPrefixes(damaged), partPrefixes(got), "the peer's index is installed as it is")
	assert.Equal(t, damaged.Wanted, got.Wanted)
}

// A hole names no objects, so a listing that shows none for it is not an index outrunning its
// objects: the index is backed and is installed.
func TestSyncInstallsIndexCarryingAHole(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	owner, replica := backend.Memory(), backend.Memory()
	ix := writeParts(t, owner, heldPrefix, gen(1, 5), 1, 2)
	damaged, lost := loseOnOwner(t, owner, ix, true)
	addr := serve(t, owner)

	s := partsync.New(replica, &partsync.Client{})

	st, err := s.Sync(ctx, heldPrefix, []string{addr}, false, nil)
	require.NoError(t, err)
	require.True(t, st.Synced)

	got := replicaIndex(t, replica)
	assert.Equal(t, damaged.Generation, got.Generation, "the index with the hole is installed")
	assert.Equal(t, partPrefixes(damaged), partPrefixes(got))

	holes := got.Holes()
	require.Len(t, holes, 1)
	assert.Equal(t, lost.Prefix, holes[0].Prefix)
}

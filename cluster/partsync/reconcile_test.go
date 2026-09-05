package partsync_test

// A converged index is not evidence of the disk under it. A replica whose objects go away while the
// owner's index stands still has to notice on its own, and the noticing has to stay cheap: a local
// listing against the peer's key set from the last pass, and the peer asked only when something is
// gone or when this process has no last pass to trust.

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/backend/bucketindex"
	"github.com/oteldb/storage/cluster/partsync"
)

const reconcilePrefix = "t/logs"

// partObjects is every object writePart lands under seq.
func partObjects(seq int) []string {
	p := reconcilePrefix + "/000000000" + itoa(seq)

	return []string{p + "/c/0", p + "/marks", p + "/manifest"}
}

// stageConverged serves an owner holding parts 1 and 2 and mirrors it into replica until a pass is a
// no-op, returning the peer with its counters zeroed at that point.
func stageConverged(t *testing.T, replica backend.Backend) (*probePeer, *partsync.Syncer, *bucketindex.Index) {
	t.Helper()
	ctx := context.Background()

	owner := backend.Memory()
	ix := writeParts(t, owner, reconcilePrefix, gen(1, 1), 1, 2)

	peer := servePeer(t, owner, reconcilePrefix, probeOpts{})
	s := partsync.New(replica, &partsync.Client{})

	st, err := s.Sync(ctx, reconcilePrefix, []string{peer.addr}, false, nil)
	require.NoError(t, err)
	require.True(t, st.Synced)

	peer.reset()

	return peer, s, ix
}

func deleteAll(t *testing.T, be backend.Backend, keys []string) {
	t.Helper()

	for _, k := range keys {
		require.NoError(t, be.Delete(context.Background(), k))
	}
}

func requireHeld(t *testing.T, be backend.Backend, keys []string) {
	t.Helper()

	for _, k := range keys {
		_, err := be.Read(context.Background(), k)
		require.NoErrorf(t, err, "%s is on disk", k)
	}
}

// TestSyncReconcilesObjectsLostUnderAnUnchangedIndex is #551: the owner's index never moves, the
// replica's copy of a part goes away, and the next pass brings it back.
func TestSyncReconcilesObjectsLostUnderAnUnchangedIndex(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	replica := backend.Memory()
	peer, s, _ := stageConverged(t, replica)

	// Converged and intact: the index is read, the disk is not asked of the peer.
	st, err := s.Sync(ctx, reconcilePrefix, []string{peer.addr}, false, nil)
	require.NoError(t, err)
	assert.False(t, st.Synced)
	assert.Equal(t, 1, peer.indexFetches(reconcilePrefix))
	assert.Zero(t, peer.listCalls(), "nothing is missing, so the peer's disk is not listed")
	peer.reset()

	deleteAll(t, replica, partObjects(1))

	st, err = s.Sync(ctx, reconcilePrefix, []string{peer.addr}, false, nil)
	require.NoError(t, err)
	require.True(t, st.Synced, "the loss is a change worth mirroring even though no index moved")
	assert.Equal(t, 3, st.Copied, "exactly the lost part's objects come back")
	assert.Zero(t, st.Pruned)
	requireHeld(t, replica, partObjects(1))
	requireHeld(t, replica, partObjects(2))

	assert.Equal(t, 1, peer.indexFetches(reconcilePrefix))
	assert.Equal(t, 1, peer.listCalls(), "one listing of the prefix, as any mirroring pass costs")

	for _, k := range partObjects(1) {
		assert.Equal(t, 1, peer.count(k), "%s fetched once", k)
	}

	for _, k := range partObjects(2) {
		assert.Zero(t, peer.count(k), "%s was never lost and is not re-fetched", k)
	}

	peer.reset()

	// Converged again: back to the index-only cost.
	st, err = s.Sync(ctx, reconcilePrefix, []string{peer.addr}, false, nil)
	require.NoError(t, err)
	assert.False(t, st.Synced)
	assert.Zero(t, peer.listCalls())
}

// TestSyncVerifiesTheDiskOnceOnLoad pins the other trigger: a process has no last pass, so its first
// pass over a converged prefix lists the peer once and copies whatever a restart found gone, and
// every pass after that is index-only again.
func TestSyncVerifiesTheDiskOnceOnLoad(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	replica := backend.Memory()
	peer, _, _ := stageConverged(t, replica)

	deleteAll(t, replica, partObjects(2))

	// A new Syncer is a restarted process: it holds no key set from any earlier pass.
	restarted := partsync.New(replica, &partsync.Client{})

	st, err := restarted.Sync(ctx, reconcilePrefix, []string{peer.addr}, false, nil)
	require.NoError(t, err)
	assert.Equal(t, 3, st.Copied)
	requireHeld(t, replica, partObjects(2))
	assert.Equal(t, 1, peer.indexFetches(reconcilePrefix))
	assert.Equal(t, 1, peer.listCalls(), "the first pass verifies the disk against the peer once")
	peer.reset()

	st, err = restarted.Sync(ctx, reconcilePrefix, []string{peer.addr}, false, nil)
	require.NoError(t, err)
	assert.False(t, st.Synced)
	assert.Equal(t, 1, peer.indexFetches(reconcilePrefix))
	assert.Zero(t, peer.listCalls(), "verified: later passes do not list the peer")
}

// TestSyncStrictNeverReconcilesTheDisk keeps the owner out of it: an owner backfilling in strict mode
// repairs its own losses through the engine's want path, and mirroring a replica's objects onto an
// owner would resurrect every part the owner had just merged away.
func TestSyncStrictNeverReconcilesTheDisk(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	replica := backend.Memory()
	peer, s, _ := stageConverged(t, replica)

	deleteAll(t, replica, partObjects(1))

	for range 2 {
		st, err := s.Sync(ctx, reconcilePrefix, []string{peer.addr}, true, nil)
		require.NoError(t, err)
		assert.False(t, st.Synced)
		assert.Zero(t, st.Copied)
	}

	assert.Zero(t, peer.listCalls())

	_, err := replica.Read(ctx, partObjects(1)[2])
	require.ErrorIs(t, err, backend.ErrNotExist)
}

// TestSyncReconcileIsNotALicenceToDelete: the pass a loss triggers runs the ordinary prune, and the
// ordinary prune deletes nothing a peer did not say it removed. A part the owner silently dropped
// from its index — withheld on this replica ever since — survives the replica's own reconcile.
func TestSyncReconcileIsNotALicenceToDelete(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	owner, replica := backend.Memory(), backend.Memory()
	ix := writeParts(t, owner, reconcilePrefix, gen(1, 1), 1, 2, 3)
	peer := servePeer(t, owner, reconcilePrefix, probeOpts{})
	s := partsync.New(replica, &partsync.Client{})

	_, err := s.Sync(ctx, reconcilePrefix, []string{peer.addr}, false, nil)
	require.NoError(t, err)

	// The owner loses part 1 and rewrites its index without a tombstone; the replica adopts the
	// superseding index but keeps its good copy, withheld.
	dropPart(t, owner, reconcilePrefix, ix, 1, gen(1, 2), false)

	for range 3 {
		st, err := s.Sync(ctx, reconcilePrefix, []string{peer.addr}, false, nil)
		require.NoError(t, err)
		assert.Zero(t, st.Pruned)
	}

	requireHeld(t, replica, partObjects(1))

	// Now the replica itself loses part 3 under the unchanged index.
	deleteAll(t, replica, partObjects(3))

	for range 3 {
		st, err := s.Sync(ctx, reconcilePrefix, []string{peer.addr}, false, nil)
		require.NoError(t, err)
		assert.Zero(t, st.Pruned, "a reconcile deletes nothing the peer did not tombstone")
	}

	requireHeld(t, replica, partObjects(1))
	requireHeld(t, replica, partObjects(3))
}

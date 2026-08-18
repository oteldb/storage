package partsync_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/backend/bucketindex"
	"github.com/oteldb/storage/cluster/partsync"
)

// gen returns an index carrying a commit generation, as a v3 writer produces.
func gen(term, counter uint64) bucketindex.Generation {
	return bucketindex.Generation{Term: term, Counter: counter}
}

func writeParts(t *testing.T, be backend.Backend, prefix string, g bucketindex.Generation, seqs ...int) *bucketindex.Index {
	t.Helper()

	ix := &bucketindex.Index{Generation: g}
	for _, seq := range seqs {
		writePart(t, be, ix, prefix, seq, int64(seq)*100, int64(seq)*100+50)
	}
	saveIndex(t, be, prefix, ix)

	return ix
}

// dropPart removes a part's objects and rewrites the index without it, at generation g. It
// records a tombstone only when stated is true — which is the only difference between a removal
// the owner performed and a part the owner lost, and the whole subject of these tests.
func dropPart(t *testing.T, be backend.Backend, prefix string, ix *bucketindex.Index, seq int, g bucketindex.Generation, stated bool) *bucketindex.Index {
	t.Helper()
	ctx := context.Background()

	part := fmt.Sprintf("%s/000000000%d", prefix, seq)
	for _, suffix := range []string{"/c/0", "/marks", "/manifest"} {
		require.NoError(t, be.Delete(ctx, part+suffix))
	}

	out := &bucketindex.Index{Generation: g, Removed: ix.Removed}
	for _, e := range ix.Entries {
		if e.Prefix != part {
			out.Add(e)
		}
	}

	if stated {
		out.Tombstone(bucketindex.Removal{Prefix: part, Generation: g})
	}

	saveIndex(t, be, prefix, out)

	return out
}

// An index that shrank without a writer saying so is never obeyed, however many passes it is
// offered over. That is the #278 scenario exactly: the loss leaves the newest part in place, so
// the part names and the flush epoch both tie, and only the commit generation separates it from a
// deliberate removal.
func TestSyncNeverPrunesForANonSupersedingPeer(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	owner, replica := backend.Memory(), backend.Memory()
	ix := writeParts(t, owner, "t/metrics", gen(1, 5), 1, 2)
	addr := serve(t, owner)

	s := partsync.New(replica, &partsync.Client{})
	_, err := s.Sync(ctx, "t/metrics", []string{addr}, false, nil)
	require.NoError(t, err)
	_, err = replica.Read(ctx, "t/metrics/0000000001/manifest")
	require.NoError(t, err, "the replica mirrored both parts")

	// Bit rot, a partial rm, an index rolled back with a snapshot: part 1 is gone and the index no
	// longer names it, but nothing advanced the generation, because no writer wrote it.
	dropPart(t, owner, "t/metrics", ix, 1, gen(1, 5), false)

	// Well past pruneAfterMisses: a claim that cannot authorize a deletion does not become able
	// to by being repeated.
	for pass := range 5 {
		st, err := s.Sync(ctx, "t/metrics", []string{addr}, false, nil)
		require.NoError(t, err)
		assert.Zerof(t, st.Pruned, "pass %d pruned for a peer that does not supersede", pass)

		_, err = replica.Read(ctx, "t/metrics/0000000001/manifest")
		require.NoErrorf(t, err, "pass %d deleted the replica's good copy", pass)
	}

	// The replica also kept its own index, so the part goes on being named by it — which is what
	// makes the protection last rather than covering only the pass that noticed.
	got, err := bucketindex.Load(ctx, replica, "t/metrics/"+bucketindex.Object)
	require.NoError(t, err)
	assert.Len(t, got.Entries, 2, "a non-superseding index is not adopted")
}

// The same shrink, with the generation advanced, is a deliberate removal and is obeyed. The two
// tests differ in one field, which is the whole claim of the design.
func TestSyncPrunesForADeliberateShrink(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	owner, replica := backend.Memory(), backend.Memory()
	ix := writeParts(t, owner, "t/metrics", gen(1, 5), 1, 2)
	addr := serve(t, owner)

	s := partsync.New(replica, &partsync.Client{})
	_, err := s.Sync(ctx, "t/metrics", []string{addr}, false, nil)
	require.NoError(t, err)

	// Retention drops part 1 and produces nothing, so the part names and the flush epoch both tie
	// — the case that was indistinguishable before the generation. The writer says it removed
	// the part, which is what makes it a removal rather than a gap.
	ix = dropPart(t, owner, "t/metrics", ix, 1, gen(1, 6), true)

	for range 2 {
		_, err = s.Sync(ctx, "t/metrics", []string{addr}, false, nil)
		require.NoError(t, err)

		// Keep the index differing so each pass reaches prune, as the pre-existing prune tests do.
		ix.Generation = ix.Generation.Next(1)
		saveIndex(t, owner, "t/metrics", ix)
	}

	_, err = replica.Read(ctx, "t/metrics/0000000001/manifest")
	require.ErrorIs(t, err, backend.ErrNotExist, "a deliberate removal is still reclaimed")
}

// A peer restored from an older snapshot is behind, not merely different: its index is ignored
// outright rather than mirrored and obeyed.
func TestSyncIgnoresAStaleSnapshotPeer(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	owner, replica := backend.Memory(), backend.Memory()
	writeParts(t, owner, "t/metrics", gen(1, 9), 1, 2)
	addr := serve(t, owner)

	// The replica is already ahead — it mirrored a later state from the shard's current owner.
	local := writeParts(t, replica, "t/metrics", gen(2, 3), 1, 2, 3)

	s := partsync.New(replica, &partsync.Client{})
	st, err := s.Sync(ctx, "t/metrics", []string{addr}, false, nil)
	require.NoError(t, err)
	assert.Zero(t, st.Pruned)

	got, err := bucketindex.Load(ctx, replica, "t/metrics/"+bucketindex.Object)
	require.NoError(t, err)
	assert.Equal(t, local.Generation, got.Generation, "the behind peer's index was not installed")
	assert.Len(t, got.Entries, 3, "nor its part set")
}

// The case the commit generation alone could not close: a damaged owner reloads from what it has
// left and goes on flushing, so its later writes legitimately supersede and legitimately do not
// name the lost part. Nothing in the ordering can tell that from a compaction — only the absence
// of a tombstone can.
func TestSyncWithholdsWhenASupersedingPeerStatesNoRemoval(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	owner, replica := backend.Memory(), backend.Memory()
	ix := writeParts(t, owner, "t/metrics", gen(1, 5), 1, 2)
	addr := serve(t, owner)

	s := partsync.New(replica, &partsync.Client{})
	_, err := s.Sync(ctx, "t/metrics", []string{addr}, false, nil)
	require.NoError(t, err)

	// Part 1 is lost, and the owner keeps flushing afterwards. Every one of those writes
	// supersedes.
	ix = dropPart(t, owner, "t/metrics", ix, 1, gen(1, 6), false)

	var withheld int
	for seq := 3; seq <= 6; seq++ {
		writePart(t, owner, ix, "t/metrics", seq, int64(seq)*1000, int64(seq)*1000+100)
		ix.Generation = ix.Generation.Next(1)
		saveIndex(t, owner, "t/metrics", ix)

		for range 2 {
			st, err := s.Sync(ctx, "t/metrics", []string{addr}, false, nil)
			require.NoError(t, err)
			withheld += st.Withheld
		}

		_, err = replica.Read(ctx, "t/metrics/0000000001/manifest")
		require.NoErrorf(t, err, "the replica's good copy was deleted after flush %d", seq)
	}

	assert.Positive(t, withheld, "a peer missing data it should hold is reported, not obeyed")

	// The parts the owner did write are still mirrored: withholding a deletion does not stop
	// replication.
	_, err = replica.Read(ctx, "t/metrics/0000000006/manifest")
	require.NoError(t, err)
}

// A peer whose index predates the format states no removals, so there is nothing to withhold on
// and absence is all there is to go on — the pre-tombstone behavior, kept for the transition.
func TestSyncFallsBackToAbsenceForALegacyPeer(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	owner, replica := backend.Memory(), backend.Memory()

	// A legacy writer: no generation, and so no removals either.
	ix := &bucketindex.Index{}
	writePart(t, owner, ix, "t/metrics", 1, 100, 200)
	writePart(t, owner, ix, "t/metrics", 2, 300, 400)
	saveIndex(t, owner, "t/metrics", ix)
	addr := serve(t, owner)

	s := partsync.New(replica, &partsync.Client{})
	_, err := s.Sync(ctx, "t/metrics", []string{addr}, false, nil)
	require.NoError(t, err)

	// A merge, the old way: parts 1+2 replaced by part 3, ordered only by the part names.
	for _, k := range []string{
		"t/metrics/0000000001/c/0", "t/metrics/0000000001/marks", "t/metrics/0000000001/manifest",
		"t/metrics/0000000002/c/0", "t/metrics/0000000002/marks", "t/metrics/0000000002/manifest",
	} {
		require.NoError(t, owner.Delete(ctx, k))
	}

	merged := &bucketindex.Index{}
	writePart(t, owner, merged, "t/metrics", 3, 100, 400)
	saveIndex(t, owner, "t/metrics", merged)

	for seq := 4; seq <= 5; seq++ {
		_, err = s.Sync(ctx, "t/metrics", []string{addr}, false, nil)
		require.NoError(t, err)

		writePart(t, owner, merged, "t/metrics", seq, int64(seq)*1000, int64(seq)*1000+100)
		saveIndex(t, owner, "t/metrics", merged)
	}

	_, err = replica.Read(ctx, "t/metrics/0000000001/manifest")
	require.ErrorIs(t, err, backend.ErrNotExist, "a legacy peer is still reconciled by absence")
}

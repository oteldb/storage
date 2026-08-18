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

// dropPart removes a part's objects and rewrites the index without it, at generation g. With g
// unchanged from the previous index this is a *silent loss*; with g advanced it is a deliberate
// removal. The two are byte-identical apart from the generation, which is the point.
func dropPart(t *testing.T, be backend.Backend, prefix string, ix *bucketindex.Index, seq int, g bucketindex.Generation) *bucketindex.Index {
	t.Helper()
	ctx := context.Background()

	part := fmt.Sprintf("%s/000000000%d", prefix, seq)
	for _, suffix := range []string{"/c/0", "/marks", "/manifest"} {
		require.NoError(t, be.Delete(ctx, part+suffix))
	}

	out := &bucketindex.Index{Generation: g}
	for _, e := range ix.Entries {
		if e.Prefix != part {
			out.Add(e)
		}
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
	dropPart(t, owner, "t/metrics", ix, 1, gen(1, 5))

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
	// — the case that was indistinguishable before the generation.
	ix = dropPart(t, owner, "t/metrics", ix, 1, gen(1, 6))

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

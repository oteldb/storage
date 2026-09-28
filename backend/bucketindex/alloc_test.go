package bucketindex_test

import (
	"context"
	"slices"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/backend/bucketindex"
)

// TestBlockAllocationResolvesThroughCAS states the handoff argument as an interleaving rather than
// racing it: two owners read the same index, both allocate the same block number, and the index
// CAS admits one. The loser is told, re-reads, and allocates above the winner — so no two parts
// ever hold the same block identity, with no coordination beyond the commit that already exists.
func TestBlockAllocationResolvesThroughCAS(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	be := backend.Memory()
	const key = "default/metrics/" + bucketindex.Object

	base := &bucketindex.Index{}
	base.Add(bucketindex.Entry{Prefix: "p1", Blocks: bucketindex.Range(0, 1, 1)})
	_, err := base.Save(ctx, be, key, backend.VersionAbsent)
	require.NoError(t, err)

	old, oldVersion, err := bucketindex.LoadVersioned(ctx, be, key)
	require.NoError(t, err)
	newOwner, newVersion, err := bucketindex.LoadVersioned(ctx, be, key)
	require.NoError(t, err)

	assert.Equal(t, bucketindex.Block{N: 2}, old.NextBlock(0))
	assert.Equal(t, bucketindex.Block{N: 2}, newOwner.NextBlock(0), "both owners allocate the same block")

	old.Add(bucketindex.Entry{Prefix: "p2-old", Blocks: bucketindex.Range(0, 2, 2)})
	newOwner.Add(bucketindex.Entry{Prefix: "p2-new", Blocks: bucketindex.Range(0, 2, 2)})

	_, err = old.Save(ctx, be, key, oldVersion)
	require.NoError(t, err)

	_, err = newOwner.Save(ctx, be, key, newVersion)
	require.ErrorIs(t, err, bucketindex.ErrConflict, "the loser is told, and has committed nothing")

	// The retry: reload, re-allocate, commit. The winner's block stands.
	retry, retryVersion, err := bucketindex.LoadVersioned(ctx, be, key)
	require.NoError(t, err)
	assert.Equal(t, bucketindex.Block{N: 3}, retry.NextBlock(0), "allocation moves above the block that landed")

	retry.Add(bucketindex.Entry{Prefix: "p2-new", Blocks: bucketindex.Range(0, 3, 3)})
	_, err = retry.Save(ctx, be, key, retryVersion)
	require.NoError(t, err)

	got, err := bucketindex.Load(ctx, be, key)
	require.NoError(t, err)

	blocks := map[bucketindex.Block]string{}
	for _, e := range got.Entries {
		_, dup := blocks[e.Blocks.Min]
		require.Falsef(t, dup, "block %v claimed twice", e.Blocks.Min)
		blocks[e.Blocks.Min] = e.Prefix
	}

	assert.Equal(t, map[bucketindex.Block]string{{N: 1}: "p1", {N: 2}: "p2-old", {N: 3}: "p2-new"}, blocks)
}

// TestMergeAllocatesSpanningInterval walks the identity a merge produces: the successor covers
// every block its inputs did, one level up, so it supersedes each of them.
func TestMergeAllocatesSpanningInterval(t *testing.T) {
	t.Parallel()

	ix := &bucketindex.Index{}
	for i := range 3 {
		b := ix.NextBlock(0)
		ix.Add(bucketindex.Entry{Prefix: string(rune('a' + i)), Blocks: bucketindex.Single(b)})
	}
	require.Equal(t, bucketindex.Block{N: 4}, ix.NextBlock(0))

	inputs := slices.Clone(ix.Entries)
	merged := bucketindex.Entry{
		Prefix: "merged",
		Blocks: bucketindex.Interval{Min: inputs[0].Blocks.Min, Max: inputs[len(inputs)-1].Blocks.Max},
		Level:  1,
	}
	for _, in := range inputs {
		assert.Truef(t, merged.Supersedes(in), "merged part must supersede %q", in.Prefix)
	}

	for _, in := range inputs {
		require.True(t, ix.Remove(in.Prefix))
	}
	ix.Add(merged)

	assert.Equal(t, bucketindex.Block{N: 4}, ix.NextBlock(0), "a merge consumes blocks, it does not allocate new ones")
}

// TestTenuresAllocateDisjointBlocks is the block collision of #725: two tenures that never see each
// other's index — a displaced owner and its successor over divergent copies — both allocate the
// next block. Each allocates under its own term, so the identities cannot collide and no identity
// relation takes one part for the other.
func TestTenuresAllocateDisjointBlocks(t *testing.T) {
	t.Parallel()

	base := &bucketindex.Index{AllocatedBlocks: bucketindex.Block{Term: 7, N: 4}}
	base.Add(bucketindex.Entry{Prefix: "p4", Blocks: bucketindex.TermBlocks(7, 4), Term: 7})

	displaced, successor := base.NextBlock(7), base.NextBlock(9)
	assert.Equal(t, bucketindex.Block{Term: 7, N: 5}, displaced)
	assert.Equal(t, bucketindex.Block{Term: 9, N: 1}, successor, "a new tenure starts its own sequence")

	a := bucketindex.Entry{Prefix: "a", Blocks: bucketindex.Single(displaced), Term: 7}
	b := bucketindex.Entry{Prefix: "b", Blocks: bucketindex.Single(successor), Term: 9}
	assert.False(t, bucketindex.LineageOf(nil).Overlaps(a, b), "the two flushes share no identity")

	merged := bucketindex.Entry{Prefix: "m", Blocks: a.Blocks.Union(b.Blocks), Level: 1, Term: 9}
	assert.True(t, merged.Supersedes(a))
	assert.True(t, merged.Supersedes(b))
	assert.EqualValues(t, 2, merged.Blocks.Len(), "a merge across tenures holds each tenure's block")
}

// TestNextBlockBelowTheIndexTerm pins the writer with no cluster over an index a clustered tenure
// wrote: it continues that tenure's sequence rather than restarting term 0, whose earlier blocks the
// index may no longer name.
func TestNextBlockBelowTheIndexTerm(t *testing.T) {
	t.Parallel()

	ix := &bucketindex.Index{AllocatedBlocks: bucketindex.Block{Term: 5, N: 3}}
	assert.Equal(t, bucketindex.Block{Term: 5, N: 4}, ix.NextBlock(0))
	assert.Equal(t, bucketindex.Block{Term: 6, N: 1}, ix.NextBlock(6))

	ix.Add(bucketindex.Entry{Prefix: "p", Blocks: bucketindex.TermBlocks(5, 9)})
	assert.Equal(t, bucketindex.Block{Term: 5, N: 10}, ix.NextBlock(5), "live blocks above the mark still count")
}

// TestIdenticalIdentityResolvesToTheLaterTenure is two owners that merged the same inputs either
// side of a handoff: one identity, two parts, and the later tenure's supersedes the earlier's.
func TestIdenticalIdentityResolvesToTheLaterTenure(t *testing.T) {
	t.Parallel()

	blocks := bucketindex.TermBlocks(3, 1, 2)
	earlier := bucketindex.Entry{Prefix: "a", Blocks: blocks, Level: 1, Term: 3}
	later := bucketindex.Entry{Prefix: "b", Blocks: blocks, Level: 1, Term: 4}

	assert.True(t, later.Supersedes(earlier))
	assert.False(t, earlier.Supersedes(later))
	assert.False(t, later.Supersedes(later), "a part does not supersede itself")
	assert.Equal(t, map[string]struct{}{"a": {}},
		bucketindex.Subsumed([]bucketindex.Entry{earlier}, []bucketindex.Entry{later}))

	grouped := later
	grouped.Claim = bucketindex.Claim{Blocks: bucketindex.Blocks(1), Group: bucketindex.Blocks(2)}
	assert.False(t, grouped.Supersedes(earlier), "a different claim is a different identity")
}

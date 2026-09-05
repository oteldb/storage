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
	base.Add(bucketindex.Entry{Prefix: "p1", Blocks: bucketindex.Interval{Min: 1, Max: 1}})
	_, err := base.Save(ctx, be, key, backend.VersionAbsent)
	require.NoError(t, err)

	old, oldVersion, err := bucketindex.LoadVersioned(ctx, be, key)
	require.NoError(t, err)
	newOwner, newVersion, err := bucketindex.LoadVersioned(ctx, be, key)
	require.NoError(t, err)

	assert.EqualValues(t, 2, old.NextBlock())
	assert.EqualValues(t, 2, newOwner.NextBlock(), "both owners allocate the same block")

	old.Add(bucketindex.Entry{Prefix: "p2-old", Blocks: bucketindex.Interval{Min: 2, Max: 2}})
	newOwner.Add(bucketindex.Entry{Prefix: "p2-new", Blocks: bucketindex.Interval{Min: 2, Max: 2}})

	_, err = old.Save(ctx, be, key, oldVersion)
	require.NoError(t, err)

	_, err = newOwner.Save(ctx, be, key, newVersion)
	require.ErrorIs(t, err, bucketindex.ErrConflict, "the loser is told, and has committed nothing")

	// The retry: reload, re-allocate, commit. The winner's block stands.
	retry, retryVersion, err := bucketindex.LoadVersioned(ctx, be, key)
	require.NoError(t, err)
	assert.EqualValues(t, 3, retry.NextBlock(), "allocation moves above the block that landed")

	retry.Add(bucketindex.Entry{Prefix: "p2-new", Blocks: bucketindex.Interval{Min: 3, Max: 3}})
	_, err = retry.Save(ctx, be, key, retryVersion)
	require.NoError(t, err)

	got, err := bucketindex.Load(ctx, be, key)
	require.NoError(t, err)

	blocks := map[uint64]string{}
	for _, e := range got.Entries {
		_, dup := blocks[e.Blocks.Min]
		require.Falsef(t, dup, "block %d claimed twice", e.Blocks.Min)
		blocks[e.Blocks.Min] = e.Prefix
	}

	assert.Equal(t, map[uint64]string{1: "p1", 2: "p2-old", 3: "p2-new"}, blocks)
}

// TestMergeAllocatesSpanningInterval walks the identity a merge produces: the successor covers
// every block its inputs did, one level up, so it supersedes each of them.
func TestMergeAllocatesSpanningInterval(t *testing.T) {
	t.Parallel()

	ix := &bucketindex.Index{}
	for i := range 3 {
		n := ix.NextBlock()
		ix.Add(bucketindex.Entry{
			Prefix: string(rune('a' + i)),
			Blocks: bucketindex.Interval{Min: n, Max: n},
		})
	}
	require.EqualValues(t, 4, ix.NextBlock())

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

	assert.EqualValues(t, 4, ix.NextBlock(), "a merge consumes blocks, it does not allocate new ones")
}

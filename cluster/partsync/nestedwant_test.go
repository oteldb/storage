package partsync_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/backend/bucketindex"
	"github.com/oteldb/storage/cluster/partsync"
)

// TestFetchWantNestedGroupFromOuterSuccessor is the nested false hole of #725. This node split blocks
// {1,2} into the outer group {10..12}, then split member 12 with block 7 into the inner group
// {20..22}, and lost member 20. Every outer member has since left its index, so the want for 20
// carries only the inner claim. The peer diverged before either split: the only surviving copy is its
// higher-level successor of {1,2,7} — which holds 12's rows through the outer ancestry, and so 20's.
//
// Nothing the want or the peer's index carries relates 20 to {1,2}: only this node's lineage catalog
// does. Without it the peer's answer is definitive absence, and three of those commit a hole over
// rows the peer still has.
func TestFetchWantNestedGroupFromOuterSuccessor(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	const prefix = "default/logs"

	peer, damaged := backend.Memory(), backend.Memory()

	outer := bucketindex.Claim{Blocks: bucketindex.Blocks(1, 2), Group: bucketindex.Blocks(10, 11, 12)}
	inner := bucketindex.Claim{Blocks: bucketindex.Blocks(7, 12), Group: bucketindex.Blocks(20, 21, 22)}

	member := func(ix *bucketindex.Index, name string, block uint64, claim bucketindex.Claim, level uint32) {
		ix.Add(bucketindex.Entry{
			Prefix: prefix + "/" + name, MinTime: 1, MaxTime: 2,
			Blocks: bucketindex.Blocks(block), Claim: claim, Level: level,
		})
	}

	local := &bucketindex.Index{}
	for i, b := range []uint64{10, 11, 12} {
		member(local, "outer"+string(rune('a'+i)), b, outer, 1)
	}

	local.RecordLineage()

	for _, name := range []string{"outera", "outerb", "outerc"} {
		require.True(t, local.Remove(prefix+"/"+name))
	}

	for i, b := range []uint64{20, 21, 22} {
		member(local, "inner"+string(rune('a'+i)), b, inner, 2)
	}

	local.RecordLineage()

	lost := local.Entries[0]
	require.True(t, local.Remove(lost.Prefix))
	local.RecordWant(bucketindex.WantOf(lost, bucketindex.Generation{}))
	saveIndex(t, damaged, prefix, local)

	pix := &bucketindex.Index{}
	successor := writeBlockPart(t, peer, pix, prefix, "succ", bucketindex.Blocks(1, 2, 7), 3)
	saveIndex(t, peer, prefix, pix)

	s := partsync.New(damaged, &partsync.Client{})

	ent, ok, err := s.FetchWant(ctx, prefix, []string{serve(t, peer)}, local.Wanted[0])
	require.NoError(t, err)
	require.True(t, ok, "the peer's successor holds the lost member's rows through the outer ancestry")
	assert.Equal(t, successor, ent.Prefix)
}

package enginetest

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/backend/bucketindex"
)

// nestedWantRepairedThroughTheCatalog is the repair half of the nested false hole. This node's index
// owes an inner split member that carries only the inner claim, and relates it to its outer ancestry
// through the catalog alone: the outer group is retired. The peer's only copy of those rows is a
// higher-level successor of the outer ancestry, which the fetcher returns. The repair pass must
// judge that successor with the catalog, admit it, and discharge the want — not drop the unit and
// leave the want to earn a hole.
func nestedWantRepairedThroughTheCatalog(t *testing.T, k Kind) {
	t.Helper()

	ctx := context.Background()
	be, peer := backend.Memory(), backend.Memory()

	outer := bucketindex.Claim{Blocks: bucketindex.Blocks(101, 102), Group: bucketindex.Blocks(110, 111, 112)}
	inner := bucketindex.Claim{Blocks: bucketindex.Blocks(107, 112), Group: bucketindex.Blocks(120, 121, 122)}
	lostRow := api(100, 1)

	// The lost member's rows, flushed here and then taken out of the index into a want.
	w := k.open(t, be)
	lost := k.flushEach(t, w, be, lostRow)[0]
	lostPrefix := k.Prefix + "/" + lost

	key := k.indexKey()
	ix, version, err := bucketindex.LoadVersioned(ctx, be, key)
	require.NoError(t, err)

	ent := ix.Entries[0]
	require.True(t, ix.Remove(lostPrefix))
	ent.Blocks, ent.Claim, ent.Level = bucketindex.Blocks(120), inner, 2
	ix.RecordWant(bucketindex.WantOf(ent, ix.Generation))
	ix.Catalog = []bucketindex.Claim{outer}
	_, err = ix.Save(ctx, be, key, version)
	require.NoError(t, err)
	dropObjects(t, be, lostPrefix+"/")

	// The peer's successor holds the same rows under a part of its own.
	p := k.open(t, peer)
	succ := k.Prefix + "/" + k.flushEach(t, p, peer, lostRow)[0]

	fetcher := NewFetcher(func(bucketindex.Want) (bucketindex.Entry, bucketindex.WantOutcome, error) {
		CopyObjects(t, peer, be, succ+"/")

		return bucketindex.Entry{
			Prefix: succ, MinTime: lostRow.Ts, MaxTime: lostRow.Ts,
			Blocks: bucketindex.Blocks(101, 102, 107), Level: 3,
		}, bucketindex.WantSatisfied, nil
	})

	r := k.openRepair(t, be, fetcher)
	require.NoError(t, r.LoadParts(ctx))
	require.Equal(t, []string{lostPrefix}, wantPrefixes(k.loadIndex(t, be).Wanted))

	require.NoError(t, r.ForceMerge(ctx))

	got := k.loadIndex(t, be)
	assert.Empty(t, got.Wanted, "the successor discharges the want through the outer claim")
	assert.Zero(t, got.LostParts)
	assert.Positive(t, r.RepairStats().Fetched)
	assert.Equal(t, []Row{lostRow}, sortedRows(t, r))
}

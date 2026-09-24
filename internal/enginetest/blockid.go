package enginetest

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/backend/bucketindex"
	"github.com/oteldb/storage/backend/faultbackend"
)

// blocksByPrefix maps each entry's prefix to the block interval and level it carries.
func blocksByPrefix(ix *bucketindex.Index) map[string]bucketindex.Entry {
	out := make(map[string]bucketindex.Entry, len(ix.Entries))
	for i := range ix.Entries {
		out[ix.Entries[i].Prefix] = ix.Entries[i]
	}

	return out
}

// flushIDs flushes n one-row parts and returns their ids in flush order.
func (k Kind) flushIDs(t *testing.T, e Engine, be backend.Backend, n int) []string {
	t.Helper()

	rs := make([]Row, n)
	for i := range rs {
		rs[i] = api(int64(100*(i+1)), int64(i+1))
	}

	return k.flushEach(t, e, be, rs...)
}

// stripBlocks rewrites the committed index with the block identity of the named entries removed,
// standing in for parts written before format v5, the only way to get one, since every commit this
// build makes assigns an identity.
func (k Kind) stripBlocks(ctx context.Context, t *testing.T, be backend.Backend, ids ...string) {
	t.Helper()

	ix, version, err := bucketindex.LoadVersioned(ctx, be, k.indexKey())
	require.NoError(t, err)

	for i := range ix.Entries {
		for _, id := range ids {
			if strings.HasSuffix(ix.Entries[i].Prefix, "/"+id) {
				ix.Entries[i].Blocks, ix.Entries[i].Level = bucketindex.Interval{}, 0
			}
		}
	}

	// An index that predates block identity predates the allocation high-water mark too, so the
	// simulation has to drop it as well or the migrated part numbers above a mark v5 never wrote.
	ix.AllocatedBlocks = 0

	_, err = ix.Save(ctx, be, k.indexKey(), version)
	require.NoError(t, err)
}

// flushAllocatesABlock is the base case: every flushed part commits [n, n] at level 0, with n taken
// from the index the commit builds, so identity is dense and monotone with no coordination.
func flushAllocatesABlock(t *testing.T, k Kind) {
	t.Helper()

	be := backend.Memory()
	ids := k.flushIDs(t, k.open(t, be), be, 3)

	got := blocksByPrefix(k.loadIndex(t, be))
	for i, id := range ids {
		ent := got[k.Prefix+"/"+id]
		assert.Equal(t, bucketindex.Interval{Min: uint64(i + 1), Max: uint64(i + 1)}, ent.Blocks)
		assert.Zero(t, ent.Level)
	}
}

// mergeUnionsItsInputs: a merge claims the blocks its inputs covered rather than allocating, which is
// exactly what makes the output supersede them.
func mergeUnionsItsInputs(t *testing.T, k Kind) {
	t.Helper()

	ctx := context.Background()
	be := backend.Memory()
	e := k.open(t, be)
	ids := k.flushIDs(t, e, be, 3)
	before := blocksByPrefix(k.loadIndex(t, be))

	require.NoError(t, e.ForceMerge(ctx))

	ix := k.loadIndex(t, be)
	require.Len(t, ix.Entries, 1, "the forced merge collapses the part set")

	out := ix.Entries[0]
	assert.Equal(t, bucketindex.Interval{Min: 1, Max: 3}, out.Blocks)
	assert.Equal(t, uint32(1), out.Level)

	for _, id := range ids {
		assert.True(t, out.Supersedes(before[k.Prefix+"/"+id]), "the merged part must supersede every input it consumed")
	}
}

// mergedNeighboursDoNotDischargeAWant is the acceptance test for block allocation: no entry in it is
// built by hand. Three parts are flushed and take real intervals, the middle one is lost so the
// index owes a repair for it, and the surviving two are merged.
//
// The merge holds not one row of the lost part, so it must not end the obligation. Its identity is
// the union of what it consumed, {1} and {3}, and that set does not contain {2}. A hull got this
// wrong: [1,3] contained the lost block, the want was discharged by a part built from neither, and
// the loss became invisible with no hole and no counter moving.
func mergedNeighboursDoNotDischargeAWant(t *testing.T, k Kind) {
	t.Helper()

	ctx := context.Background()
	be := backend.Memory()
	ids := k.flushIDs(t, k.open(t, be), be, 3)

	k.erasePart(ctx, t, be, ids[1])

	r := k.open(t, be)
	require.NoError(t, r.LoadParts(ctx))

	ix := k.loadIndex(t, be)
	require.Equal(t, []string{k.Prefix + "/" + ids[1]}, wantPrefixes(ix.Wanted))

	lost := ix.Wanted[0]
	require.True(t, lost.Blocks.Valid(), "the want carries the identity the flush allocated")
	require.Equal(t, bucketindex.Interval{Min: 2, Max: 2}, lost.Blocks)

	require.NoError(t, r.ForceMerge(ctx))

	ix = k.loadIndex(t, be)
	require.Len(t, ix.Entries, 1)

	merged := ix.Entries[0]
	assert.Equal(t, []string{k.Prefix + "/" + ids[1]}, wantPrefixes(ix.Wanted),
		"the merged part %+v holds none of the lost rows, so the want must stand", merged)
	assert.False(t, merged.Supersedes(lost.Entry()), "and it claims none of the lost part's blocks")
	assert.Empty(t, ix.Holes(), "the loss is not acknowledged either: the want is still repairable")
	assert.Zero(t, ix.LostParts)
	assert.Zero(t, r.RepairStats().Unsatisfiable, "the want never had to reach a peer")

	_, ok := ix.Satisfying(lost)
	assert.False(t, ok, "and nothing in the index answers for it")
}

// blocksSurviveRestart pins that identity is durable: it is read back from the committed index on
// open, so a restart neither re-allocates nor forgets, and the next flush numbers above it.
func blocksSurviveRestart(t *testing.T, k Kind) {
	t.Helper()

	ctx := context.Background()
	be := backend.Memory()
	ids := k.flushIDs(t, k.open(t, be), be, 2)
	before := blocksByPrefix(k.loadIndex(t, be))

	r := k.open(t, be)
	require.NoError(t, r.LoadParts(ctx))
	r.Append(t, api(900, 9))
	require.NoError(t, r.Flush(ctx))

	after := blocksByPrefix(k.loadIndex(t, be))
	for _, id := range ids {
		assert.Equal(t, before[k.Prefix+"/"+id].Blocks, after[k.Prefix+"/"+id].Blocks, "a restart must not renumber a part")
	}

	fresh := k.partDirs(ctx, t, be)
	require.Len(t, fresh, 3)
	assert.Equal(t, bucketindex.Interval{Min: 3, Max: 3}, after[k.Prefix+"/"+fresh[2]].Blocks,
		"the new owner allocates above what it inherited")
}

// rebaseAllocatesAboveTheRival states the interleaving instead of racing it: a flush is suspended
// inside its conditional index write, a rival commits a part claiming block 1 over the same prefix,
// and the flush is released to lose, rebase and retry.
//
// Both halves of the per-attempt discipline turn on it. Allocating over the engine's own parts alone,
// ignoring the rival's entries the rebase adopted, hands the retry block 1 again; writing the first
// attempt's assignment onto the part before its CAS lands leaves the retry nothing to re-allocate.
// Either way two different parts claim one identity.
func rebaseAllocatesAboveTheRival(t *testing.T, k Kind) {
	t.Helper()

	ctx := context.Background()
	rival := k.Prefix + "/0000009999"

	inner := backend.Memory()
	be := faultbackend.Wrap(inner)
	gate := faultbackend.NewGate()
	be.Add(gate.Rule(faultbackend.CompareAndSwap, func(op faultbackend.Op) bool {
		return strings.HasSuffix(op.Key, "/"+bucketindex.Object)
	}))

	e := k.open(t, be)
	e.Append(t, api(100, 1))

	flushed := make(chan error, 1)
	go func() { flushed <- e.Flush(ctx) }()

	gate.Await(t)

	other := &bucketindex.Index{Generation: bucketindex.Generation{Term: 1, Counter: 1}}
	other.Add(bucketindex.Entry{
		Prefix: rival, MinTime: 1, MaxTime: 2,
		Blocks: bucketindex.Interval{Min: 1, Max: 1},
	})
	_, err := other.Save(ctx, inner, k.indexKey(), backend.VersionAbsent)
	require.NoError(t, err)

	gate.Release()
	require.NoError(t, <-flushed)

	ix, err := bucketindex.Load(ctx, inner, k.indexKey())
	require.NoError(t, err)
	require.Len(t, ix.Entries, 2)

	got := blocksByPrefix(ix)
	require.Equal(t, bucketindex.Interval{Min: 1, Max: 1}, got[rival].Blocks, "the winner keeps the block it claimed")

	mine := k.partDirs(ctx, t, inner)
	require.Len(t, mine, 1)
	assert.Equal(t, bucketindex.Interval{Min: 2, Max: 2}, got[k.Prefix+"/"+mine[0]].Blocks, "the loser re-allocates above the winner")

	for a := range got {
		for b := range got {
			if a != b {
				assert.NotEqual(t, got[a].Blocks, got[b].Blocks, "two parts share a block identity")
			}
		}
	}
}

// outstandingWantHoldsItsBlock guards the reservation NextBlock makes for a want: the part it names
// may still be repaired back into the index, so its blocks stay claimed though no entry carries them.
func outstandingWantHoldsItsBlock(t *testing.T, k Kind) {
	t.Helper()

	ctx := context.Background()
	be := backend.Memory()
	ids := k.flushIDs(t, k.open(t, be), be, 2)

	k.erasePart(ctx, t, be, ids[1])

	r := k.open(t, be)
	require.NoError(t, r.LoadParts(ctx))
	require.Len(t, k.loadIndex(t, be).Wanted, 1)

	r.Append(t, api(900, 9))
	require.NoError(t, r.Flush(ctx))

	ix := k.loadIndex(t, be)
	require.Len(t, ix.Wanted, 1)

	fresh := k.partDirs(ctx, t, be)
	assert.Equal(t, bucketindex.Interval{Min: 3, Max: 3}, blocksByPrefix(ix)[k.Prefix+"/"+fresh[len(fresh)-1]].Blocks,
		"the wanted part's block 2 is not handed out again")
}

// preV5PartsMigrateOnMerge: a merge is the only thing that rewrites an old part, so a merge whose
// inputs carry no interval allocates a fresh one for its output rather than committing another unset
// identity.
func preV5PartsMigrateOnMerge(t *testing.T, k Kind) {
	t.Helper()

	ctx := context.Background()
	be := backend.Memory()
	ids := k.flushIDs(t, k.open(t, be), be, 2)
	k.stripBlocks(ctx, t, be, ids...)

	r := k.open(t, be)
	require.NoError(t, r.LoadParts(ctx))
	require.NoError(t, r.ForceMerge(ctx))

	ix := k.loadIndex(t, be)
	require.Len(t, ix.Entries, 1)
	assert.Equal(t, bucketindex.Interval{Min: 1, Max: 1}, ix.Entries[0].Blocks, "the migrated part takes the first free block")
	assert.Equal(t, uint32(1), ix.Entries[0].Level)
}

// mixedMergeInheritsTheKnownInputs pins the choice for a merge with only some pre-v5 inputs: the
// output claims the union of the intervals that exist and allocates nothing. A want naming a pre-v5
// part records that part's unset interval, so no claim the output could make would contain it, and
// claiming a fresh block on top would name blocks the output does not cover.
func mixedMergeInheritsTheKnownInputs(t *testing.T, k Kind) {
	t.Helper()

	ctx := context.Background()
	be := backend.Memory()
	ids := k.flushIDs(t, k.open(t, be), be, 3)
	k.stripBlocks(ctx, t, be, ids[0])

	r := k.open(t, be)
	require.NoError(t, r.LoadParts(ctx))
	require.NoError(t, r.ForceMerge(ctx))

	ix := k.loadIndex(t, be)
	require.Len(t, ix.Entries, 1)
	assert.Equal(t, bucketindex.Interval{Min: 2, Max: 3}, ix.Entries[0].Blocks)
	assert.Equal(t, uint32(1), ix.Entries[0].Level)
}

// blockNumbersSurviveAnEmptiedShard pins that numbering never rewinds. Retention drops every part in
// the shard, leaving tombstones that carry no blocks, and the next flush must still number above
// what the shard has ever held.
//
// Identity has to be unique over a shard's whole life, not its current contents: numbering derived
// from the live set hands a new part the identity an expired one had, at which point a stale peer's
// old part satisfies a want for the new one and expired data is committed as a repair.
func blockNumbersSurviveAnEmptiedShard(t *testing.T, k Kind) {
	t.Helper()

	ctx := context.Background()
	be := backend.Memory()
	e := k.open(t, be)

	k.flushIDs(t, e, be, 2)

	// Retention horizon past every row: both parts are dropped whole (tombstoned).
	require.NoError(t, e.Merge(ctx, 1<<40))

	ix := k.loadIndex(t, be)
	require.Empty(t, ix.Entries)
	require.EqualValues(t, 3, ix.NextBlock(), "the high-water mark outlives every part it numbered")

	e.Append(t, api(1<<41, 1))
	require.NoError(t, e.Flush(ctx))

	ix = k.loadIndex(t, be)
	require.Len(t, ix.Entries, 1)
	require.Equal(t, bucketindex.Interval{Min: 3, Max: 3}, ix.Entries[0].Blocks, "block numbers are never reused within a shard")
}

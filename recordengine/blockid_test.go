package recordengine_test

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/backend/bucketindex"
	"github.com/oteldb/storage/backend/faultbackend"
	"github.com/oteldb/storage/recordengine"
)

// blocksByPrefix maps each entry's prefix to the block interval and level it carries.
func blocksByPrefix(ix *bucketindex.Index) map[string]bucketindex.Entry {
	out := make(map[string]bucketindex.Entry, len(ix.Entries))
	for _, e := range ix.Entries {
		out[e.Prefix] = e
	}

	return out
}

// flushIDs flushes n one-record parts and returns their ids in flush order.
func flushIDs(ctx context.Context, t *testing.T, e *recordengine.Engine, be backend.Backend, n int) []string {
	t.Helper()

	for i := range n {
		ingest(t, e, mkBatch("api", rrec{ts: int64(100 * (i + 1)), body: "p" + string(rune('1'+i))}))
		require.NoError(t, e.Flush(ctx))
	}

	ids := diskPartIDs(ctx, t, be)
	require.Len(t, ids, n)

	return ids
}

// stripBlocks rewrites the committed index with the block identity of the named entries removed,
// standing in for parts written before format v5 — the only way to get one, since every commit
// this build makes assigns an identity.
func stripBlocks(ctx context.Context, t *testing.T, be backend.Backend, ids ...string) {
	t.Helper()

	ix, version, err := bucketindex.LoadVersioned(ctx, be, indexKey())
	require.NoError(t, err)

	for i := range ix.Entries {
		for _, id := range ids {
			if strings.HasSuffix(ix.Entries[i].Prefix, "/"+id) {
				ix.Entries[i].Blocks, ix.Entries[i].Level = bucketindex.Interval{}, 0
			}
		}
	}

	_, err = ix.Save(ctx, be, indexKey(), version)
	require.NoError(t, err)
}

// TestFlushAllocatesABlock is the base case: every flushed part commits [n, n] at level 0, with n
// taken from the index the commit builds, so identity is dense and monotone with no coordination.
func TestFlushAllocatesABlock(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	be := backend.Memory()
	e := newEngine(t, be)
	ids := flushIDs(ctx, t, e, be, 3)

	got := blocksByPrefix(loadIndex(t, be))
	for i, id := range ids {
		ent := got[enginePrefix+"/"+id]
		assert.Equal(t, bucketindex.Interval{Min: uint64(i + 1), Max: uint64(i + 1)}, ent.Blocks)
		assert.Zero(t, ent.Level)
	}
}

// TestMergeUnionsItsInputs is the other half of Decision 2: a merge claims the blocks its inputs
// covered rather than allocating, which is exactly what makes the output supersede them.
func TestMergeUnionsItsInputs(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	be := backend.Memory()
	e := newEngine(t, be)

	ids := flushIDs(ctx, t, e, be, 3)
	before := blocksByPrefix(loadIndex(t, be))

	require.NoError(t, e.MergeWith(ctx, recordengine.MergeOptions{Force: true}))

	ix := loadIndex(t, be)
	require.Len(t, ix.Entries, 1, "the forced merge collapses the part set")

	out := ix.Entries[0]
	assert.Equal(t, bucketindex.Interval{Min: 1, Max: 3}, out.Blocks)
	assert.Equal(t, uint32(1), out.Level)

	for _, id := range ids {
		assert.True(t, out.Supersedes(before[enginePrefix+"/"+id]),
			"the merged part must supersede every input it consumed")
	}
}

// TestWantDischargedByAMergedSuccessor is the acceptance test for block allocation: no entry in it
// is built by hand. Three parts are flushed and take real intervals, one is lost so the index owes
// a repair for it, and the surviving two are merged into a part whose interval contains the lost
// one's. That containment — and nothing else — is what ends the obligation.
func TestWantDischargedByAMergedSuccessor(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	be := backend.Memory()
	e := newEngine(t, be)
	ids := flushIDs(ctx, t, e, be, 3)

	erasePart(ctx, t, be, ids[1])

	r := newEngine(t, be)
	require.NoError(t, r.LoadParts(ctx))

	ix := loadIndex(t, be)
	require.Equal(t, []string{enginePrefix + "/" + ids[1]}, wantPrefixes(ix.Wanted))

	lost := ix.Wanted[0]
	require.True(t, lost.Blocks.Valid(), "the want carries the identity the flush allocated")
	require.Equal(t, bucketindex.Interval{Min: 2, Max: 2}, lost.Blocks)

	require.NoError(t, r.MergeWith(ctx, recordengine.MergeOptions{Force: true}))

	ix = loadIndex(t, be)
	require.Empty(t, ix.Wanted, "a successor containing the lost blocks discharges the want")
	require.Len(t, ix.Entries, 1)

	succ := ix.Entries[0]
	assert.True(t, succ.Data(), "the want is repaired, not acknowledged as a hole")
	assert.True(t, succ.Supersedes(lost.Entry()))
	assert.Zero(t, ix.LostParts)
	assert.Zero(t, r.RepairStats().Unsatisfiable, "the want never had to reach a peer")

	_, ok := ix.Satisfying(lost)
	assert.True(t, ok, "and the successor is what Satisfying picks for it")
}

// TestBlocksSurviveRestart pins that identity is durable: it is read back from the committed index
// on open, so a restart neither re-allocates nor forgets, and the next flush numbers above it.
func TestBlocksSurviveRestart(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	be := backend.Memory()
	e := newEngine(t, be)
	ids := flushIDs(ctx, t, e, be, 2)
	before := blocksByPrefix(loadIndex(t, be))

	r := newEngine(t, be)
	require.NoError(t, r.LoadParts(ctx))

	ingest(t, r, mkBatch("api", rrec{ts: 900, body: "p3"}))
	require.NoError(t, r.Flush(ctx))

	after := blocksByPrefix(loadIndex(t, be))
	for _, id := range ids {
		assert.Equal(t, before[enginePrefix+"/"+id].Blocks, after[enginePrefix+"/"+id].Blocks,
			"a restart must not renumber a part")
	}

	fresh := diskPartIDs(ctx, t, be)
	require.Len(t, fresh, 3)
	assert.Equal(t, bucketindex.Interval{Min: 3, Max: 3}, after[enginePrefix+"/"+fresh[2]].Blocks,
		"the new owner allocates above what it inherited")
}

// TestRebaseAllocatesAboveTheRival states the interleaving instead of racing it: a flush is
// suspended inside its conditional index write, a rival commits a part claiming block 1 over the
// same prefix, and the flush is released to lose, rebase and retry.
//
// It is the test both halves of the per-attempt discipline turn on. Allocating over e.parts alone
// — ignoring the rival's entries the rebase adopted — hands the retry block 1 again; writing the
// first attempt's assignment onto the part before its CAS lands leaves the retry with nothing to
// re-allocate. Either way two different parts claim one identity.
func TestRebaseAllocatesAboveTheRival(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	const rival = enginePrefix + "/0000009999"

	inner := backend.Memory()
	be := faultbackend.Wrap(inner)
	gate := faultbackend.NewGate()
	be.Add(gate.Rule(faultbackend.CompareAndSwap, func(op faultbackend.Op) bool {
		return strings.HasSuffix(op.Key, "/"+bucketindex.Object)
	}))

	e := newEngine(t, be)
	ingest(t, e, mkBatch("api", rrec{ts: 100, body: "p1"}))

	flushed := make(chan error, 1)
	go func() { flushed <- e.Flush(ctx) }()

	gate.Await(t)

	other := &bucketindex.Index{Generation: bucketindex.Generation{Term: 1, Counter: 1}}
	other.Add(bucketindex.Entry{
		Prefix: rival, MinTime: 1, MaxTime: 2,
		Blocks: bucketindex.Interval{Min: 1, Max: 1},
	})
	_, err := other.Save(ctx, inner, indexKey(), backend.VersionAbsent)
	require.NoError(t, err)

	gate.Release()
	require.NoError(t, <-flushed)

	ix, err := bucketindex.Load(ctx, inner, indexKey())
	require.NoError(t, err)
	require.Len(t, ix.Entries, 2)

	got := blocksByPrefix(ix)
	require.Equal(t, bucketindex.Interval{Min: 1, Max: 1}, got[rival].Blocks,
		"the winner keeps the block it claimed")

	mine := diskPartIDs(ctx, t, inner)
	require.Len(t, mine, 1)
	assert.Equal(t, bucketindex.Interval{Min: 2, Max: 2}, got[enginePrefix+"/"+mine[0]].Blocks,
		"the loser re-allocates above the winner")

	for a := range got {
		for b := range got {
			if a != b {
				assert.NotEqual(t, got[a].Blocks, got[b].Blocks, "two parts share a block identity")
			}
		}
	}
}

// TestOutstandingWantHoldsItsBlock guards the reservation NextBlock makes for a want: the part it
// names may still be repaired back into the index, so its blocks stay claimed even though no entry
// carries them.
func TestOutstandingWantHoldsItsBlock(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	be := backend.Memory()
	e := newEngine(t, be)
	ids := flushIDs(ctx, t, e, be, 2)

	erasePart(ctx, t, be, ids[1])

	r := newEngine(t, be)
	require.NoError(t, r.LoadParts(ctx))
	require.Len(t, loadIndex(t, be).Wanted, 1)

	ingest(t, r, mkBatch("api", rrec{ts: 900, body: "p3"}))
	require.NoError(t, r.Flush(ctx))

	ix := loadIndex(t, be)
	require.Len(t, ix.Wanted, 1)

	fresh := diskPartIDs(ctx, t, be)
	got := blocksByPrefix(ix)
	assert.Equal(t, bucketindex.Interval{Min: 3, Max: 3}, got[enginePrefix+"/"+fresh[len(fresh)-1]].Blocks,
		"the wanted part's block 2 is not handed out again")
}

// TestPreV5PartsMigrateOnMerge is the migration path Decision 2 relies on: a merge is the only
// thing that rewrites an old part, so a merge whose inputs carry no interval allocates a fresh one
// for its output rather than committing another unset identity.
func TestPreV5PartsMigrateOnMerge(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	be := backend.Memory()
	e := newEngine(t, be)
	ids := flushIDs(ctx, t, e, be, 2)
	stripBlocks(ctx, t, be, ids...)

	r := newEngine(t, be)
	require.NoError(t, r.LoadParts(ctx))
	require.NoError(t, r.MergeWith(ctx, recordengine.MergeOptions{Force: true}))

	ix := loadIndex(t, be)
	require.Len(t, ix.Entries, 1)
	assert.Equal(t, bucketindex.Interval{Min: 1, Max: 1}, ix.Entries[0].Blocks,
		"the migrated part takes the first free block")
	assert.Equal(t, uint32(1), ix.Entries[0].Level)
}

// TestMixedMergeInheritsTheKnownInputs pins the documented choice for a merge with only some
// pre-v5 inputs: the output claims the union of the intervals that exist and allocates nothing.
// It cannot do better — a want naming a pre-v5 part records that part's unset interval, so no
// claim the output could make would contain it — and claiming a fresh block on top would name
// blocks the output does not cover.
func TestMixedMergeInheritsTheKnownInputs(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	be := backend.Memory()
	e := newEngine(t, be)
	ids := flushIDs(ctx, t, e, be, 3)
	stripBlocks(ctx, t, be, ids[0])

	r := newEngine(t, be)
	require.NoError(t, r.LoadParts(ctx))
	require.NoError(t, r.MergeWith(ctx, recordengine.MergeOptions{Force: true}))

	ix := loadIndex(t, be)
	require.Len(t, ix.Entries, 1)
	assert.Equal(t, bucketindex.Interval{Min: 2, Max: 3}, ix.Entries[0].Blocks)
	assert.Equal(t, uint32(1), ix.Entries[0].Level)
}

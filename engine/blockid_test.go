package engine_test

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/backend/bucketindex"
	"github.com/oteldb/storage/backend/faultbackend"
	"github.com/oteldb/storage/engine"
	"github.com/oteldb/storage/signal"
)

// blocksByPrefix maps each entry's prefix to the block interval and level it carries.
func blocksByPrefix(ix *bucketindex.Index) map[string]bucketindex.Entry {
	out := make(map[string]bucketindex.Entry, len(ix.Entries))
	for _, e := range ix.Entries {
		out[e.Prefix] = e
	}

	return out
}

// flushIDs flushes n one-sample parts and returns their ids in flush order.
func flushIDs(ctx context.Context, t *testing.T, e *engine.Engine, be backend.Backend, s signal.Series, n int) []string {
	t.Helper()

	for i := range n {
		mustAppend(t, e, s, int64(100*(i+1)), float64(i+1))
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

	ix, version, err := bucketindex.LoadVersioned(ctx, be, lostIndexKey())
	require.NoError(t, err)

	for i := range ix.Entries {
		for _, id := range ids {
			if strings.HasSuffix(ix.Entries[i].Prefix, "/"+id) {
				ix.Entries[i].Blocks, ix.Entries[i].Level = bucketindex.Interval{}, 0
			}
		}
	}

	_, err = ix.Save(ctx, be, lostIndexKey(), version)
	require.NoError(t, err)
}

// TestFlushAllocatesABlock is the base case: every flushed part commits [n, n] at level 0, with n
// taken from the index the commit builds, so identity is dense and monotone with no coordination.
func TestFlushAllocatesABlock(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	be := backend.Memory()
	e := newLostEngine(be)
	ids := flushIDs(ctx, t, e, be, mkSeries("job", "api"), 3)

	got := blocksByPrefix(loadIndex(t, be, lostPrefix))
	for i, id := range ids {
		ent := got[lostPrefix+"/"+id]
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
	e := newLostEngine(be)
	ids := flushIDs(ctx, t, e, be, mkSeries("job", "api"), 3)
	before := blocksByPrefix(loadIndex(t, be, lostPrefix))

	require.NoError(t, e.MergeWith(ctx, engine.MergeOptions{Force: true}))

	ix := loadIndex(t, be, lostPrefix)
	require.Len(t, ix.Entries, 1, "the forced merge collapses the part set")

	out := ix.Entries[0]
	assert.Equal(t, bucketindex.Interval{Min: 1, Max: 3}, out.Blocks)
	assert.Equal(t, uint32(1), out.Level)

	for _, id := range ids {
		assert.True(t, out.Supersedes(before[lostPrefix+"/"+id]),
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
	e := newLostEngine(be)
	ids := flushIDs(ctx, t, e, be, mkSeries("job", "api"), 3)

	erasePart(ctx, t, be, ids[1])

	r := newLostEngine(be)
	require.NoError(t, r.LoadParts(ctx))

	ix := loadIndex(t, be, lostPrefix)
	require.Equal(t, []string{lostPrefix + "/" + ids[1]}, wantPrefixes(ix.Wanted))

	lost := ix.Wanted[0]
	require.Equal(t, bucketindex.Interval{Min: 2, Max: 2}, lost.Blocks,
		"the want carries the identity the flush allocated")

	require.NoError(t, r.MergeWith(ctx, engine.MergeOptions{Force: true}))

	ix = loadIndex(t, be, lostPrefix)
	require.Empty(t, ix.Wanted, "a successor containing the lost blocks discharges the want")
	require.Len(t, ix.Entries, 1)

	succ := ix.Entries[0]
	assert.True(t, succ.Data(), "the want is repaired, not acknowledged as a hole")
	assert.True(t, succ.Supersedes(lost.Entry()))
	assert.Zero(t, ix.LostParts)

	_, ok := ix.Satisfying(lost)
	assert.True(t, ok, "and the successor is what Satisfying picks for it")
}

// TestBlocksSurviveRestart pins that identity is durable: it is read back from the committed index
// on open, so a restart neither re-allocates nor forgets, and the next flush numbers above it.
func TestBlocksSurviveRestart(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	be := backend.Memory()
	s := mkSeries("job", "api")
	e := newLostEngine(be)
	ids := flushIDs(ctx, t, e, be, s, 2)
	before := blocksByPrefix(loadIndex(t, be, lostPrefix))

	r := newLostEngine(be)
	require.NoError(t, r.LoadParts(ctx))
	mustAppend(t, r, s, 900, 9.0)
	require.NoError(t, r.Flush(ctx))

	after := blocksByPrefix(loadIndex(t, be, lostPrefix))
	for _, id := range ids {
		assert.Equal(t, before[lostPrefix+"/"+id].Blocks, after[lostPrefix+"/"+id].Blocks,
			"a restart must not renumber a part")
	}

	fresh := diskPartIDs(ctx, t, be)
	require.Len(t, fresh, 3)
	assert.Equal(t, bucketindex.Interval{Min: 3, Max: 3}, after[lostPrefix+"/"+fresh[2]].Blocks,
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

	const rival = lostPrefix + "/0000009999"

	inner := backend.Memory()
	be := faultbackend.Wrap(inner)
	gate := faultbackend.NewGate()
	be.Add(gate.Rule(faultbackend.CompareAndSwap, func(op faultbackend.Op) bool {
		return strings.HasSuffix(op.Key, "/"+bucketindex.Object)
	}))

	e := newLostEngine(be)
	mustAppend(t, e, mkSeries("job", "api"), 100, 1.0)

	flushed := make(chan error, 1)
	go func() { flushed <- e.Flush(ctx) }()

	gate.Await(t)

	other := &bucketindex.Index{Generation: bucketindex.Generation{Term: 1, Counter: 1}}
	other.Add(bucketindex.Entry{
		Prefix: rival, MinTime: 1, MaxTime: 2,
		Blocks: bucketindex.Interval{Min: 1, Max: 1},
	})
	_, err := other.Save(ctx, inner, lostIndexKey(), backend.VersionAbsent)
	require.NoError(t, err)

	gate.Release()
	require.NoError(t, <-flushed)

	ix, err := bucketindex.Load(ctx, inner, lostIndexKey())
	require.NoError(t, err)
	require.Len(t, ix.Entries, 2)

	got := blocksByPrefix(ix)
	require.Equal(t, bucketindex.Interval{Min: 1, Max: 1}, got[rival].Blocks,
		"the winner keeps the block it claimed")

	mine := diskPartIDs(ctx, t, inner)
	require.Len(t, mine, 1)
	assert.Equal(t, bucketindex.Interval{Min: 2, Max: 2}, got[lostPrefix+"/"+mine[0]].Blocks,
		"the loser re-allocates above the winner")
}

// TestPreV5PartsMigrateOnMerge is the migration path Decision 2 relies on: a merge is the only
// thing that rewrites an old part, so a merge whose inputs carry no interval allocates a fresh one
// for its output rather than committing another unset identity.
func TestPreV5PartsMigrateOnMerge(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	be := backend.Memory()
	e := newLostEngine(be)
	ids := flushIDs(ctx, t, e, be, mkSeries("job", "api"), 2)
	stripBlocks(ctx, t, be, ids...)

	r := newLostEngine(be)
	require.NoError(t, r.LoadParts(ctx))
	require.NoError(t, r.MergeWith(ctx, engine.MergeOptions{Force: true}))

	ix := loadIndex(t, be, lostPrefix)
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
	e := newLostEngine(be)
	ids := flushIDs(ctx, t, e, be, mkSeries("job", "api"), 3)
	stripBlocks(ctx, t, be, ids[0])

	r := newLostEngine(be)
	require.NoError(t, r.LoadParts(ctx))
	require.NoError(t, r.MergeWith(ctx, engine.MergeOptions{Force: true}))

	ix := loadIndex(t, be, lostPrefix)
	require.Len(t, ix.Entries, 1)
	assert.Equal(t, bucketindex.Interval{Min: 2, Max: 3}, ix.Entries[0].Blocks)
	assert.Equal(t, uint32(1), ix.Entries[0].Level)
}

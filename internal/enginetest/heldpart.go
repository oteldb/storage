package enginetest

// The index an engine loads may be a peer's copy that knows nothing of this node's disk, so a want
// is tried against the disk before any peer is asked. And a node whose claim over a prefix is not
// established records nothing: a part it cannot read stays a pending want until it commits as an
// owner.

import (
	"context"
	"slices"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/backend/bucketindex"
	"github.com/oteldb/storage/internal/partid"
)

func repairDischargedByPartHeldOnDisk(t *testing.T, k Kind) {
	t.Helper()

	ctx := context.Background()
	be := backend.Memory()
	f := NewFetcher(nil)
	e := k.openRepair(t, be, f)

	held := flushTwo(t, e)[0]

	// The index says the part is gone; the objects never left.
	e.LosePart(held, bucketindex.Interval{Min: 1, Max: 1})
	require.Equal(t, []string{held}, e.WantPrefixes())

	require.NoError(t, e.Merge(ctx, 0))

	assert.Empty(t, f.Asks(), "a part this disk holds is never asked of a peer")
	assert.Empty(t, e.WantPrefixes(), "opening it is what discharges the want")
	assert.Equal(t, int64(1), e.RepairStats().Local)
	assert.Equal(t, []Row{api(100, 1), api(200, 2)}, rows(t, e, apiStream),
		"the held rows joined the merge that carried the repair")
	assert.Empty(t, k.loadIndex(t, be).Wanted)
}

func loadPartsUnclaimedDefersTheWant(t *testing.T, k Kind) {
	t.Helper()

	ctx := context.Background()
	be := backend.Memory()
	parts := flushTwo(t, k.open(t, be))
	lost := parts[0]

	before := k.loadIndex(t, be)
	dropObjects(t, be, lost)

	e := k.open(t, be)
	require.NoError(t, e.LoadPartsUnclaimed(ctx))

	assert.Equal(t, 1, e.Stats().WantedParts, "the obligation is known here")
	assert.True(t, e.WantOverlaps(0, 1<<62), "and reads over it disclaim")
	assert.Equal(t, []string{parts[1]}, e.PartPrefixes())

	after := k.loadIndex(t, be)
	assert.Equal(t, before.Generation, after.Generation, "nothing was committed")
	assert.Empty(t, after.Wanted)
	assert.Len(t, after.Entries, 2, "the index still names the part it could not open")

	// The first commit this engine makes as a writer carries the want.
	e.Append(t, api(300, 3))
	require.NoError(t, e.Flush(ctx))

	committed := k.loadIndex(t, be)
	require.Len(t, committed.Wanted, 1)
	assert.Equal(t, lost, committed.Wanted[0].Prefix)
	assert.NotContains(t, prefixes(committed.Entries), lost, "a part leaves Entries only into Wanted")
	assert.Empty(t, committed.Removed, "a loss is not restated as a removal")
}

func loadPartsUnclaimedWantIsRepaired(t *testing.T, k Kind) {
	t.Helper()

	ctx := context.Background()
	be, peer := backend.Memory(), backend.Memory()
	lost := flushTwo(t, k.open(t, be))[0]

	CopyObjects(t, be, peer, lost)
	dropObjects(t, be, lost)

	f := NewFetcher(func(w bucketindex.Want) (bucketindex.Entry, bucketindex.WantOutcome, error) {
		CopyObjects(t, peer, be, w.Prefix)

		return w.Entry(), bucketindex.WantSatisfied, nil
	})
	e := k.openRepair(t, be, f)
	require.NoError(t, e.LoadPartsUnclaimed(ctx))
	require.Equal(t, 1, e.Stats().WantedParts)

	require.NoError(t, e.Merge(ctx, 0))

	assert.Equal(t, []string{lost}, f.Asks(), "a pending want is an obligation repair services")
	assert.Zero(t, e.Stats().WantedParts)
	assert.Equal(t, int64(1), e.RepairStats().Fetched)
	assert.Equal(t, []Row{api(100, 1), api(200, 2)}, rows(t, e, apiStream),
		"the repaired rows joined the merge that carried the repair")

	committed := k.loadIndex(t, be)
	assert.Empty(t, committed.Wanted)
	assert.NotEmpty(t, committed.Entries)
}

// loadPartsReadOnlySweepsNothing pins the mode storage.WithReadOnly selects: an orphan part object
// survives the load and nothing is committed, where the owning load reclaims it.
func loadPartsReadOnlySweepsNothing(t *testing.T, k Kind) {
	t.Helper()

	ctx := context.Background()
	be := backend.Memory()
	dropObjects(t, be, flushTwo(t, k.open(t, be))[0])

	orphan := k.Prefix + "/" + partid.New().String() + "/manifest"
	require.NoError(t, be.Write(ctx, orphan, []byte("orphaned part object")))

	before := k.loadIndex(t, be)

	e := k.Open(t, Config{Backend: be, Now: aged})
	require.NoError(t, e.LoadPartsReadOnly(ctx))

	assert.Equal(t, 1, e.Stats().WantedParts)
	assert.True(t, e.WantOverlaps(0, 1<<62), "reads over the missing part disclaim")

	_, err := be.Read(ctx, orphan)
	require.NoError(t, err, "a read-only load sweeps no orphan")
	assert.Equal(t, before.Generation, k.loadIndex(t, be).Generation, "nothing was committed")

	require.NoError(t, k.Open(t, Config{Backend: be, Now: aged}).LoadParts(ctx))

	_, err = be.Read(ctx, orphan)
	assert.ErrorIs(t, err, backend.ErrNotExist, "the owning load reclaims it")
}

// unindexedPeerPart produces the shape issue #587 is about: a part whose objects only the peer holds
// and which the local index does not name, has no tombstone for and has no want for — the state an
// owner is left in when a part was flushed by a node that then lost the shard.
func (k Kind) unindexedPeerPart(t *testing.T, be, peer backend.Backend) bucketindex.Entry {
	t.Helper()

	parts := flushTwo(t, k.open(t, be))

	ix := k.loadIndex(t, be)
	require.Len(t, ix.Entries, 2)

	i := slices.IndexFunc(ix.Entries, func(e bucketindex.Entry) bool { return e.Prefix == parts[0] })
	require.GreaterOrEqual(t, i, 0)
	ent := ix.Entries[i]

	CopyObjects(t, be, peer, ent.Prefix)
	dropObjects(t, be, ent.Prefix)

	out := &bucketindex.Index{Generation: ix.Generation, AllocatedBlocks: ix.AllocatedBlocks}
	for j := range ix.Entries {
		if ix.Entries[j].Prefix != ent.Prefix {
			out.Add(ix.Entries[j])
		}
	}

	require.NoError(t, be.Write(context.Background(), k.indexKey(), out.AppendBinary(nil)))

	return ent
}

// adoptedWantIsRepairedIntoTheIndex is the #587 acceptance: a part only a peer holds, which this
// engine's index never named and so could never report losing, becomes an obligation when handed
// in and is fetched and committed by the ordinary repair pass.
func adoptedWantIsRepairedIntoTheIndex(t *testing.T, k Kind) {
	t.Helper()

	ctx := context.Background()
	be, peer := backend.Memory(), backend.Memory()

	ent := k.unindexedPeerPart(t, be, peer)

	f := NewFetcher(nil)
	e := k.openRepair(t, be, f)
	require.NoError(t, e.LoadParts(ctx))
	require.NotContains(t, e.PartPrefixes(), ent.Prefix, "the local index never named it")
	require.False(t, e.HasWants(), "and nothing states that it is owed")

	e.AdoptWants([]bucketindex.Want{bucketindex.WantOf(ent, bucketindex.Generation{})})

	assert.True(t, e.HasWants(), "an adopted obligation is outstanding immediately")
	assert.True(t, e.WantOverlaps(ent.MinTime, ent.MaxTime), "a read of the window it covers is short until it is repaired")

	f.SetAnswer(func(w bucketindex.Want) (bucketindex.Entry, bucketindex.WantOutcome, error) {
		CopyObjects(t, peer, be, w.Prefix)

		return ent, bucketindex.WantSatisfied, nil
	})

	require.NoError(t, e.Merge(ctx, 0))

	assert.Equal(t, []string{ent.Prefix}, f.Asks())
	assert.Equal(t, int64(1), e.RepairStats().Fetched)
	assert.False(t, e.HasWants(), "committing the part discharges the obligation")
	assert.Empty(t, k.loadIndex(t, be).Wanted)

	// The prefix itself is gone: the same maintenance pass compacts the repaired part with the one
	// already here. The rows are the point, so they are what is asserted.
	assert.Equal(t, []Row{api(100, 1), api(200, 2)}, rows(t, e, apiStream), "the orphaned part's rows are readable here now")
}

// adoptWantsIgnoresWhatIsAlreadyHere pins the two no-ops: a part the index already names owes
// nothing, and the same obligation reported on every sync pass is recorded once.
func adoptWantsIgnoresWhatIsAlreadyHere(t *testing.T, k Kind) {
	t.Helper()

	ctx := context.Background()
	be, peer := backend.Memory(), backend.Memory()
	ent := k.unindexedPeerPart(t, be, peer)

	e := k.openRepair(t, be, NewFetcher(nil))
	require.NoError(t, e.LoadParts(ctx))
	mine := e.PartPrefixes()[0]

	e.AdoptWants([]bucketindex.Want{{Prefix: mine, MinTime: 100, MaxTime: 100}})
	assert.False(t, e.HasWants(), "a part this index already names is not owed")

	w := bucketindex.WantOf(ent, bucketindex.Generation{})
	for range 3 {
		e.AdoptWants([]bucketindex.Want{w})
	}

	assert.Equal(t, 1, e.Stats().WantedParts, "repeated reports of one part are one obligation")
}

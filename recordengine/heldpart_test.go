package recordengine_test

// The index an engine loads may be a peer's copy that knows nothing of this node's disk, so a want
// is tried against the disk before any peer is asked. And a node whose claim over a prefix is not
// established records nothing: a part it cannot read stays a pending want until it commits as an
// owner.

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/backend/bucketindex"
	"github.com/oteldb/storage/internal/partid"
	"github.com/oteldb/storage/recordengine"
)

func flushTwo(t *testing.T, e *recordengine.Engine) []string {
	t.Helper()
	ctx := context.Background()

	ingest(t, e, mkBatch("api", rrec{ts: 100, body: "p1"}))
	require.NoError(t, e.Flush(ctx))
	ingest(t, e, mkBatch("api", rrec{ts: 200, body: "p2"}))
	require.NoError(t, e.Flush(ctx))

	parts := e.PartPrefixes()
	require.Len(t, parts, 2)

	return parts
}

func TestRepairDischargedByPartHeldOnDisk(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	be := backend.Memory()
	f := &fakeFetcher{}
	e := newRepairEngine(t, be, f)

	parts := flushTwo(t, e)
	held := parts[0]

	// The index says the part is gone; the objects never left.
	e.LosePart(held, bucketindex.Interval{Min: 1, Max: 1})
	require.Equal(t, []string{held}, e.WantPrefixes())

	require.NoError(t, e.Merge(ctx, 0))

	assert.Empty(t, f.asks(), "a part this disk holds is never asked of a peer")
	assert.Empty(t, e.WantPrefixes(), "opening it is what discharges the want")
	assert.Equal(t, int64(1), e.RepairStats().Local)
	assert.ElementsMatch(t, []string{"p1", "p2"}, streamBodies(t, e), "the held rows joined the merge that carried the repair")
	assert.Empty(t, committedIndex(t, be).Wanted)
}

func TestLoadPartsUnclaimedDefersTheWant(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	be := backend.Memory()
	writer := newRepairEngine(t, be, nil)
	parts := flushTwo(t, writer)
	lost := parts[0]

	before := committedIndex(t, be)
	dropObjects(t, be, lost)

	e := newRepairEngine(t, be, nil)
	require.NoError(t, e.LoadPartsUnclaimed(ctx))

	assert.Equal(t, 1, e.Stats().WantedParts, "the obligation is known here")
	assert.True(t, e.WantOverlaps(0, 1<<62), "and reads over it disclaim")
	assert.Equal(t, []string{parts[1]}, e.PartPrefixes())

	after := committedIndex(t, be)
	assert.Equal(t, before.Generation, after.Generation, "nothing was committed")
	assert.Empty(t, after.Wanted)
	assert.Len(t, after.Entries, 2, "the index still names the part it could not open")

	// The first commit this engine makes as a writer carries the want.
	ingest(t, e, mkBatch("api", rrec{ts: 300, body: "p3"}))
	require.NoError(t, e.Flush(ctx))

	committed := committedIndex(t, be)
	require.Len(t, committed.Wanted, 1)
	assert.Equal(t, lost, committed.Wanted[0].Prefix)
	assert.NotContains(t, partPrefixesOf(committed), lost, "a part leaves Entries only into Wanted")
	assert.Empty(t, committed.Removed, "a loss is not restated as a removal")
}

func TestLoadPartsUnclaimedWantIsRepaired(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	be, peer := backend.Memory(), backend.Memory()
	writer := newRepairEngine(t, be, nil)
	parts := flushTwo(t, writer)
	lost := parts[0]

	copyObjects(t, be, peer, lost)
	dropObjects(t, be, lost)

	f := &fakeFetcher{answer: func(w bucketindex.Want) (bucketindex.Entry, bucketindex.WantOutcome, error) {
		copyObjects(t, peer, be, w.Prefix)

		return w.Entry(), bucketindex.WantSatisfied, nil
	}}
	e := newRepairEngine(t, be, f)
	require.NoError(t, e.LoadPartsUnclaimed(ctx))
	require.Equal(t, 1, e.Stats().WantedParts)

	require.NoError(t, e.Merge(ctx, 0))

	assert.Equal(t, []string{lost}, f.asks(), "a pending want is an obligation repair services")
	assert.Zero(t, e.Stats().WantedParts)
	assert.Equal(t, int64(1), e.RepairStats().Fetched)
	assert.ElementsMatch(t, []string{"p1", "p2"}, streamBodies(t, e), "the repaired rows joined the merge that carried the repair")

	committed := committedIndex(t, be)
	assert.Empty(t, committed.Wanted)
	assert.NotEmpty(t, committed.Entries)
}

// TestLoadPartsReadOnlySweepsNothing pins the mode storage.WithReadOnly selects: an orphan part
// object survives the load and nothing is committed, where the owning load reclaims it.
func TestLoadPartsReadOnlySweepsNothing(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	be := backend.Memory()
	writer := newRepairEngine(t, be, nil)
	parts := flushTwo(t, writer)
	dropObjects(t, be, parts[0])

	orphan := "t/recs/" + partid.New().String() + "/manifest"
	require.NoError(t, be.Write(ctx, orphan, []byte("orphaned part object")))

	before := committedIndex(t, be)

	e := newRepairEngine(t, be, nil)
	require.NoError(t, e.LoadPartsReadOnly(ctx))

	assert.Equal(t, 1, e.Stats().WantedParts)
	assert.True(t, e.WantOverlaps(0, 1<<62), "reads over the missing part disclaim")

	_, err := be.Read(ctx, orphan)
	assert.NoError(t, err, "a read-only load sweeps no orphan")
	assert.Equal(t, before.Generation, committedIndex(t, be).Generation, "nothing was committed")

	owner := newRepairEngine(t, be, nil)
	require.NoError(t, owner.LoadParts(ctx))

	_, err = be.Read(ctx, orphan)
	assert.ErrorIs(t, err, backend.ErrNotExist, "the owning load reclaims it")
}

func partPrefixesOf(ix *bucketindex.Index) []string {
	out := make([]string, 0, len(ix.Entries))
	for i := range ix.Entries {
		out = append(out, ix.Entries[i].Prefix)
	}

	return out
}

package recordengine_test

import (
	"context"
	"slices"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/backend/bucketindex"
)

// unindexedPeerPart leaves the state issue #587 is about: a part whose objects only the peer holds,
// which the local index does not name, has no tombstone for and has no want for.
func unindexedPeerPart(t *testing.T, be, peer backend.Backend) bucketindex.Entry {
	t.Helper()
	ctx := context.Background()

	writer := newRepairEngine(t, be, nil)
	ingest(t, writer, mkBatch("api", rrec{ts: 100, body: "p1"}))
	require.NoError(t, writer.Flush(ctx))
	ingest(t, writer, mkBatch("api", rrec{ts: 200, body: "p2"}))
	require.NoError(t, writer.Flush(ctx))

	parts := writer.PartPrefixes()
	require.Len(t, parts, 2)

	ix := committedIndex(t, be)
	i := slices.IndexFunc(ix.Entries, func(e bucketindex.Entry) bool { return e.Prefix == parts[0] })
	require.GreaterOrEqual(t, i, 0)
	ent := ix.Entries[i]

	copyObjects(t, be, peer, ent.Prefix)
	dropObjects(t, be, ent.Prefix)

	out := &bucketindex.Index{Generation: ix.Generation, AllocatedBlocks: ix.AllocatedBlocks}
	for j := range ix.Entries {
		if ix.Entries[j].Prefix != ent.Prefix {
			out.Add(ix.Entries[j])
		}
	}

	require.NoError(t, be.Write(ctx, "t/recs/"+bucketindex.Object, out.AppendBinary(nil)))

	return ent
}

// TestAdoptedWantIsRepairedIntoTheIndex is the record engine's half of the #587 acceptance: a part
// only a peer holds, which this index never named and so could never report losing, is fetched and
// committed once the obligation is handed in.
func TestAdoptedWantIsRepairedIntoTheIndex(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	be, peer := backend.Memory(), backend.Memory()

	ent := unindexedPeerPart(t, be, peer)

	f := &fakeFetcher{}
	e := newRepairEngine(t, be, f)
	require.NoError(t, e.LoadParts(ctx))
	require.NotContains(t, e.PartPrefixes(), ent.Prefix, "the local index never named it")
	require.False(t, e.HasWants(), "and nothing states that it is owed")

	e.AdoptWants([]bucketindex.Want{bucketindex.WantOf(ent, bucketindex.Generation{})})

	assert.True(t, e.HasWants())
	assert.True(t, e.WantOverlaps(ent.MinTime, ent.MaxTime),
		"a read of the window it covers is short until it is repaired")

	f.answer = func(w bucketindex.Want) (bucketindex.Entry, bucketindex.WantOutcome, error) {
		copyObjects(t, peer, be, w.Prefix)

		return ent, bucketindex.WantSatisfied, nil
	}

	require.NoError(t, e.Merge(ctx, 0))

	assert.Equal(t, []string{ent.Prefix}, f.asks())
	assert.Equal(t, int64(1), e.RepairStats().Fetched)
	assert.False(t, e.HasWants(), "committing the part discharges the obligation")
	assert.ElementsMatch(t, []string{"p1", "p2"}, streamBodies(t, e),
		"the orphaned part's records are readable here now")
	assert.Empty(t, committedIndex(t, be).Wanted)
}

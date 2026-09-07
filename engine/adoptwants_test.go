package engine_test

import (
	"context"
	"slices"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/backend/bucketindex"
	"github.com/oteldb/storage/engine"
)

// unindexedPeerPart produces the shape issue #587 is about: a part whose objects only the peer
// holds and which the local index does not name, has no tombstone for, and has no want for — the
// state an owner is left in when a part was flushed by a node that then lost the shard.
func unindexedPeerPart(t *testing.T, be, peer backend.Backend) bucketindex.Entry {
	t.Helper()
	ctx := context.Background()

	writer := engine.New(engine.Config{Backend: be, Prefix: "default/metrics"})
	parts := flushSamples(t, writer, 2)

	ix := metricsIndex(t, be)
	require.Len(t, ix.Entries, 2)

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

	require.NoError(t, be.Write(ctx, "default/metrics/"+bucketindex.Object, out.AppendBinary(nil)))

	return ent
}

// TestAdoptedWantIsRepairedIntoTheIndex is the issue #587 acceptance: a part only a peer holds,
// which this engine's index never named and therefore could never report losing, becomes an
// obligation when handed in and is fetched and committed by the ordinary repair pass.
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

	assert.True(t, e.HasWants(), "an adopted obligation is outstanding immediately")
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
	assert.Empty(t, metricsIndex(t, be).Wanted)

	// The prefix itself is gone: the same maintenance pass compacts the repaired part with the one
	// already here. The rows are the point, so they are what is asserted.
	ts, vals := seriesSamples(t, e)
	assert.Equal(t, []int64{100, 200}, ts, "the orphaned part's rows are readable here now")
	assert.Equal(t, []float64{1, 2}, vals)
}

// TestAdoptWantsIgnoresWhatIsAlreadyHere pins the two no-ops: a part the index already names owes
// nothing, and the same obligation reported on every sync pass is recorded once.
func TestAdoptWantsIgnoresWhatIsAlreadyHere(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	be, peer := backend.Memory(), backend.Memory()
	ent := unindexedPeerPart(t, be, peer)

	e := newRepairEngine(t, be, &fakeFetcher{})
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

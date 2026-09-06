package engine_test

import (
	"context"
	"math"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/backend/bucketindex"
	"github.com/oteldb/storage/engine"
	"github.com/oteldb/storage/query/fetch"
)

// splitCeilingBytes is a merge ceiling the streamed rewrite of three parts crosses several times, so
// a retention rewrite over them — forced regardless of the seal — splits its output. The writer
// seals on frames already compressed, and a frame holds 64 KiB of encoded values, so the parts have
// to carry tens of thousands of incompressible samples for the ceiling to be reached at all.
const (
	splitCeilingBytes = 1024
	seriesPerPart     = 5
	samplesPerSeries  = 2000
)

// retainFrom drops an expiring part's first sample per series and keeps the rest, so a retention
// merge rewrites the part without emptying it.
const retainFrom = 50

func splitLineageEngine(be backend.Backend, ceiling int64, repair engine.PartFetcher) *engine.Engine {
	return engine.New(engine.Config{Backend: be, Prefix: lostPrefix, MergeCeilingBytes: ceiling, Repair: repair})
}

// flushSeriesRun flushes one part holding seriesPerPart distinct series numbered from base, each
// with samplesPerSeries incompressible samples after retainFrom and, if expiring, one before it.
func flushSeriesRun(ctx context.Context, t *testing.T, e *engine.Engine, base int, expiring bool) {
	t.Helper()

	for i := range seriesPerPart {
		s := mkSeries("id", strconv.Itoa(base+i))
		if expiring {
			mustAppend(t, e, s, 1, math.Sin(float64(base+i)))
		}

		for k := range samplesPerSeries {
			ts := int64(100 + 10*samplesPerSeries*(base+i) + k)
			mustAppend(t, e, s, ts, math.Sin(float64(ts*7919)))
		}
	}

	require.NoError(t, e.Flush(ctx))
}

func countSeries(t *testing.T, e *engine.Engine) int {
	t.Helper()

	return len(fetchAll(t, e, fetch.Request{Start: 0, End: 1 << 60}))
}

// flushInputs flushes three expiring parts — the inputs of the split — and, with a bystander, a
// fourth that retention leaves alone and the ceiling seals, so it keeps the top block number live
// while the split runs. It returns the committed entries in flush order.
func flushInputs(ctx context.Context, t *testing.T, be backend.Backend, bystander bool) []bucketindex.Entry {
	t.Helper()

	e := splitLineageEngine(be, splitCeilingBytes, nil)

	n := 3
	if bystander {
		n = 4
	}

	for i := range n {
		flushSeriesRun(ctx, t, e, seriesPerPart*i, i < 3)
	}

	ids := diskPartIDs(ctx, t, be)
	require.Len(t, ids, n)

	ix, err := bucketindex.Load(ctx, be, lostIndexKey())
	require.NoError(t, err)

	byPrefix := blocksByPrefix(ix)
	out := make([]bucketindex.Entry, 0, n)

	for i, id := range ids {
		ent := byPrefix[lostPrefix+"/"+id]
		require.Equal(t, bucketindex.Interval{Min: uint64(i + 1), Max: uint64(i + 1)}, ent.Blocks)
		out = append(out, ent)
	}

	return out
}

// splitMerge rewrites the expiring parts for retention under the split ceiling and returns the
// committed index, which must hold several outputs in their place.
func splitMerge(ctx context.Context, t *testing.T, be backend.Backend, before []bucketindex.Entry) *bucketindex.Index {
	t.Helper()

	e := splitLineageEngine(be, splitCeilingBytes, nil)
	require.NoError(t, e.LoadParts(ctx))
	require.NoError(t, e.MergeWith(ctx, engine.MergeOptions{RetainFrom: retainFrom}))

	ix, err := bucketindex.Load(ctx, be, lostIndexKey())
	require.NoError(t, err)
	require.Len(t, ix.Removed, 3, "the rewrite retires the three expiring inputs")

	fragments := 0

	for i := range ix.Entries {
		ent := &ix.Entries[i]
		if !slices.ContainsFunc(before, func(in bucketindex.Entry) bool { return in.Prefix == ent.Prefix }) {
			fragments++
		}
	}

	require.Greater(t, fragments, 1, "and splits its output under the ceiling")

	return ix
}

// TestSplitMergeAllocatesAboveItsInputs pins that a split output never takes a block number one of
// its inputs held, even though the commit that publishes the fragments is the one that retires
// those inputs. Numbering runs above the persisted high-water mark rather than above the live set,
// so nothing rewinds into the identity of a part the same commit removed — a fragment holding a
// third of the rows would otherwise supersede a whole input, and answer a peer's want with it.
func TestSplitMergeAllocatesAboveItsInputs(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	be := backend.Memory()
	inputs := flushInputs(ctx, t, be, false)

	ix := splitMerge(ctx, t, be, inputs)
	for _, f := range ix.Entries {
		assert.Greater(t, f.Blocks.Min, inputs[2].Blocks.Max, "fragment %+v is numbered below a retired input", f)

		for _, in := range inputs {
			assert.False(t, f.Supersedes(in), "fragment %+v holds a fraction of %+v and must not claim it", f, in)
		}
	}
}

// TestSplitMergeSeversLineage pins that a split does not sever the lineage. The fragments take fresh
// blocks and none of them supersedes an input on its own, but they carry the inputs' blocks as a
// joint claim, so the group answers a want for a pre-split part while it is spread across them and a
// merge that consumes the whole group folds the claim back into one ordinary interval — which every
// later merge then inherits.
func TestSplitMergeSeversLineage(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	be := backend.Memory()
	inputs := flushInputs(ctx, t, be, true)
	lost := inputs[1]
	want := bucketindex.WantOf(lost, bucketindex.Generation{})

	ix := splitMerge(ctx, t, be, inputs)
	for _, f := range ix.Entries {
		require.False(t, f.Supersedes(lost), "a fragment holds a fraction of the input and must not claim it: %+v", f)
	}

	// No *single* fragment answers the want, but the group does: every one of its members is here,
	// so the rows are too. That is what TestSplitLineageWantBecomesHole needs a peer to say, and it
	// is the opposite of what this line asserted while the split severed the lineage outright.
	got, ok := ix.Satisfying(want)
	require.True(t, ok, "the whole group is present, so the want is answerable")
	require.False(t, got.Supersedes(lost), "and it is answered by a member, not by a part containing it")

	// "corrected by the next merge": rejoin the fragments, and keep merging until the part set is a
	// fixed point, then fold in one more part.
	rejoin := splitLineageEngine(be, -1, nil)
	require.NoError(t, rejoin.LoadParts(ctx))

	for parts := 0; parts != rejoin.PartCount(); {
		parts = rejoin.PartCount()
		require.NoError(t, rejoin.MergeWith(ctx, engine.MergeOptions{Force: true}))
	}

	require.Equal(t, 4*seriesPerPart, countSeries(t, rejoin), "every row of the lost part is inside the rejoined set")

	ix = loadIndex(t, be, lostPrefix)
	require.Len(t, ix.Entries, 1, "the forced merges collapse the part set")
	assert.True(t, ix.Entries[0].Supersedes(lost),
		"the rejoined part %+v holds all of %+v and must supersede it", ix.Entries[0], lost)

	flushSeriesRun(ctx, t, rejoin, 4*seriesPerPart, false)
	require.NoError(t, rejoin.MergeWith(ctx, engine.MergeOptions{Force: true}))

	ix = loadIndex(t, be, lostPrefix)
	require.Len(t, ix.Entries, 1)

	_, ok = ix.Satisfying(want)
	assert.True(t, ok, "a want naming the pre-split part is never satisfied: after further merges the index holds %+v", ix.Entries[0])
}

// TestSplitLineageWantBecomesHole pins the loss #548 led to. One replica loses a part; the other has
// merged it into split fragments that hold every one of its rows. The peer's index must answer that
// the want is satisfiable, repair must pull the whole group rather than the one member the peer
// names, and no hole may be acknowledged for data that is entirely on the peer's disk.
func TestSplitLineageWantBecomesHole(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	be, peer := backend.Memory(), backend.Memory()
	inputs := flushInputs(ctx, t, be, true)
	lost := inputs[1]

	copyObjects(t, be, peer, lostPrefix+"/")
	splitMerge(ctx, t, peer, inputs)

	served := splitLineageEngine(peer, splitCeilingBytes, nil)
	require.NoError(t, served.LoadParts(ctx))
	require.Equal(t, 4*seriesPerPart, countSeries(t, served), "the peer holds every row of the lost part")

	// The cluster layer's answer to a want: the best part in the peer's index satisfying it, copied
	// over; definitive absence when the index names none.
	fetcher := &fakeFetcher{answer: func(w bucketindex.Want) (bucketindex.Entry, bucketindex.WantOutcome, error) {
		ent, ok := loadIndex(t, peer, lostPrefix).Satisfying(w)
		if !ok {
			return bucketindex.Entry{}, bucketindex.WantAbsent, nil
		}

		copyObjects(t, peer, be, ent.Prefix+"/")

		return ent, bucketindex.WantSatisfied, nil
	}}

	_, id, _ := strings.Cut(lost.Prefix, lostPrefix+"/")
	erasePart(ctx, t, be, id)

	r := splitLineageEngine(be, splitCeilingBytes, fetcher)
	require.NoError(t, r.LoadParts(ctx))
	require.Equal(t, []string{lost.Prefix}, wantPrefixes(loadIndex(t, be, lostPrefix).Wanted))

	for range 3 {
		require.NoError(t, r.MergeWith(ctx, engine.MergeOptions{Force: true}))
	}

	ix := loadIndex(t, be, lostPrefix)
	assert.Zero(t, ix.LostParts, "the peer holds the data, so no loss may be acknowledged; holes: %v", prefixes(ix.Holes()))
	assert.Equal(t, 4*seriesPerPart, countSeries(t, r), "repair brings the lost rows back from the peer")
}

// TestMergeAroundALostPartKeepsItsWant pins that a merge of a lost part's neighbors claims none of
// its blocks. The union of block sets is a set and not a hull, so merging {1} and {3} yields {1,3}
// and cannot discharge a want for block 2 — which a hull did, leaving no hole, no want, and a read
// short by the whole part.
func TestMergeAroundALostPartKeepsItsWant(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	be := backend.Memory()
	e := newLostEngine(be)
	ids := flushIDs(ctx, t, e, be, mkSeries("job", "api"), 3)

	erasePart(ctx, t, be, ids[1])

	r := newLostEngine(be)
	require.NoError(t, r.LoadParts(ctx))
	require.Equal(t, []string{lostPrefix + "/" + ids[1]}, wantPrefixes(loadIndex(t, be, lostPrefix).Wanted))

	require.NoError(t, r.MergeWith(ctx, engine.MergeOptions{Force: true}))

	ix := loadIndex(t, be, lostPrefix)
	require.Len(t, ix.Entries, 1)
	assert.Equal(t, []float64{1, 3}, lostValues(t, r), "the merged part holds only the neighbors' rows")
	assert.Equal(t, []string{lostPrefix + "/" + ids[1]}, wantPrefixes(ix.Wanted),
		"no part holds the lost rows, so the want must stand; the index has %+v", ix.Entries[0])
}

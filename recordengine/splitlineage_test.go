package recordengine_test

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/backend/bucketindex"
	"github.com/oteldb/storage/internal/reproduce"
	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/recordengine"
	"github.com/oteldb/storage/signal"
)

// splitPartBytes is both the flush cap and — with the merge memory allowance driven to nothing — the
// decoded size at which a merge seals its output. Three parts each well under it rewrite into more
// than one part. The merge seals only at a stream boundary, so every record is its own stream.
const (
	splitPartBytes = 2048
	recordsPerPart = 3
	splitBodyBytes = 400
)

// retainFrom drops an expiring part's one early record and keeps the rest, so a retention merge
// rewrites the part without emptying it.
const retainFrom = 50

func splitLineageEngine(be backend.Backend, maxPartBytes int64, repair recordengine.PartFetcher) *recordengine.Engine {
	return recordengine.New(recordengine.Config{
		Schema: testSchema, Backend: be, Prefix: enginePrefix,
		MaxPartBytes: maxPartBytes, MergeMemoryBytes: 1, Repair: repair,
	})
}

// flushRecordRun flushes one part holding recordsPerPart single-record streams numbered from base
// after retainFrom and, if expiring, one record before it.
func flushRecordRun(ctx context.Context, t *testing.T, e *recordengine.Engine, base int, expiring bool) {
	t.Helper()

	if expiring {
		ingest(t, e, mkBatch("expired", rrec{ts: 1, body: "expired"}))
	}

	for i := range recordsPerPart {
		n := base + i
		ingest(t, e, mkBatch(fmt.Sprintf("svc-%02d", n), rrec{
			ts: int64(100 + n), body: fmt.Sprintf("body-%02d-", n) + strings.Repeat("x", splitBodyBytes),
		}))
	}

	require.NoError(t, e.Flush(ctx))
}

// countRecords counts the records retention keeps, across every stream.
func countRecords(t *testing.T, e *recordengine.Engine) int {
	t.Helper()

	n := 0
	for _, b := range fetchAll(t, e, fetch.Request{Signal: signal.Log, Start: retainFrom, End: 1 << 60}) {
		n += len(bodies(b))
	}

	return n
}

// flushInputs flushes three expiring parts — the inputs of the split — and, with a bystander, a
// fourth that retention leaves alone and the cap keeps out of the rewrite, so it holds the top block
// number live while the split runs. It returns the committed entries in flush order.
func flushInputs(ctx context.Context, t *testing.T, be backend.Backend, bystander bool) []bucketindex.Entry {
	t.Helper()

	e := splitLineageEngine(be, splitPartBytes, nil)

	n := 3
	if bystander {
		n = 4
	}

	for i := range n {
		flushRecordRun(ctx, t, e, recordsPerPart*i, i < 3)
	}

	ids := diskPartIDs(ctx, t, be)
	require.Len(t, ids, n, "one part per flush")

	ix, err := bucketindex.Load(ctx, be, indexKey())
	require.NoError(t, err)

	byPrefix := blocksByPrefix(ix)
	out := make([]bucketindex.Entry, 0, n)

	for i, id := range ids {
		ent := byPrefix[enginePrefix+"/"+id]
		require.Equal(t, bucketindex.Interval{Min: uint64(i + 1), Max: uint64(i + 1)}, ent.Blocks)
		out = append(out, ent)
	}

	return out
}

// splitMerge rewrites the expiring parts for retention under the cap and returns the committed index,
// which must hold several outputs in their place.
func splitMerge(ctx context.Context, t *testing.T, be backend.Backend, before []bucketindex.Entry) *bucketindex.Index {
	t.Helper()

	e := splitLineageEngine(be, splitPartBytes, nil)
	require.NoError(t, e.LoadParts(ctx))
	require.NoError(t, e.MergeWith(ctx, recordengine.MergeOptions{RetainFrom: retainFrom}))

	ix, err := bucketindex.Load(ctx, be, indexKey())
	require.NoError(t, err)
	require.Len(t, ix.Removed, 3, "the rewrite retires the three expiring inputs")

	fragments := 0

	for _, ent := range ix.Entries {
		if !slices.ContainsFunc(before, func(in bucketindex.Entry) bool { return in.Prefix == ent.Prefix }) {
			fragments++
		}
	}

	require.Greater(t, fragments, 1, "and splits its output under the cap")

	return ix
}

// TestSplitMergeAllocatesAboveItsInputs pins that a split output never takes a block number one of
// its inputs held. NextBlock is one above the highest block the index still names, and the commit
// that publishes the fragments is the one that retires the inputs — so with the inputs holding the
// top blocks, the fragments are numbered from the bottom again, each one the same interval as an
// input one level up. A fragment holding part of the rows then supersedes a whole input, and a
// peer's want for that input is answered with it.
func TestSplitMergeAllocatesAboveItsInputs(t *testing.T) {
	t.Parallel()
	reproduce.Unfixed(t, 548, "split fragments reuse the block numbers of the inputs the same commit retires")

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

// TestSplitMergeSeversLineage is the reproducer for #548. A merge that splits its output hands the
// fragments fresh intervals, so none of them supersedes an input; the doc on planMergeBlocks says the
// next merge corrects that. It does not: the fragments' successor inherits the fresh intervals and
// never the ancestors', so a want naming a pre-split part is not satisfied by any later part, however
// many merges follow and even when every row of it is inside one.
func TestSplitMergeSeversLineage(t *testing.T) {
	t.Parallel()
	reproduce.Unfixed(t, 548, "a split merge's fragments and every part merged from them claim fresh blocks, so no successor ever contains a pre-split part's interval")

	ctx := context.Background()
	be := backend.Memory()
	inputs := flushInputs(ctx, t, be, true)
	lost := inputs[1]
	want := bucketindex.WantOf(lost, bucketindex.Generation{})

	ix := splitMerge(ctx, t, be, inputs)
	for _, f := range ix.Entries {
		require.False(t, f.Supersedes(lost), "a fragment holds a fraction of the input and must not claim it: %+v", f)
	}

	_, ok := ix.Satisfying(want)
	require.False(t, ok, "no fragment satisfies the want, as documented")

	// "corrected by the next merge": rejoin the fragments, and keep merging until the part set is a
	// fixed point, then fold in one more part.
	rejoin := splitLineageEngine(be, 0, nil)
	require.NoError(t, rejoin.LoadParts(ctx))

	for parts := 0; parts != rejoin.PartCount(); {
		parts = rejoin.PartCount()
		require.NoError(t, rejoin.MergeWith(ctx, recordengine.MergeOptions{Force: true}))
	}

	require.Equal(t, 4*recordsPerPart, countRecords(t, rejoin), "every row of the lost part is inside the rejoined set")

	ix = loadIndex(t, be)
	require.Len(t, ix.Entries, 1, "the forced merges collapse the part set")
	assert.True(t, ix.Entries[0].Supersedes(lost),
		"the rejoined part %+v holds all of %+v and must supersede it", ix.Entries[0], lost)

	flushRecordRun(ctx, t, rejoin, 4*recordsPerPart, false)
	require.NoError(t, rejoin.MergeWith(ctx, recordengine.MergeOptions{Force: true}))

	ix = loadIndex(t, be)
	require.Len(t, ix.Entries, 1)

	_, ok = ix.Satisfying(want)
	assert.True(t, ok, "a want naming the pre-split part is never satisfied: after further merges the index holds %+v", ix.Entries[0])
}

// TestSplitLineageWantBecomesHole is the loss #548 leads to. One replica loses a part; the other
// has merged it into split fragments that hold every one of its rows. Repair asks the peer's index
// for a part satisfying the want, none claims the interval, and after enough confirmations the want
// is acknowledged as a hole — for data that is entirely on the peer's disk.
func TestSplitLineageWantBecomesHole(t *testing.T) {
	t.Parallel()
	reproduce.Unfixed(t, 548, "repair commits a hole for a part whose rows are all inside a peer's split merge output")

	ctx := context.Background()
	be, peer := backend.Memory(), backend.Memory()
	inputs := flushInputs(ctx, t, be, true)
	lost := inputs[1]

	copyObjects(t, be, peer, enginePrefix+"/")
	splitMerge(ctx, t, peer, inputs)

	served := splitLineageEngine(peer, splitPartBytes, nil)
	require.NoError(t, served.LoadParts(ctx))
	require.Equal(t, 4*recordsPerPart, countRecords(t, served), "the peer holds every row of the lost part")

	// The cluster layer's answer to a want: the best part in the peer's index satisfying it, copied
	// over; definitive absence when the index names none.
	fetcher := &fakeFetcher{answer: func(w bucketindex.Want) (bucketindex.Entry, bucketindex.WantOutcome, error) {
		ent, ok := loadIndex(t, peer).Satisfying(w)
		if !ok {
			return bucketindex.Entry{}, bucketindex.WantAbsent, nil
		}

		copyObjects(t, peer, be, ent.Prefix+"/")

		return ent, bucketindex.WantSatisfied, nil
	}}

	_, id, _ := strings.Cut(lost.Prefix, enginePrefix+"/")
	erasePart(ctx, t, be, id)

	r := splitLineageEngine(be, splitPartBytes, fetcher)
	require.NoError(t, r.LoadParts(ctx))
	require.Equal(t, []string{lost.Prefix}, wantPrefixes(loadIndex(t, be).Wanted))

	for range 3 {
		require.NoError(t, r.MergeWith(ctx, recordengine.MergeOptions{Force: true}))
	}

	ix := loadIndex(t, be)
	assert.Zero(t, ix.LostParts, "the peer holds the data, so no loss may be acknowledged; holes: %v", prefixes(ix.Holes()))
	assert.Equal(t, 4*recordsPerPart, countRecords(t, r), "repair brings the lost rows back from the peer")
}

// TestMergeAroundALostPartKeepsItsWant is a defect the algebra behind #548 shares its root with,
// though not the one the issue names. Interval.Union is a hull: merging the neighbors of a lost part
// yields an interval covering its block, so the want is discharged with the data still gone — no
// hole, no want, and a read short by the whole part.
func TestMergeAroundALostPartKeepsItsWant(t *testing.T) {
	t.Parallel()
	reproduce.Unfixed(t, 548, "the hull of a lost part's neighbors discharges the want for it while its rows are absent")

	ctx := context.Background()
	be := backend.Memory()
	e := newEngine(t, be)
	ids := flushIDs(ctx, t, e, be, 3)

	erasePart(ctx, t, be, ids[1])

	r := newEngine(t, be)
	require.NoError(t, r.LoadParts(ctx))
	require.Equal(t, []string{enginePrefix + "/" + ids[1]}, wantPrefixes(loadIndex(t, be).Wanted))

	require.NoError(t, r.MergeWith(ctx, recordengine.MergeOptions{Force: true}))

	ix := loadIndex(t, be)
	require.Len(t, ix.Entries, 1)

	got := make([]string, 0, 2)
	for _, b := range fetchAll(t, r, req("api")) {
		got = append(got, bodies(b)...)
	}

	slices.Sort(got)
	assert.Equal(t, []string{"p1", "p3"}, got, "the merged part holds only the neighbors' rows")
	assert.Equal(t, []string{enginePrefix + "/" + ids[1]}, wantPrefixes(ix.Wanted),
		"no part holds the lost rows, so the want must stand; the index has %+v", ix.Entries[0])
}

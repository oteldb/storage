package enginetest

import (
	"context"
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/backend/bucketindex"
)

// splitInputs is how many straddlers one split merge takes together.
const splitInputs = 3

// flushSplitInputs flushes splitInputs straddlers, each a stream of its own with a row on the stale
// day and one on the next, and returns their committed entries in flush order.
func (k Kind) flushSplitInputs(t *testing.T, be backend.Backend) []bucketindex.Entry {
	t.Helper()

	e := k.open(t, be)
	spans := make([][2]int64, splitInputs)

	for i := range spans {
		spans[i] = [2]int64{staleTs + int64(i), day + hour + int64(i)}
	}

	flushSpans(t, e, spans)

	byPrefix := blocksByPrefix(k.loadIndex(t, be))
	ids := k.partDirs(context.Background(), t, be)
	require.Len(t, ids, splitInputs)

	out := make([]bucketindex.Entry, 0, splitInputs)
	for _, id := range ids {
		out = append(out, byPrefix[k.Prefix+"/"+id])
	}

	return out
}

// splitAll merges the straddlers over be in one merge, which writes a part per day, and returns the
// committed index and the fragments.
func (k Kind) splitAll(t *testing.T, be backend.Backend, inputs []bucketindex.Entry) (*bucketindex.Index, []bucketindex.Entry) {
	t.Helper()

	e := k.open(t, be)
	require.NoError(t, e.LoadParts(context.Background()))
	require.NoError(t, e.Merge(context.Background(), 0))

	ix := k.loadIndex(t, be)
	require.Len(t, ix.Removed, len(inputs), "one merge retires every straddler")

	var fragments []bucketindex.Entry

	for i := range ix.Entries {
		ent := &ix.Entries[i]
		if !slices.ContainsFunc(inputs, func(in bucketindex.Entry) bool { return in.Prefix == ent.Prefix }) {
			fragments = append(fragments, *ent)
		}
	}

	require.Len(t, fragments, 2, "the merge writes one fragment per day")

	return ix, fragments
}

// requireWantsSatisfied fails unless ix answers a want for every input.
func requireWantsSatisfied(t *testing.T, ix *bucketindex.Index, inputs []bucketindex.Entry) {
	t.Helper()

	for i := range inputs {
		in := &inputs[i]
		_, ok := ix.Satisfying(bucketindex.WantOf(*in, bucketindex.Generation{}))
		require.True(t, ok, "a want for %s must be answerable", in.Prefix)
	}
}

// splitOfSeveralInputsKeepsEveryLineage is the multi-input half of the split-lineage tests: a merge
// of several sources writing several fragments must claim every source's blocks jointly, so a want
// for any one of them is answered while the group is whole and after the fragments merge on.
func splitOfSeveralInputsKeepsEveryLineage(t *testing.T, k Kind) {
	t.Helper()

	be := backend.Memory()
	inputs := k.flushSplitInputs(t, be)

	ix, fragments := k.splitAll(t, be, inputs)

	for i := range fragments {
		f := &fragments[i]
		for j := range inputs {
			in := &inputs[j]
			assert.Positive(t, f.Blocks.Min.Compare(in.Blocks.Max), "fragment %+v is numbered below retired %+v", f, in)
			assert.False(t, f.Supersedes(*in), "fragment %+v holds a fraction of %+v and must not claim it", f, in)
		}
	}

	requireWantsSatisfied(t, ix, inputs)

	e := k.open(t, be)
	require.NoError(t, e.LoadParts(context.Background()))

	want := storedRows(t, e, streamNames(splitInputs)...)

	e.Append(t, Row{Stream: "p0", Ts: staleTs + int64(time.Minute), Val: 100}, Row{Stream: "p0", Ts: staleTs + 2*int64(time.Minute), Val: 101})
	require.NoError(t, e.Flush(context.Background()))
	e.Append(t, Row{Stream: "p1", Ts: day + hour + int64(time.Minute), Val: 102})
	require.NoError(t, e.Flush(context.Background()))

	mergeToFixpoint(t, e)
	require.Less(t, e.PartCount(), len(fragments)+2, "the fragments merge on with their days' parts")

	requireWantsSatisfied(t, k.loadIndex(t, be), inputs)
	assert.Subset(t, storedRows(t, e, streamNames(splitInputs)...), want)
}

// splitOfSeveralInputsRepairsFromPeer is the loss the joint claim prevents: one replica loses a
// straddler the other merged into day fragments. The peer's index must answer the want, and repair
// must bring every lost row back rather than acknowledge a hole.
func splitOfSeveralInputsRepairsFromPeer(t *testing.T, k Kind) {
	t.Helper()

	ctx := context.Background()
	be, peer := backend.Memory(), backend.Memory()
	inputs := k.flushSplitInputs(t, be)
	lost := inputs[1]

	CopyObjects(t, be, peer, k.Prefix+"/")
	k.splitAll(t, peer, inputs)

	w := k.open(t, be)
	require.NoError(t, w.LoadParts(ctx))
	want := storedRows(t, w, streamNames(splitInputs)...)

	fetcher := NewFetcher(func(wt bucketindex.Want) (bucketindex.Entry, bucketindex.WantOutcome, error) {
		ent, ok := k.loadIndex(t, peer).Satisfying(wt)
		if !ok {
			return bucketindex.Entry{}, bucketindex.WantAbsent, nil
		}

		CopyObjects(t, peer, be, ent.Prefix+"/")

		return ent, bucketindex.WantSatisfied, nil
	})

	dropObjects(t, be, lost.Prefix+"/")

	r := k.openRepair(t, be, fetcher)
	require.NoError(t, r.LoadParts(ctx))
	require.Equal(t, []string{lost.Prefix}, wantPrefixes(k.loadIndex(t, be).Wanted))

	for range splitInputs {
		require.NoError(t, r.ForceMerge(ctx))
	}

	assert.Zero(t, k.loadIndex(t, be).LostParts, "the peer holds the data, so no loss may be acknowledged")
	assert.Equal(t, want, storedRows(t, r, streamNames(splitInputs)...), "repair brings every lost row back, once")
}

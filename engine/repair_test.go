package engine_test

import (
	"context"
	"slices"
	"sync"
	"testing"

	"github.com/go-faster/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/backend/bucketindex"
	"github.com/oteldb/storage/engine"
)

// fakeFetcher stands in for the cluster layer's part-sync pull: it records what repair asked for
// and answers with whatever the test staged, so the engine half of the seam is exercised without a
// peer, a transport or a syncer.
type fakeFetcher struct {
	// mu guards asked: repair runs up to repairFetchConcurrency fetches at once.
	mu     sync.Mutex
	asked  []string
	answer func(w bucketindex.Want) (bucketindex.Entry, bucketindex.WantOutcome, error)
}

func (f *fakeFetcher) FetchWant(_ context.Context, w bucketindex.Want) (bucketindex.Entry, bucketindex.WantOutcome, error) {
	f.mu.Lock()
	f.asked = append(f.asked, w.Prefix)
	answer := f.answer
	f.mu.Unlock()

	if answer == nil {
		return bucketindex.Entry{}, bucketindex.WantAbsent, nil
	}

	return answer(w)
}

// asks reports what repair has asked for, in call order.
func (f *fakeFetcher) asks() []string {
	f.mu.Lock()
	defer f.mu.Unlock()

	return slices.Clone(f.asked)
}

func newRepairEngine(t *testing.T, be backend.Backend, r engine.PartFetcher) *engine.Engine {
	t.Helper()

	return engine.New(engine.Config{Backend: be, Prefix: "default/metrics", Repair: r})
}

// copyObjects copies every object under prefix from src to dst, which is what a repair fetch does
// to the objects of a part it pulls back.
func copyObjects(t *testing.T, src, dst backend.Backend, prefix string) {
	t.Helper()
	ctx := context.Background()

	keys, err := src.List(ctx, prefix)
	require.NoError(t, err)
	require.NotEmpty(t, keys)

	for _, k := range keys {
		data, err := backend.ReadView(ctx, src, k)
		require.NoError(t, err)
		require.NoError(t, dst.Write(ctx, k, data))
	}
}

// dropObjects deletes every object of a part, simulating the disk loss a want records.
func dropObjects(t *testing.T, be backend.Backend, prefix string) {
	t.Helper()
	ctx := context.Background()

	keys, err := be.List(ctx, prefix)
	require.NoError(t, err)
	require.NotEmpty(t, keys)

	for _, k := range keys {
		require.NoError(t, be.Delete(ctx, k))
	}
}

// metricsIndex reads the index the repair tests' engine prefix holds.
func metricsIndex(t *testing.T, be backend.Backend) *bucketindex.Index {
	t.Helper()

	ix, err := bucketindex.Load(context.Background(), be, "default/metrics/"+bucketindex.Object)
	require.NoError(t, err)

	return ix
}

// flushSamples writes one part per sample, returning the engine's part prefixes.
func flushSamples(t *testing.T, e *engine.Engine, n int) []string {
	t.Helper()
	ctx := context.Background()
	s := mkSeries("job", "api")

	for i := range n {
		mustAppend(t, e, s, int64(100*(i+1)), float64(i+1))
		require.NoError(t, e.Flush(ctx))
	}

	parts := e.PartPrefixes()
	require.Len(t, parts, n)

	return parts
}

// TestRepairFetchesWantedPartFromPeer is the plain repair: the exact part comes back from a peer,
// is readable again, and its want is discharged by the commit that publishes it.
func TestRepairFetchesWantedPartFromPeer(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	be, peer := backend.Memory(), backend.Memory()

	f := &fakeFetcher{}
	e := newRepairEngine(t, be, f)

	parts := flushSamples(t, e, 2)
	lost := parts[0]

	copyObjects(t, be, peer, lost)
	dropObjects(t, be, lost)
	e.LosePart(lost, bucketindex.Interval{Min: 1, Max: 1})

	f.answer = func(w bucketindex.Want) (bucketindex.Entry, bucketindex.WantOutcome, error) {
		copyObjects(t, peer, be, w.Prefix)

		return bucketindex.Entry{Prefix: w.Prefix, MinTime: 100, MaxTime: 100, Blocks: w.Blocks}, bucketindex.WantSatisfied, nil
	}

	require.NoError(t, e.Merge(ctx, 0))

	assert.Equal(t, []string{lost}, f.asks())
	assert.Empty(t, e.WantPrefixes(), "committing the part is what discharges the want")
	assert.Equal(t, int64(1), e.RepairStats().Fetched)

	ts, vals := seriesSamples(t, e)
	assert.Equal(t, []int64{100, 200}, ts)
	assert.Equal(t, []float64{1, 2}, vals)
	assert.Empty(t, metricsIndex(t, be).Wanted)
}

// TestRepairDischargedByContainingSuccessor is the property that makes repair terminate: the
// wanted part exists nowhere any more, only inside a merged successor, and accepting that
// successor both discharges the want and retires the local parts it contains.
func TestRepairDischargedByContainingSuccessor(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	be, peerBE := backend.Memory(), backend.Memory()

	f := &fakeFetcher{}
	e := newRepairEngine(t, be, f)

	parts := flushSamples(t, e, 2)
	lost, kept := parts[0], parts[1]

	e.SetPartBlocks(lost, bucketindex.Interval{Min: 1, Max: 1}, 0)
	e.SetPartBlocks(kept, bucketindex.Interval{Min: 2, Max: 2}, 0)

	dropObjects(t, be, lost)
	e.LosePart(lost, bucketindex.Interval{Min: 1, Max: 1})

	// The peer merged both blocks away into one level-1 part; the wanted prefix is gone there too.
	peer := newRepairEngine(t, peerBE, nil)
	s := mkSeries("job", "api")
	mustAppend(t, peer, s, 100, 1)
	mustAppend(t, peer, s, 200, 2)
	require.NoError(t, peer.Flush(ctx))

	peerParts := peer.PartPrefixes()
	require.Len(t, peerParts, 1)
	successor := peerParts[0]

	f.answer = func(bucketindex.Want) (bucketindex.Entry, bucketindex.WantOutcome, error) {
		copyObjects(t, peerBE, be, successor)

		return bucketindex.Entry{
			Prefix: successor, MinTime: 100, MaxTime: 200,
			Blocks: bucketindex.Interval{Min: 1, Max: 2}, Level: 1,
		}, bucketindex.WantSatisfied, nil
	}

	require.NoError(t, e.Merge(ctx, 0))

	assert.Equal(t, []string{lost}, f.asks(), "repair asks for the want it holds")
	assert.Empty(t, e.WantPrefixes(), "a containing successor discharges the want")

	local, err := be.List(ctx, lost)
	require.NoError(t, err)
	assert.Empty(t, local, "the vanished prefix is never fetched — only the successor containing it")

	assert.Equal(t, []string{successor}, e.PartPrefixes(),
		"the superseded local part is retired, not kept alongside the part containing it")

	ts, vals := seriesSamples(t, e)
	assert.Equal(t, []int64{100, 200}, ts, "accepting a containing part must not double-count its rows")
	assert.Equal(t, []float64{1, 2}, vals)
}

// TestRepairDischargedByLocalPart verifies the check that runs before the network: a want this
// node's own index already covers costs no fetch at all.
func TestRepairDischargedByLocalPart(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	be := backend.Memory()

	f := &fakeFetcher{}
	e := newRepairEngine(t, be, f)

	parts := flushSamples(t, e, 2)
	e.SetPartBlocks(parts[1], bucketindex.Interval{Min: 1, Max: 4}, 1)

	dropObjects(t, be, parts[0])
	e.LosePart(parts[0], bucketindex.Interval{Min: 1, Max: 1})

	require.NoError(t, e.Merge(ctx, 0))

	assert.Empty(t, f.asks(), "no network call for a want the local index covers")
	assert.Empty(t, e.WantPrefixes())
	assert.Equal(t, int64(1), e.RepairStats().Local)
}

// TestRepairNoPeerLeavesWant verifies definitive absence: the obligation survives, is counted, and
// the merge that carried the repair still compacts.
func TestRepairNoPeerLeavesWant(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	be := backend.Memory()

	f := &fakeFetcher{answer: func(bucketindex.Want) (bucketindex.Entry, bucketindex.WantOutcome, error) {
		return bucketindex.Entry{}, bucketindex.WantAbsent, nil
	}}
	e := newRepairEngine(t, be, f)

	lost := loseOneOfThree(t, e, be)

	require.NoError(t, e.Merge(ctx, 0), "a repair that cannot proceed must not block compaction")

	assert.Equal(t, []string{lost}, e.WantPrefixes())
	assert.Equal(t, int64(1), e.RepairStats().Unsatisfiable)
	assert.Less(t, len(e.PartPrefixes()), 2, "the merge still compacted the parts that are here")

	wanted := metricsIndex(t, be).Wanted
	require.Len(t, wanted, 1)
	assert.Equal(t, lost, wanted[0].Prefix, "the obligation is durable, not only in memory")
}

// TestRepairTransientFailureKeepsWant verifies the permanent-vs-transient discipline: an
// unreachable peer leaves the want intact and the index consistent, and the next cycle retries.
func TestRepairTransientFailureKeepsWant(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	be := backend.Memory()

	f := &fakeFetcher{answer: func(bucketindex.Want) (bucketindex.Entry, bucketindex.WantOutcome, error) {
		return bucketindex.Entry{}, bucketindex.WantIncomplete, errors.New("peer unreachable")
	}}
	e := newRepairEngine(t, be, f)

	lost := loseOneOfThree(t, e, be)

	require.NoError(t, e.Merge(ctx, 0))

	assert.Equal(t, []string{lost}, e.WantPrefixes())
	assert.Equal(t, int64(1), e.RepairStats().Failed)
	assert.Zero(t, e.RepairStats().Unsatisfiable, "a peer we could not ask is not evidence of absence")

	r := newRepairEngine(t, be, nil)
	require.NoError(t, r.LoadParts(ctx), "the index must still name only parts that are here")

	require.NoError(t, e.Merge(ctx, 0))
	assert.Equal(t, []string{lost}, e.WantPrefixes(), "the obligation is retried, not forgotten")
	assert.Equal(t, int64(2), e.RepairStats().Failed)
}

// TestRepairWithoutCallbackIsNoOp verifies single-node mode: with no cluster to pull from, a merge
// runs exactly as it did before and the want is left alone.
func TestRepairWithoutCallbackIsNoOp(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	be := backend.Memory()
	e := newRepairEngine(t, be, nil)

	lost := loseOneOfThree(t, e, be)

	require.NoError(t, e.Merge(ctx, 0))

	assert.Equal(t, []string{lost}, e.WantPrefixes())
	assert.Equal(t, engine.RepairStats{}, e.RepairStats())
}

// loseOneOfThree flushes three parts, destroys one of them and records its want, returning the
// lost prefix. Two parts survive so the merge the repair rides on still has work to do.
func loseOneOfThree(t *testing.T, e *engine.Engine, be backend.Backend) string {
	t.Helper()

	parts := flushSamples(t, e, 3)
	lost := parts[0]

	dropObjects(t, be, lost)
	e.LosePart(lost, bucketindex.Interval{Min: 1, Max: 1})

	require.True(t, slices.Contains(e.WantPrefixes(), lost))

	return lost
}

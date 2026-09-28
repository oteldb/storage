package engine_test

import (
	"context"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/go-faster/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/backend/bucketindex"
	"github.com/oteldb/storage/engine"
	"github.com/oteldb/storage/internal/reproduce"
	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/signal"
)

// These cover what a *rebase* leaves behind. A commit that loses the conditional write reloads the
// winner's index and retries on top of it: it must keep the winner's entries without stamping its
// own WAL watermark over the winner's (#397), and must serve the parts it thereby names (#398).

// rivals returns two engines over one shared backend, each with its own node identity and each
// holding the state the other's commits will move under it.
func rivals(t *testing.T, be backend.Backend) (a, b *engine.Engine, aID, bID string) {
	t.Helper()

	a, aID = newSharedWriter(t, be)
	b, bID = newSharedWriter(t, be)

	return a, b, aID, bID
}

// committedIndex reads the index the shared prefix currently holds.
func committedIndex(t *testing.T, be backend.Backend) *bucketindex.Index {
	t.Helper()

	ix, err := bucketindex.Load(context.Background(), be, sharedPrefix+"/"+bucketindex.Object)
	require.NoError(t, err)

	return ix
}

// TestRebaseDoesNotLowerTheFlushWatermark is the reproducer for #397. FlushedEpoch is a per-node
// WAL generation, but the index holding it is shared, so a rebasing writer stamps its own counter
// over one that means something else. Here the loser's counter is the lower of the two, and the
// watermark a later recovery reads goes backwards — which is a replay of records the parts already
// hold.
//
// Non-regression is asserted rather than any particular value: it is a necessary condition of
// every fix in #397 (per-writer slots keep each writer's own; a globally comparable epoch is
// monotone), while the value itself depends on which is chosen. The watermark is read per writer
// because that is the design that won — a scalar in a shared object has no owner.
func TestRebaseDoesNotLowerTheFlushWatermark(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	be := backend.Memory()
	a, b, aID, bID := rivals(t, be)

	s := mkSeries("job", "api")
	for i := range 3 {
		mustAppend(t, a, s, int64(100+i), float64(i))
		require.NoError(t, a.Flush(ctx))
	}

	committed := committedIndex(t, be).WriterEpoch(aID)
	require.EqualValues(t, 3, committed, "three flushes advanced the watermark")

	// b has flushed once, so its own watermark is 1. Its commit loses and rebases onto a's.
	mustAppend(t, b, mkSeries("job", "web"), 200, 2.0)
	require.NoError(t, b.Flush(ctx))

	after := committedIndex(t, be)
	require.GreaterOrEqual(t, after.WriterEpoch(aID), committed,
		"a rebased commit must not lower the flush watermark a previous commit recorded")
	require.EqualValues(t, 1, after.WriterEpoch(bID), "and records its own under its own name")
}

// TestRebaseServesTheAdoptedParts is the reproducer for #398. A rebase carries the rival's entries
// into the index but never opens them, so this engine publishes a part set it will not serve until
// its next LoadParts. Queried directly — an embedder, a single-replica read, or a completeness
// check that trusts the local index — the rival's rows are missing while the index says they are
// there.
func TestRebaseServesTheAdoptedParts(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	be := backend.Memory()
	a, b, _, _ := rivals(t, be)

	mustAppend(t, a, mkSeries("job", "api"), 100, 1.0)
	require.NoError(t, a.Flush(ctx))

	// b's commit loses to a's and rebases, adopting a's entry.
	mustAppend(t, b, mkSeries("job", "web"), 200, 2.0)
	require.NoError(t, b.Flush(ctx))

	require.Len(t, committedIndex(t, be).Entries, 2, "the rebased commit names both writers' parts")

	got := fetchAll(t, b, fetch.Request{
		Start:    0,
		End:      10000,
		Matchers: []fetch.Matcher{eqMatcher("job", "api")},
	})

	require.Len(t, got, 1, "an engine must serve every part the index it committed names")
	require.Equal(t, []int64{100}, got[0].Timestamps)
}

// TestRebaseNeverCommitsTwoAggs: writer b plans a Sum rollup of its own part while writer a, sharing
// the prefix, commits a Count rollup b has not seen. b's commit loses the CAS and rebases onto a's
// index; committing its Sum output beside a's Count part would put two Aggs in the index, which no
// merge can fold. The committed index must never hold both.
func TestRebaseNeverCommitsTwoAggs(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	be := backend.Memory()
	a, b, _, _ := rivals(t, be)

	tier := func(agg signal.Aggregation) engine.MergeOptions {
		return engine.MergeOptions{Downsample: []engine.DownsampleTier{{Before: 1 << 62, Interval: 1000, Agg: agg}}}
	}

	for i := range int64(5) {
		mustAppend(t, a, mkSeries("job", "api"), 100+i, 1)
		mustAppend(t, b, mkSeries("job", "web"), 100+i, 1)
	}

	require.NoError(t, a.Flush(ctx))
	require.NoError(t, b.Flush(ctx))
	require.NoError(t, a.MergeWith(ctx, tier(signal.AggCount)))
	require.NoError(t, b.MergeWith(ctx, tier(signal.AggSum)))

	r := engine.New(engine.Config{Backend: be, Prefix: sharedPrefix})
	require.NoError(t, r.LoadParts(ctx))
	require.Len(t, r.RecordedAggs(), 1, "the committed index holds one Agg")

	require.NoError(t, b.MergeWith(ctx, tier(signal.AggSum)))
	require.NoError(t, r.LoadParts(ctx))
	require.Len(t, r.RecordedAggs(), 1, "and keeps holding one on the next merge")

	require.ElementsMatch(t, []string{"api", "web"}, queryable(t, be, "api", "web"), "nothing is lost")
}

// TestAdoptedDataClosesOwnedBucket: writer a's last parts sit in one day, one per 6h bucket, so only
// the day level can merge them, and the day is a's newest while a writes nothing newer. Writer b
// keeps ingesting days later, and a adopts b's parts on its next commit. The day is then no longer
// the newest of the data a serves, so both a's merge and its merge shape must take the run.
func TestAdoptedDataClosesOwnedBucket(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	be := backend.Memory()
	a, b, _, _ := rivals(t, be)

	hour, day := int64(time.Hour), 24*int64(time.Hour)
	base := 10 * day

	for _, ts := range []int64{base + hour, base + 7*hour, base + 13*hour} {
		mustAppend(t, a, mkSeries("job", "api"), ts, 1)
		require.NoError(t, a.Flush(ctx))
	}

	mustAppend(t, b, mkSeries("job", "web"), base+3*day, 1)
	require.NoError(t, b.Flush(ctx))

	// A late sample in the same day: a's commit loses the CAS and adopts b's part.
	mustAppend(t, a, mkSeries("job", "api"), base+19*hour, 1)
	require.NoError(t, a.Flush(ctx))
	require.Equal(t, 4, a.PartCount())

	assert.Equal(t, 4, a.MergeShapeWith(engine.MergeOptions{}).Candidates, "the shape takes the day's run")

	require.NoError(t, a.MergeWith(ctx, engine.MergeOptions{}))
	assert.Equal(t, 1, a.PartCount(), "the merge takes it")
}

// TestNonNestingGridsStayApart: two writers roll one Agg on grids that do not nest, 7m Sum and 1h
// Sum, and a store ends up holding both. The rebase guard keeps one engine from committing that,
// so the second writer's entries are grafted into the first's index here, as a writer that never
// saw the first would have committed them. After a restart a fresh engine owns every part. Merging
// the 7m representatives with the 1h ones would coarsen them into hours by their timestamps, moving
// the minutes of a 7m bucket that straddles an hour into the wrong hour. Forced merges must leave
// the 7m series exactly as it read before them, and still compact each grid's parts.
func TestNonNestingGridsStayApart(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	be := backend.Memory()

	const otherPrefix = "other/metrics"

	a := engine.New(engine.Config{Backend: be, Prefix: sharedPrefix, WriterID: "a"})
	require.NoError(t, a.LoadParts(ctx))
	b := engine.New(engine.Config{Backend: be, Prefix: otherPrefix, WriterID: "b"})
	require.NoError(t, b.LoadParts(ctx))

	minute, hour, day := int64(time.Minute), int64(time.Hour), 24*int64(time.Hour)
	tier := func(interval int64) engine.MergeOptions {
		return engine.MergeOptions{Downsample: []engine.DownsampleTier{{Before: 1 << 62, Interval: interval, Agg: signal.AggSum}}}
	}

	roll := func(e *engine.Engine, job string, from int64, interval int64) {
		for h := from; h < from+2; h++ {
			for m := range int64(60) {
				mustAppend(t, e, mkSeries("job", job), h*hour+m*minute, 1)
			}

			require.NoError(t, e.Flush(ctx))
			require.NoError(t, e.MergeWith(ctx, tier(interval)))
		}
	}

	roll(a, "api", 0, 7*minute)
	roll(b, "web", 2, hour)

	mustAppend(t, a, mkSeries("job", "api"), 5*day, 1)
	require.NoError(t, a.Flush(ctx))

	key := sharedPrefix + "/" + bucketindex.Object
	ix, version, err := bucketindex.LoadVersioned(ctx, be, key)
	require.NoError(t, err)

	other, err := bucketindex.Load(ctx, be, otherPrefix+"/"+bucketindex.Object)
	require.NoError(t, err)

	for _, ent := range other.Entries {
		ix.Add(ent)
	}

	_, err = ix.Save(ctx, be, key, version)
	require.NoError(t, err)

	api := fetch.Request{Start: 0, End: 1 << 62, Matchers: []fetch.Matcher{eqMatcher("job", "api")}}

	r := engine.New(engine.Config{Backend: be, Prefix: sharedPrefix})
	require.NoError(t, r.LoadParts(ctx))
	require.Equal(t, 5, r.PartCount(), "two parts per grid and the raw anchor")

	before := fetchAll(t, r, api)
	require.Len(t, before, 1)
	require.Contains(t, before[0].Timestamps, 56*minute, "a 7m bucket straddles the hour")

	for range 8 {
		require.NoError(t, r.MergeWith(ctx, engine.MergeOptions{Force: true}))
	}

	after := fetchAll(t, r, api)
	require.Len(t, after, 1)

	// The first two hours: the anchor, raw in the 7m cohort, is rolled by its recorded tier.
	n := slices.Index(before[0].Timestamps, 5*day)
	require.Positive(t, n)
	require.Greater(t, len(after[0].Timestamps), n)
	assert.Equal(t, before[0].Timestamps[:n], after[0].Timestamps[:n], "the 7m buckets keep their attribution")
	assert.Equal(t, before[0].Values[:n], after[0].Values[:n])
	assert.Equal(t, 3, r.PartCount(), "each grid compacts on its own, beside the anchor's day")
}

// TestRebaseNeverCommitsTwoGrids is [TestRebaseNeverCommitsTwoAggs] for one Agg on grids that do not
// nest: writer a commits a 7m Sum rollup that b has not seen when it rolls its own part with 1h Sum.
// b's commit rebases onto a's index and must not land beside it.
func TestRebaseNeverCommitsTwoGrids(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	be := backend.Memory()
	a, b, _, _ := rivals(t, be)

	minute, hour := int64(time.Minute), int64(time.Hour)
	tier := func(interval int64) engine.MergeOptions {
		return engine.MergeOptions{Downsample: []engine.DownsampleTier{{Before: 1 << 62, Interval: interval, Agg: signal.AggSum}}}
	}

	for m := range int64(60) {
		mustAppend(t, b, mkSeries("job", "web"), hour+m*minute, 1)
		mustAppend(t, a, mkSeries("job", "api"), m*minute, 1)
	}

	require.NoError(t, b.Flush(ctx))
	require.NoError(t, a.Flush(ctx))
	require.NoError(t, a.MergeWith(ctx, tier(7*minute)))
	require.NoError(t, b.MergeWith(ctx, tier(hour)))

	r := engine.New(engine.Config{Backend: be, Prefix: sharedPrefix})
	require.NoError(t, r.LoadParts(ctx))

	intervals := r.RecordedIntervals()
	for _, x := range intervals {
		for _, y := range intervals {
			require.True(t, x%y == 0 || y%x == 0, "recorded grids %v nest", intervals)
		}
	}

	for _, job := range []string{"api", "web"} {
		got := fetchAll(t, r, fetch.Request{Start: 0, End: 1 << 62, Matchers: []fetch.Matcher{eqMatcher("job", job)}})
		require.Len(t, got, 1, "nothing is lost: %s", job)
	}
}

// blindBackend fails reads of the manifests hide names, as a flaky backend or one lagging on a
// freshly written object would.
type blindBackend struct {
	backend.Backend

	mu   sync.Mutex
	hide map[string]bool
}

func (b *blindBackend) Read(ctx context.Context, key string) ([]byte, error) {
	b.mu.Lock()
	hidden := b.hide[key]
	b.mu.Unlock()

	if hidden {
		return nil, errors.New("injected read failure")
	}

	return b.Backend.Read(ctx, key)
}

func (b *blindBackend) setHidden(prefixes ...string) {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.hide = make(map[string]bool, len(prefixes))
	for _, p := range prefixes {
		b.hide[p+"/manifest"] = true
	}
}

// TestRebaseBlindToAdoptedPart: writer b rolls its part with Sum while writer a commits a Count rollup
// b has not seen, and b's backend cannot read that part's manifest for a while. b's commit loses the
// CAS and rebases onto a's index, but the part it adopts does not open, so the rebase guard never
// sees its layout and b commits its Sum output beside a's Count part. The committed index must never
// hold both; once the manifest reads again, b's rollup must still go through.
func TestRebaseBlindToAdoptedPart(t *testing.T) {
	reproduce.Unfixed(t, 745, "a rebase that cannot open an adopted part commits a rollup beside an incompatible one")
	t.Parallel()

	ctx := context.Background()
	shared := backend.Memory()
	blind := &blindBackend{Backend: shared}

	a := engine.New(engine.Config{Backend: shared, Prefix: sharedPrefix, WriterID: "a"})
	require.NoError(t, a.LoadParts(ctx))
	b := engine.New(engine.Config{Backend: blind, Prefix: sharedPrefix, WriterID: "b"})
	require.NoError(t, b.LoadParts(ctx))

	tier := func(agg signal.Aggregation) engine.MergeOptions {
		return engine.MergeOptions{Downsample: []engine.DownsampleTier{{Before: 1 << 62, Interval: 1000, Agg: agg}}}
	}

	for i := range int64(5) {
		mustAppend(t, a, mkSeries("job", "api"), 100+i, 1)
		mustAppend(t, b, mkSeries("job", "web"), 100+i, 1)
	}

	require.NoError(t, a.Flush(ctx))
	require.NoError(t, b.Flush(ctx))
	require.NoError(t, a.MergeWith(ctx, tier(signal.AggCount)))

	parts := a.Parts()

	rolled := make([]string, 0, len(parts))
	for _, p := range parts {
		rolled = append(rolled, p.ID)
	}

	blind.setHidden(rolled...)
	require.NoError(t, b.MergeWith(ctx, tier(signal.AggSum)))

	r := engine.New(engine.Config{Backend: shared, Prefix: sharedPrefix})
	require.NoError(t, r.LoadParts(ctx))
	require.Len(t, r.RecordedAggs(), 1, "the committed index holds one Agg while the adopted part is unreadable")

	blind.setHidden()
	require.NoError(t, b.MergeWith(ctx, tier(signal.AggSum)))
	require.NoError(t, r.LoadParts(ctx))
	require.Len(t, r.RecordedAggs(), 1, "and after it reads again")
	require.ElementsMatch(t, []string{"api", "web"}, queryable(t, shared, "api", "web"), "nothing is lost")
}

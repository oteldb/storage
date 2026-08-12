package engine_test

import (
	"context"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/engine"
	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/signal"
)

// churnSeries is one series of a churn-shaped set: a stable job with a per-series instance, the
// shape (instance-id-style resource attributes) that makes identity outlive its data.
func churnSeries(i int) signal.Series {
	return mkSeries("job", "api", "inst", strconv.Itoa(i))
}

// pruneEngine ingests `total` churned series at t=1, flushes, then re-ingests the last `keep` of
// them at t=1000 and flushes again — so a retention merge cutting at t=1000 leaves exactly `keep`
// identities backed by data.
func pruneEngine(t *testing.T, total, keep int) *engine.Engine {
	t.Helper()

	ctx := context.Background()
	e := engine.New(engine.Config{Backend: backend.Memory(), Prefix: "t/prune"})

	for i := range total {
		mustAppend(t, e, churnSeries(i), 1, float64(i))
	}

	require.NoError(t, e.Flush(ctx))

	for i := total - keep; i < total; i++ {
		mustAppend(t, e, churnSeries(i), 1000, float64(i))
	}

	require.NoError(t, e.Flush(ctx))

	return e
}

func TestPruneIdentitiesDropsDeadSeries(t *testing.T) {
	t.Parallel()

	const (
		total = pruneMinIdentitiesTest
		keep  = total / 8
	)

	ctx := context.Background()
	e := pruneEngine(t, total, keep)

	require.EqualValues(t, total, e.Stats().Series)
	before := e.Stats().IdentityBytes

	// Retention past the first flush's samples: the merge drops their rows, leaving the identities
	// of every series that did not re-ingest with no data behind them.
	require.NoError(t, e.MergeWith(ctx, engine.MergeOptions{RetainFrom: 500}))

	removed, err := e.PruneIdentities(ctx)
	require.NoError(t, err)
	assert.Equal(t, total-keep, removed)
	assert.EqualValues(t, keep, e.Stats().Series, "only the series with surviving rows remain")
	assert.Less(t, e.Stats().IdentityBytes, before/4, "the reclaimed memory is reported")

	// A pruned identity no longer resolves; a live one still does.
	dead := fetchAll(t, e, fetch.Request{
		Start: 0, End: 1 << 60,
		Matchers: []fetch.Matcher{eqMatcher("inst", "0")},
	})
	assert.Empty(t, dead, "a pruned series matches nothing")

	live := fetchAll(t, e, fetch.Request{
		Start: 0, End: 1 << 60,
		Matchers: []fetch.Matcher{eqMatcher("inst", strconv.Itoa(total-1))},
	})
	require.Len(t, live, 1, "a series with surviving rows is still queryable")
	assert.Equal(t, []int64{1000}, live[0].Timestamps)
}

func TestPruneIdentitiesKeepsIngestingSeries(t *testing.T) {
	t.Parallel()

	const (
		total = pruneMinIdentitiesTest
		keep  = 4
	)

	ctx := context.Background()
	e := engine.New(engine.Config{OOOWindow: 100, Backend: backend.Memory(), Prefix: "t/prune-live"})

	for i := range total {
		mustAppend(t, e, churnSeries(i), 1000, float64(i))
	}

	require.NoError(t, e.Flush(ctx))

	// The survivors keep ingesting into the *head* — unflushed, so only the in-memory tier proves
	// them live.
	for i := range keep {
		mustAppend(t, e, churnSeries(i), 2000, 1)
	}

	require.NoError(t, e.MergeWith(ctx, engine.MergeOptions{RetainFrom: 1500}))

	removed, err := e.PruneIdentities(ctx)
	require.NoError(t, err)
	assert.Equal(t, total-keep, removed)

	// The OOO watermark of a surviving series must survive with it: a sample older than the window
	// is still rejected, exactly as before the prune.
	ok, err := e.Append(churnSeries(0), 1000, 1)
	require.NoError(t, err)
	assert.False(t, ok, "a live series keeps its own lateness bound across a prune")

	ok, err = e.Append(churnSeries(0), 2001, 1)
	require.NoError(t, err)
	assert.True(t, ok, "an in-window sample is still admitted")
}

func TestPruneIdentitiesReIngestAfterPrune(t *testing.T) {
	t.Parallel()

	const (
		total = pruneMinIdentitiesTest
		keep  = 1
	)

	ctx := context.Background()
	e := pruneEngine(t, total, keep)

	require.NoError(t, e.MergeWith(ctx, engine.MergeOptions{RetainFrom: 500}))

	removed, err := e.PruneIdentities(ctx)
	require.NoError(t, err)
	require.Positive(t, removed)

	// A pruned series that starts ingesting again re-registers cleanly: it is a new identity to the
	// index, and its old lateness bound is gone with it.
	revived := churnSeries(0)
	mustAppend(t, e, revived, 2000, 42)

	got := fetchAll(t, e, fetch.Request{
		Start: 0, End: 1 << 60,
		Matchers: []fetch.Matcher{eqMatcher("inst", "0")},
	})
	require.Len(t, got, 1)
	assert.Equal(t, revived.Hash(), got[0].ID)
	assert.Equal(t, []float64{42}, got[0].Values)
	assert.Equal(t, revived, got[0].Series, "the re-registered identity is complete")
}

func TestPruneIdentitiesNoopWithoutRetention(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	e := pruneEngine(t, pruneMinIdentitiesTest, pruneMinIdentitiesTest)

	// A merge that drops no row arms nothing: every identity is still backed.
	require.NoError(t, e.MergeWith(ctx, engine.MergeOptions{}))

	removed, err := e.PruneIdentities(ctx)
	require.NoError(t, err)
	assert.Zero(t, removed)
	assert.EqualValues(t, pruneMinIdentitiesTest, e.Stats().Series)
}

func TestPruneIdentitiesSkipsSmallIndex(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	e := pruneEngine(t, 64, 1)

	require.NoError(t, e.MergeWith(ctx, engine.MergeOptions{RetainFrom: 500}))

	removed, err := e.PruneIdentities(ctx)
	require.NoError(t, err)
	assert.Zero(t, removed, "a small index is not worth a rebuild")
	assert.EqualValues(t, 64, e.Stats().Series)
}

func TestPruneIdentitiesHeadOnlyEngine(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	e := engine.New(engine.Config{})

	mustAppend(t, e, churnSeries(0), 1, 1)

	removed, err := e.PruneIdentities(ctx)
	require.NoError(t, err)
	assert.Zero(t, removed, "nothing is flushed, so no identity can have outlived its data")
	assert.EqualValues(t, 1, e.Stats().Series)
}

// pruneMinIdentitiesTest mirrors the engine's minimum-index gate: below it a prune is skipped, so
// the tests that expect one to run must exceed it.
const pruneMinIdentitiesTest = 1 << 13

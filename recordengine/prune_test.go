package recordengine_test

import (
	"context"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/recordengine"
	"github.com/oteldb/storage/wal"
)

// pruneMinIdentitiesTest mirrors the engine's minimum-index gate: below it a prune is skipped, so
// the tests that expect one to run must exceed it.
const pruneMinIdentitiesTest = 1 << 13

// pruneEngine ingests `total` churned streams at t=1, flushes, then re-ingests the last `keep` of
// them at t=1000 and flushes again — so retention cutting at t=1000 leaves exactly `keep` streams
// backed by data.
func pruneEngine(t *testing.T, total, keep int) *recordengine.Engine {
	t.Helper()

	ctx := context.Background()
	e := newEngine(t, backend.Memory())

	for i := range total {
		ingest(t, e, mkBatch("svc-"+strconv.Itoa(i), rrec{ts: 1, body: "old"}))
	}

	require.NoError(t, e.Flush(ctx))

	for i := total - keep; i < total; i++ {
		ingest(t, e, mkBatch("svc-"+strconv.Itoa(i), rrec{ts: 1000, body: "new"}))
	}

	require.NoError(t, e.Flush(ctx))

	return e
}

func TestPruneIdentitiesDropsDeadStreams(t *testing.T) {
	t.Parallel()

	const (
		total = pruneMinIdentitiesTest
		keep  = total / 8
	)

	ctx := context.Background()
	e := pruneEngine(t, total, keep)

	require.EqualValues(t, total, e.Stats().Streams)
	before := e.Stats().IdentityBytes

	require.NoError(t, e.Merge(ctx, 500)) // retention drops the first flush's records

	removed, err := e.PruneIdentities(ctx)
	require.NoError(t, err)
	assert.Equal(t, total-keep, removed)
	assert.EqualValues(t, keep, e.Stats().Streams, "only streams with surviving records remain")
	// Compared against an engine that only ever held the survivors, not against a fraction of `before`.
	// IdentityBytes counts the *capacity* of the interning maps, and those come from a pool — so a
	// pruned engine's floor depends on how large a map the pool happened to hand its rebuilt table,
	// which depends on what else ran in this process. Both sides here draw from the same pool, so the
	// comparison is like-for-like; a fraction of `before` is not (it failed CI at 31.6% of before,
	// where the same code measures 13.0% in a quiet process).
	fresh := pruneEngine(t, keep, keep).Stats().IdentityBytes

	assert.Less(t, e.Stats().IdentityBytes, before/2, "the reclaimed memory is reported")
	assert.Lessf(t, e.Stats().IdentityBytes, fresh*3/2,
		"a pruned engine holds about what an engine of %d streams holds (fresh %d)", keep, fresh)

	// A surviving stream still resolves, with its records; a pruned one matches nothing.
	live := fetchAll(t, e, req("svc-"+strconv.Itoa(total-1)))
	require.Len(t, live, 1)
	assert.Equal(t, []string{"new"}, bodies(live[0]))

	assert.Empty(t, fetchAll(t, e, req("svc-0")), "a pruned stream matches nothing")
}

func TestPruneIdentitiesKeepsIngestingStreams(t *testing.T) {
	t.Parallel()

	const (
		total = pruneMinIdentitiesTest
		keep  = 4
	)

	ctx := context.Background()
	e := newEngine(t, backend.Memory())

	for i := range total {
		ingest(t, e, mkBatch("svc-"+strconv.Itoa(i), rrec{ts: 1000, body: "old"}))
	}

	require.NoError(t, e.Flush(ctx))

	// The survivors keep ingesting into the *head* — unflushed, so only the in-memory tier proves
	// them live.
	for i := range keep {
		ingest(t, e, mkBatch("svc-"+strconv.Itoa(i), rrec{ts: 2000, body: "fresh"}))
	}

	require.NoError(t, e.Merge(ctx, 1500))

	removed, err := e.PruneIdentities(ctx)
	require.NoError(t, err)
	assert.Equal(t, total-keep, removed)

	got := fetchAll(t, e, req("svc-0"))
	require.Len(t, got, 1)
	assert.Equal(t, []string{"fresh"}, bodies(got[0]), "an unflushed stream survives the prune with its records")
}

func TestPruneIdentitiesReIngestAfterPrune(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	e := pruneEngine(t, pruneMinIdentitiesTest, 1)

	require.NoError(t, e.Merge(ctx, 500))

	removed, err := e.PruneIdentities(ctx)
	require.NoError(t, err)
	require.Positive(t, removed)

	// A pruned stream that starts ingesting again re-registers cleanly.
	ingest(t, e, mkBatch("svc-0", rrec{ts: 2000, body: "revived"}))

	got := fetchAll(t, e, req("svc-0"))
	require.Len(t, got, 1)
	assert.Equal(t, []string{"revived"}, bodies(got[0]))
}

func TestPruneIdentitiesNoopWithoutRetention(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	e := pruneEngine(t, pruneMinIdentitiesTest, pruneMinIdentitiesTest)

	require.NoError(t, e.Merge(ctx, 0)) // a merge that drops no record arms nothing

	removed, err := e.PruneIdentities(ctx)
	require.NoError(t, err)
	assert.Zero(t, removed)
	assert.EqualValues(t, pruneMinIdentitiesTest, e.Stats().Streams)
}

func TestPruneIdentitiesSkipsSmallIndex(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	e := pruneEngine(t, 64, 1)

	require.NoError(t, e.Merge(ctx, 500))

	removed, err := e.PruneIdentities(ctx)
	require.NoError(t, err)
	assert.Zero(t, removed, "a small index is not worth a rebuild")

	// ...but an operator-forced sweep runs regardless, and reports what it reclaimed.
	removed, err = e.PruneIdentitiesWith(ctx, recordengine.PruneOptions{Force: true})
	require.NoError(t, err)
	assert.Equal(t, 63, removed)
	assert.EqualValues(t, 1, e.Stats().Streams)
}

func TestPruneIdentitiesHeadOnlyEngine(t *testing.T) {
	t.Parallel()

	e := recordengine.New(recordengine.Config{Schema: testSchema})
	ingest(t, e, mkBatch("svc", rrec{ts: 1, body: "x"}))

	removed, err := e.PruneIdentities(context.Background())
	require.NoError(t, err)
	assert.Zero(t, removed, "nothing is flushed, so no identity can have outlived its data")
	assert.EqualValues(t, 1, e.Stats().Streams)
}

// TestPruneIdentitiesSurvivesRestart covers the WAL hazard: a pruned identity must never be one the
// log still needs. The log is checkpointed at every flush and re-logs a stream record whenever a
// stream starts a fresh buffer, so its live records always name streams it describes itself.
func TestPruneIdentitiesSurvivesRestart(t *testing.T) {
	t.Parallel()

	const (
		total = pruneMinIdentitiesTest
		keep  = 4
	)

	ctx := context.Background()
	dir := t.TempDir()
	be := backend.Memory()

	sw, err := wal.Create(dir, 0)
	require.NoError(t, err)

	src := recordengine.New(recordengine.Config{Schema: testSchema, Backend: be, Prefix: "t/recs", WAL: sw})

	for i := range total {
		ingest(t, src, mkBatch("svc-"+strconv.Itoa(i), rrec{ts: 1000, body: "old"}))
	}

	require.NoError(t, src.Flush(ctx)) // checkpoints the WAL

	for i := range keep {
		ingest(t, src, mkBatch("svc-"+strconv.Itoa(i), rrec{ts: 2000, body: "unflushed"}))
	}

	require.NoError(t, src.Merge(ctx, 1500))

	removed, perr := src.PruneIdentities(ctx)
	require.NoError(t, perr)
	require.Equal(t, total-keep, removed)
	require.NoError(t, sw.Close())

	restored := recordengine.New(recordengine.Config{Schema: testSchema, Backend: be, Prefix: "t/recs"})
	require.NoError(t, restored.LoadParts(ctx))
	require.NoError(t, restored.Replay(dir))

	assert.EqualValues(t, keep, restored.Stats().Streams, "recovery rebuilds the pruned set, not the old one")

	got := fetchAll(t, restored, req("svc-0"))
	require.Len(t, got, 1, "a survivor's unflushed records replay")
	assert.Equal(t, []string{"unflushed"}, bodies(got[0]))
}

package engine_test

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/backend/bucketindex"
	"github.com/oteldb/storage/engine"
)

func loadIndex(t *testing.T, be backend.Backend, prefix string) *bucketindex.Index {
	t.Helper()

	ix, err := bucketindex.Load(context.Background(), be, prefix+"/"+bucketindex.Object)
	require.NoError(t, err)

	return ix
}

// Every index write advances the generation, including one that removes a part without adding
// any — which is the rewrite neither the part names nor the flush epoch can express, and the
// reason the generation exists.
func TestIndexGenerationAdvancesOnEveryWrite(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	be := backend.Memory()
	e := engine.New(engine.Config{Backend: be, Prefix: "default/metrics"})

	mustAppend(t, e, mkSeries("job", "api"), 100, 1.0)
	require.NoError(t, e.Flush(ctx))

	first := loadIndex(t, be, "default/metrics").Generation
	require.False(t, first.Zero(), "a v3 writer stamps a generation")

	mustAppend(t, e, mkSeries("job", "api"), 200, 2.0)
	require.NoError(t, e.Flush(ctx))

	second := loadIndex(t, be, "default/metrics").Generation
	assert.Equal(t, 1, second.Compare(first), "the second write supersedes the first")
	assert.Equal(t, first.Term, second.Term, "the term did not move, so only the counter did")
}

// A reader restores the generation with the parts, so it continues the writer's sequence instead
// of restarting below it — a node that reopened a prefix must still be able to supersede the
// index it is reopening.
func TestIndexGenerationSurvivesReload(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	be := backend.Memory()
	cfg := engine.Config{Backend: be, Prefix: "default/metrics"}

	w := engine.New(cfg)
	mustAppend(t, w, mkSeries("job", "api"), 100, 1.0)
	require.NoError(t, w.Flush(ctx))
	before := loadIndex(t, be, "default/metrics").Generation

	r := engine.New(cfg)
	require.NoError(t, r.LoadParts(ctx))
	mustAppend(t, r, mkSeries("job", "api"), 200, 2.0)
	require.NoError(t, r.Flush(ctx))

	after := loadIndex(t, be, "default/metrics").Generation
	assert.Equal(t, 1, after.Compare(before), "the reopened engine continued the sequence")
}

// The term comes from the cluster and is read per write, not captured: a shard reclaimed under a
// new tenure supersedes everything the tenure before it wrote, however far that counter had run.
func TestIndexGenerationFollowsTheTerm(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	be := backend.Memory()

	var term atomic.Uint64
	term.Store(7)

	e := engine.New(engine.Config{
		Backend: be,
		Prefix:  "default/metrics",
		Term:    term.Load,
	})

	mustAppend(t, e, mkSeries("job", "api"), 100, 1.0)
	require.NoError(t, e.Flush(ctx))

	first := loadIndex(t, be, "default/metrics").Generation
	assert.EqualValues(t, 7, first.Term)

	term.Store(9) // the shard was handed away and taken back.

	mustAppend(t, e, mkSeries("job", "api"), 200, 2.0)
	require.NoError(t, e.Flush(ctx))

	second := loadIndex(t, be, "default/metrics").Generation
	assert.EqualValues(t, 9, second.Term)
	assert.EqualValues(t, 1, second.Counter, "a new tenure starts its own sequence")
	assert.Equal(t, 1, second.Compare(first))
}

// A merge states what it removed. That is the difference between a part a compaction consumed and
// one the node lost, which nothing else in the index expresses.
func TestMergeRecordsTombstones(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	be := backend.Memory()
	e := engine.New(engine.Config{Backend: be, Prefix: "default/metrics"})

	mustAppend(t, e, mkSeries("job", "api"), 100, 1.0)
	require.NoError(t, e.Flush(ctx))
	mustAppend(t, e, mkSeries("job", "api"), 200, 2.0)
	require.NoError(t, e.Flush(ctx))

	before := loadIndex(t, be, "default/metrics")
	require.Len(t, before.Entries, 2)
	require.Empty(t, before.Removed, "nothing has been removed yet")

	require.NoError(t, e.Merge(ctx, 0))

	after := loadIndex(t, be, "default/metrics")
	require.Len(t, after.Entries, 1, "the merge replaced both parts")

	removed := after.Removals()
	for _, entry := range before.Entries {
		assert.Contains(t, removed, entry.Prefix, "the merge said it removed the part it consumed")
	}

	assert.True(t, after.RecordsRemovals())
	assert.Equal(t, 1, after.Generation.Compare(before.Generation))
}

// A tombstone is dropped once the part it names is live again, so the index never states two
// contradictory things about one part.
func TestTombstoneClearedWhenThePartReturns(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	be := backend.Memory()
	cfg := engine.Config{Backend: be, Prefix: "default/metrics"}

	e := engine.New(cfg)
	mustAppend(t, e, mkSeries("job", "api"), 100, 1.0)
	require.NoError(t, e.Flush(ctx))
	mustAppend(t, e, mkSeries("job", "api"), 200, 2.0)
	require.NoError(t, e.Flush(ctx))
	require.NoError(t, e.Merge(ctx, 0))

	merged := loadIndex(t, be, "default/metrics")
	require.NotEmpty(t, merged.Removed)

	// A reader restores the tombstones with the parts, so they are carried forward rather than
	// forgotten by whoever writes next.
	r := engine.New(cfg)
	require.NoError(t, r.LoadParts(ctx))
	mustAppend(t, r, mkSeries("job", "api"), 300, 3.0)
	require.NoError(t, r.Flush(ctx))

	next := loadIndex(t, be, "default/metrics")
	assert.Equal(t, merged.Removals(), next.Removals(), "tombstones survive a reload and a write")
}

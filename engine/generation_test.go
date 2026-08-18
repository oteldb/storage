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

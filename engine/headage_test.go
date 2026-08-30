package engine_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/engine"
)

// TestHeadAgeTracksFlushLag pins the one wall-clock the head keeps: it starts when the head takes
// its first bytes and ends when a flush drains them, which is the flush lag no data-time field can
// report (a backfilled timestamp says nothing about how long the head has held it).
func TestHeadAgeTracksFlushLag(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	e := engine.New(engine.Config{Backend: backend.Memory(), Prefix: "t/headage"})

	require.Zero(t, e.Stats().HeadAge, "an empty head has no age")

	mustAppend(t, e, mkSeries("job", "api"), 1, 1)
	assert.Positive(t, e.Stats().HeadAge, "the head is accumulating")

	require.NoError(t, e.Flush(ctx))
	assert.Zero(t, e.Stats().HeadAge, "the flush drained it")

	mustAppend(t, e, mkSeries("job", "api"), 2, 2)
	assert.Positive(t, e.Stats().HeadAge, "the next head starts its own clock")
}

// TestMergeShapeReportsBytes covers the part-size gauge's input: a part count says nothing about
// whether the parts are small enough to be worth merging.
func TestMergeShapeReportsBytes(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	e := engine.New(engine.Config{Backend: backend.Memory(), Prefix: "t/shapebytes"})

	require.Zero(t, e.MergeShape().Bytes, "no parts, no bytes")

	mustAppend(t, e, mkSeries("job", "api"), 1, 1)
	require.NoError(t, e.Flush(ctx))

	sh := e.MergeShape()
	require.Equal(t, 1, sh.Parts)
	assert.Positive(t, sh.Bytes, "the flushed part reports what it occupies")
}

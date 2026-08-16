package engine_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/engine"
	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/wal"
)

// TestReplaySkipsSegmentsBelowFlushWatermark: a WAL segment survives the flush that superseded it
// whenever the flush happened somewhere else — in a cluster the shard's compaction owner writes the
// part, so a node that logged those same records never checkpoints them away. Recovery must still be
// exactly-once, which the flush watermark in the bucket index decides: segments at or below it hold
// nothing the loaded parts do not.
func TestReplaySkipsSegmentsBelowFlushWatermark(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	dir := t.TempDir()
	be := backend.Memory()
	api := mkSeries("job", "api")

	// This node logs the sample but never flushes it, so its segment stays on disk.
	sw, err := wal.Create(dir, 0)
	require.NoError(t, err)
	logger := engine.New(engine.Config{WAL: sw, Backend: be, Prefix: "t/epoch"})
	mustAppend(t, logger, api, 100, 1)
	require.NoError(t, sw.Close())

	// The shard's compaction owner flushes the same sample into a part, advancing the watermark.
	owner := engine.New(engine.Config{Backend: be, Prefix: "t/epoch"})
	mustAppend(t, owner, api, 100, 1)
	require.NoError(t, owner.Flush(ctx))

	restored := engine.New(engine.Config{Backend: be, Prefix: "t/epoch"})
	require.NoError(t, restored.LoadParts(ctx))
	require.NoError(t, restored.Replay(dir))

	assert.Zero(t, restored.HeadSampleCount(), "the superseded segment is skipped, not replayed into the head")

	got := fetchAll(t, restored, fetch.Request{Start: 0, End: 1000, Matchers: []fetch.Matcher{eqMatcher("job", "api")}})
	require.Len(t, got, 1)
	assert.Equal(t, []int64{100}, got[0].Timestamps, "the flushed sample is served once, from its part")
}

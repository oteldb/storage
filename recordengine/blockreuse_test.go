package recordengine_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/backend/bucketindex"
)

// TestBlockNumbersSurviveAnEmptiedShard pins that numbering never rewinds. Retention drops every
// part in the shard, leaving tombstones that carry no blocks, and the next flush must still number
// above what the shard has ever held.
//
// Identity has to be unique over a shard's whole life, not over its current contents: numbering
// derived from the live set hands a new part the identity an expired one had, at which point a
// stale peer's old part satisfies a want for the new one and expired data is committed as a repair.
func TestBlockNumbersSurviveAnEmptiedShard(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	be := backend.Memory()
	e := newEngine(t, be)

	flushIDs(ctx, t, e, be, 2)

	// Retention horizon past every record: both parts are dropped whole (tombstoned).
	require.NoError(t, e.Merge(ctx, 1<<40))

	ix := loadIndex(t, be)
	require.Empty(t, ix.Entries)
	require.EqualValues(t, 3, ix.NextBlock(), "the high-water mark outlives every part it numbered")

	ingest(t, e, mkBatch("api", rrec{ts: 1 << 41, body: "after"}))
	require.NoError(t, e.Flush(ctx))

	ix = loadIndex(t, be)
	require.Len(t, ix.Entries, 1)
	require.Equal(t, bucketindex.Interval{Min: 3, Max: 3}, ix.Entries[0].Blocks,
		"block numbers are never reused within a shard")
}

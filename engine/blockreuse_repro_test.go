package engine_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/backend/bucketindex"
)

func TestReproBlockNumbersReusedAfterRetentionEmptiesShard(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	be := backend.Memory()
	e := newLostEngine(be)

	ids := flushIDs(ctx, t, e, be, mkSeries("job", "api"), 2)
	ix := loadIndex(t, be, lostPrefix)
	t.Logf("before retention: %d entries, blocks %v %v", len(ix.Entries), ix.Entries[0].Blocks, ix.Entries[1].Blocks)
	_ = ids

	// Retention horizon past every sample: both parts are dropped whole (tombstoned).
	require.NoError(t, e.Merge(ctx, 1<<40))
	ix = loadIndex(t, be, lostPrefix)
	require.Empty(t, ix.Entries)
	t.Logf("after retention: %d removals, NextBlock=%d", len(ix.Removed), ix.NextBlock())

	// A new flush.
	mustAppend(t, e, mkSeries("job", "api"), 1<<41, 1)
	require.NoError(t, e.Flush(ctx))
	ix = loadIndex(t, be, lostPrefix)
	require.Len(t, ix.Entries, 1)
	t.Logf("new part %s has blocks %v", ix.Entries[0].Prefix, ix.Entries[0].Blocks)
	require.Equal(t, bucketindex.Interval{Min: 3, Max: 3}, ix.Entries[0].Blocks,
		"DESIGN EXPECTATION: block numbers are never reused within a shard")
}

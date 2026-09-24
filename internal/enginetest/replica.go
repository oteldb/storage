package enginetest

import (
	"context"
	"path"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/backend/bucketindex"
)

// stashPart removes a part's objects from the backend and returns them for restorePart.
func stashPart(t *testing.T, be backend.Backend, prefix string) map[string][]byte {
	t.Helper()
	ctx := context.Background()

	keys, err := be.List(ctx, prefix)
	require.NoError(t, err)
	require.NotEmpty(t, keys)

	saved := make(map[string][]byte, len(keys))

	for _, key := range keys {
		data, err := be.Read(ctx, key)
		require.NoError(t, err)

		saved[key] = data
		require.NoError(t, be.Delete(ctx, key))
	}

	return saved
}

func restorePart(t *testing.T, be backend.Backend, saved map[string][]byte) {
	t.Helper()

	for key, data := range saved {
		require.NoError(t, be.Write(context.Background(), key, data))
	}
}

// refreshReplicaGonePartBecomesPendingWant: a replica whose refresh finds a part's objects gone drops
// the handle and counts a want instead of failing and keeping the stale handle, and never commits
// the index, which is the owner's to write.
func refreshReplicaGonePartBecomesPendingWant(t *testing.T, k Kind) {
	t.Helper()

	ctx := context.Background()
	be := backend.Memory()
	k.flushEach(t, k.open(t, be), be, api(100, 1), Row{Stream: "web", Ts: 100000, Val: 2})

	replica := k.open(t, be)
	require.NoError(t, replica.RefreshReplica(ctx))
	require.Equal(t, 2, replica.PartCount())

	gone := replica.Parts()[0]
	indexKey := path.Dir(gone.ID) + "/" + bucketindex.Object

	indexBefore, err := be.Read(ctx, indexKey)
	require.NoError(t, err)

	saved := stashPart(t, be, gone.ID)

	require.NoError(t, replica.RefreshReplica(ctx))
	assert.Equal(t, 1, replica.PartCount(), "the gone part is no longer served as present")
	assert.Equal(t, 1, replica.Stats().WantedParts)
	assert.True(t, replica.WantOverlaps(gone.MinTime, gone.MaxTime))

	indexAfter, err := be.Read(ctx, indexKey)
	require.NoError(t, err)
	assert.Equal(t, indexBefore, indexAfter, "a replica records the want in memory only")

	restorePart(t, be, saved)

	require.NoError(t, replica.RefreshReplica(ctx))
	assert.Equal(t, 2, replica.PartCount())
	assert.Zero(t, replica.Stats().WantedParts, "the part coming back discharges the want")
	assert.False(t, replica.WantOverlaps(gone.MinTime, gone.MaxTime))
}

// blockNumbersSurviveAnEmptiedShard pins that numbering never rewinds. Retention drops every part in
// the shard, leaving tombstones that carry no blocks, and the next flush must still number above
// what the shard has ever held.
//
// Identity has to be unique over a shard's whole life, not its current contents: numbering derived
// from the live set hands a new part the identity an expired one had, at which point a stale peer's
// old part satisfies a want for the new one and expired data is committed as a repair.
func blockNumbersSurviveAnEmptiedShard(t *testing.T, k Kind) {
	t.Helper()

	ctx := context.Background()
	be := backend.Memory()
	e := k.open(t, be)

	k.flushEach(t, e, be, api(100, 1), api(200, 2))

	// Retention horizon past every row: both parts are dropped whole (tombstoned).
	require.NoError(t, e.Merge(ctx, 1<<40))

	ix := k.loadIndex(t, be)
	require.Empty(t, ix.Entries)
	require.EqualValues(t, 3, ix.NextBlock(), "the high-water mark outlives every part it numbered")

	e.Append(t, api(1<<41, 1))
	require.NoError(t, e.Flush(ctx))

	ix = k.loadIndex(t, be)
	require.Len(t, ix.Entries, 1)
	require.Equal(t, bucketindex.Interval{Min: 3, Max: 3}, ix.Entries[0].Blocks, "block numbers are never reused within a shard")
}

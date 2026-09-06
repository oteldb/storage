package engine_test

import (
	"context"
	"path"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/backend/bucketindex"
	"github.com/oteldb/storage/engine"
)

// stashPart removes a part's objects from the backend and returns them for restorePart.
func stashPart(t *testing.T, be backend.Backend, prefix string) map[string][]byte {
	t.Helper()
	ctx := context.Background()

	keys, err := be.List(ctx, prefix)
	require.NoError(t, err)
	require.NotEmpty(t, keys)

	saved := make(map[string][]byte, len(keys))

	for _, k := range keys {
		data, err := be.Read(ctx, k)
		require.NoError(t, err)

		saved[k] = data
		require.NoError(t, be.Delete(ctx, k))
	}

	return saved
}

func restorePart(t *testing.T, be backend.Backend, saved map[string][]byte) {
	t.Helper()

	for k, data := range saved {
		require.NoError(t, be.Write(context.Background(), k, data))
	}
}

// TestRefreshReplicaGonePartBecomesPendingWant: a replica whose refresh finds a part's objects gone
// drops the handle and counts a want instead of failing and keeping the stale handle, and never
// commits the index, which is the owner's to write.
func TestRefreshReplicaGonePartBecomesPendingWant(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	be := backend.Memory()
	flushedApartFromEachOther(t, be)

	replica := engine.New(engine.Config{Backend: be, Prefix: replicaPrefix})
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

package partsync_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/backend/bucketindex"
	"github.com/oteldb/storage/cluster/partsync"
)

// TestSyncKeepsTheLocalLineage: installing a newer peer's index keeps the groups only this node's
// catalog recorded. Lineage is facts about groups committed anywhere, and a want for a member of one
// may still need it.
func TestSyncKeepsTheLocalLineage(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	owner, replica := backend.Memory(), backend.Memory()

	mine := bucketindex.Claim{Blocks: bucketindex.Blocks(1, 2), Group: bucketindex.Blocks(10, 11)}
	theirs := bucketindex.Claim{Blocks: bucketindex.Blocks(3), Group: bucketindex.Blocks(20, 21)}

	lix := &bucketindex.Index{Catalog: []bucketindex.Claim{mine}}
	saveIndex(t, replica, "default/metrics", lix)

	pix := &bucketindex.Index{
		Generation: bucketindex.Generation{Term: 9, Counter: 1},
		Catalog:    []bucketindex.Claim{theirs},
	}
	writePart(t, owner, pix, "default/metrics", 1, 100, 200)
	saveIndex(t, owner, "default/metrics", pix)

	s := partsync.New(replica, &partsync.Client{})
	st, err := s.Sync(ctx, "default/metrics", []string{serve(t, owner)}, false, nil)
	require.NoError(t, err)
	require.True(t, st.Synced)

	got, err := bucketindex.Load(ctx, replica, "default/metrics/"+bucketindex.Object)
	require.NoError(t, err)
	assert.Equal(t, []bucketindex.Claim{mine, theirs}, got.Catalog)
	assert.Len(t, got.Entries, 1, "the peer's parts are installed")
}

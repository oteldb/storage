package recordengine_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/backend/bucketindex"
)

// TestFlushRetriesOntoAPeersCommit is the engine half of the shared-store commit: another writer
// over the same prefix committed since this engine last saw the index, so the flush's claim is lost.
// It must reload and rebuild — reporting success while unlinking the peer's part would leave that
// part durable and unreachable, and the next open's orphan sweep would delete it (#392).
func TestFlushRetriesOntoAPeersCommit(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	be := backend.Memory()
	key := "t/recs/" + bucketindex.Object

	e := newEngine(t, be)
	ingest(t, e, mkBatch("api", rrec{ts: 100, body: "p1"}))
	require.NoError(t, e.Flush(ctx))

	peer, err := bucketindex.Load(ctx, be, key)
	require.NoError(t, err)
	peer.Add(bucketindex.Entry{Prefix: "t/recs/0000009999", MinTime: 1, MaxTime: 2})
	require.NoError(t, peer.Save(ctx, be, key))

	ingest(t, e, mkBatch("api", rrec{ts: 200, body: "p2"}))
	require.NoError(t, e.Flush(ctx), "a lost claim is retried, not returned as a flush failure")

	got, err := bucketindex.Load(ctx, be, key)
	require.NoError(t, err)

	prefixes := make([]string, 0, len(got.Entries))
	for _, ent := range got.Entries {
		prefixes = append(prefixes, ent.Prefix)
	}

	assert.Contains(t, prefixes, "t/recs/0000009999", "the peer's part must survive this engine's commit")
	assert.Contains(t, prefixes, "t/recs/0000000000")
	assert.Contains(t, prefixes, "t/recs/0000000001")
}

// TestFlushFailsWhenTheClaimNeverLands guards the other direction: a commit that cannot be made
// must not be reported as a flush that happened.
func TestFlushFailsWhenTheClaimNeverLands(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	e := newEngine(t, neverClaims{backend.Memory()})

	ingest(t, e, mkBatch("api", rrec{ts: 100, body: "p1"}))
	require.ErrorIs(t, e.Flush(ctx), bucketindex.ErrConflict)
}

// neverClaims is a backend whose claims never succeed, as they would not for a writer starved by
// peers committing faster than it can rebuild.
type neverClaims struct{ backend.Backend }

func (neverClaims) PutIfAbsent(context.Context, string, []byte) (bool, error) { return false, nil }

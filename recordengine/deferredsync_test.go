package recordengine_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/backend/backendtest"
	"github.com/oteldb/storage/backend/bucketindex"
)

// syncedCommits fails a test whose index commit names a part with deferred objects no SyncPrefix
// has covered: a power cut after that commit could take the part the index promises.
type syncedCommits struct {
	*backendtest.Deferred

	t       *testing.T
	key     string
	checked int
}

func (c *syncedCommits) CompareAndSwap(
	ctx context.Context, key string, expected backend.Version, data []byte,
) (backend.Version, bool, error) {
	if key == c.key {
		next, err := bucketindex.Decode(data)
		require.NoError(c.t, err)

		for i := range next.Entries {
			if next.Entries[i].Hole {
				continue
			}

			assert.Empty(c.t, c.Pending(next.Entries[i].Prefix), "index names part %q before it is synced", next.Entries[i].Prefix)
			c.checked++
		}
	}

	return c.Deferred.CompareAndSwap(ctx, key, expected, data)
}

func TestPartsSyncedBeforeIndexCommit(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	be := &syncedCommits{Deferred: backendtest.WithDeferred(backend.Memory()), t: t, key: indexKey()}
	e := newEngine(t, be)

	for i := range 3 {
		ingest(t, e, mkBatch("api", rrec{ts: int64(100 * (i + 1)), body: "r", attr: [2]string{"k", "v"}}))
		require.NoError(t, e.Flush(ctx))
	}

	require.NoError(t, e.Merge(ctx, 0))

	assert.Positive(t, be.checked)
}

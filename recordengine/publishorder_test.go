package recordengine_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/backend/faultbackend"
	"github.com/oteldb/storage/recordengine"
	"github.com/oteldb/storage/wal"
)

// TestPublishWritesIdentityBeforeCommit verifies the publish ordering that keeps a committed part's
// rows reachable: the part's identity object must be durable before the bucket index, which carries
// the flush watermark and is therefore the commit point. Here the identity write fails, so the
// flush must not commit — otherwise the watermark would make replay skip the records while the
// identities naming them are missing, leaving the rows unreachable forever.
func TestPublishWritesIdentityBeforeCommit(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	be := faultbackend.Wrap(backend.Memory())
	walDir := t.TempDir()

	w, err := wal.Create(walDir, 0)
	require.NoError(t, err)
	t.Cleanup(func() { _ = w.Close() })

	e := recordengine.New(recordengine.Config{Schema: testSchema, Backend: be, Prefix: "t/recs", WAL: w})

	ingest(t, e, mkBatch("api", rrec{ts: 100, body: "stranded"}))
	require.NoError(t, w.Sync())

	rejectWrites(be, "/identity", errWriteRejected)
	require.Error(t, e.Flush(ctx), "flush must fail while the part's identities cannot be written")
	be.Reset()

	// Restart: recover the part set and watermark from the bucket index, then replay the WAL.
	w2, err := wal.Create(walDir, 0)
	require.NoError(t, err)
	t.Cleanup(func() { _ = w2.Close() })

	r := recordengine.New(recordengine.Config{Schema: testSchema, Backend: be, Prefix: "t/recs", WAL: w2})
	require.NoError(t, r.LoadParts(ctx))
	require.NoError(t, r.Replay(t.Context(), walDir))

	require.Equal(t, []string{"stranded"}, streamBodies(t, r),
		"rows must stay reachable after a crash between the identity object and the bucket index")
}

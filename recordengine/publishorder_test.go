package recordengine_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/recordengine"
	"github.com/oteldb/storage/wal"
)

// TestPublishWritesStreamIndexBeforeCommit verifies the publish ordering that keeps a committed
// part's rows reachable: the stream identity index must be durable before the bucket index, which
// carries the flush watermark and is therefore the commit point. Here the stream-index write fails,
// so the flush must not commit — otherwise the watermark makes replay skip the records while their
// stream identity is missing from the series index, leaving the rows unreachable forever.
func TestPublishWritesStreamIndexBeforeCommit(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	be := &rejectWrites{Backend: backend.Memory(), only: "/streams.bin"}
	walDir := t.TempDir()

	w, err := wal.Create(walDir, 0)
	require.NoError(t, err)
	t.Cleanup(func() { _ = w.Close() })

	e := recordengine.New(recordengine.Config{Schema: testSchema, Backend: be, Prefix: "t/recs", WAL: w})

	ingest(t, e, mkBatch("api", rrec{ts: 100, body: "stranded"}))
	require.NoError(t, w.Sync())

	be.armed.Store(true)
	require.Error(t, e.Flush(ctx), "flush must fail while the stream index cannot be written")
	be.armed.Store(false)

	// Restart: recover the part set and watermark from the bucket index, then replay the WAL.
	w2, err := wal.Create(walDir, 0)
	require.NoError(t, err)
	t.Cleanup(func() { _ = w2.Close() })

	r := recordengine.New(recordengine.Config{Schema: testSchema, Backend: be, Prefix: "t/recs", WAL: w2})
	require.NoError(t, r.LoadParts(ctx))
	require.NoError(t, r.Replay(walDir))

	require.Equal(t, []string{"stranded"}, streamBodies(t, r),
		"rows must stay reachable after a crash between the stream index and the bucket index")
}

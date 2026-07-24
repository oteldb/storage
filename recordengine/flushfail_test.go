package recordengine_test

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/go-faster/errors"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/recordengine"
	"github.com/oteldb/storage/wal"
)

// rejectWrites wraps a backend and fails Write while armed — the routine case a flush must survive:
// a transient object-store error.
type rejectWrites struct {
	backend.Backend

	armed atomic.Bool
}

func (r *rejectWrites) Write(ctx context.Context, key string, data []byte) error {
	if r.armed.Load() {
		return errors.New("injected write failure")
	}

	return r.Backend.Write(ctx, key, data)
}

// streamBodies returns every body the engine holds for the "api" stream, over all time.
func streamBodies(t *testing.T, e *recordengine.Engine) []string {
	t.Helper()

	batches := fetchAll(t, e, req("api"))

	out := make([]string, 0, len(batches))
	for _, b := range batches {
		out = append(out, bodies(b)...)
	}

	return out
}

// TestFlushFailureKeepsRows verifies a flush that fails before publishing a part folds the detached
// head buffers back, so the records are retried by the next flush instead of being stranded in the
// in-flight buffer (which the next flush would overwrite — silent, permanent loss).
func TestFlushFailureKeepsRows(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	be := &rejectWrites{Backend: backend.Memory()}
	e := newEngine(t, be)

	ingest(t, e, mkBatch("api", rrec{ts: 100, body: "buffered-1"}, rrec{ts: 200, body: "buffered-2"}))

	be.armed.Store(true)
	require.Error(t, e.Flush(ctx), "flush must fail while the backend rejects writes")
	be.armed.Store(false)

	require.Equal(t, []string{"buffered-1", "buffered-2"}, streamBodies(t, e), "readable after the failed flush")
	require.Positive(t, e.HeadBytes(), "the folded-back rows are accounted as head bytes again")

	ingest(t, e, mkBatch("api", rrec{ts: 300, body: "later"}))
	require.NoError(t, e.Flush(ctx))

	require.Equal(t, []string{"buffered-1", "buffered-2", "later"}, streamBodies(t, e),
		"the retried flush persists both the folded-back rows and the ones appended after it")
	require.Equal(t, 1, e.PartCount())
}

// TestFlushFailureKeepsRowsAcrossRestart is [TestFlushFailureKeepsRows] with durability: because the
// failed flush's records stay in the head, the next flush persists them and the WAL checkpoint only
// discards segments the committed part supersedes.
func TestFlushFailureKeepsRowsAcrossRestart(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	be := &rejectWrites{Backend: backend.Memory()}
	walDir := t.TempDir()

	w, err := wal.Create(walDir, 0)
	require.NoError(t, err)
	t.Cleanup(func() { _ = w.Close() })

	e := recordengine.New(recordengine.Config{Schema: testSchema, Backend: be, Prefix: "t/recs", WAL: w})

	ingest(t, e, mkBatch("api", rrec{ts: 100, body: "buffered-1"}, rrec{ts: 200, body: "buffered-2"}))
	require.NoError(t, w.Sync())

	be.armed.Store(true)
	require.Error(t, e.Flush(ctx))
	be.armed.Store(false)

	ingest(t, e, mkBatch("api", rrec{ts: 300, body: "later"}))
	require.NoError(t, w.Sync())
	require.NoError(t, e.Flush(ctx))

	// Restart: recover the watermark from the bucket index, then replay whatever WAL is left.
	w2, err := wal.Create(walDir, 0)
	require.NoError(t, err)
	t.Cleanup(func() { _ = w2.Close() })

	e2 := recordengine.New(recordengine.Config{Schema: testSchema, Backend: be, Prefix: "t/recs", WAL: w2})
	require.NoError(t, e2.LoadParts(ctx))
	require.NoError(t, e2.Replay(walDir))

	require.Equal(t, []string{"buffered-1", "buffered-2", "later"}, streamBodies(t, e2),
		"records logged to the WAL must survive a restart after a failed flush")
}

// TestFlushFailureRestoresSideStore verifies the side-store snapshot taken with the head detach is
// restored when the flush fails, so the retried flush writes sidecars that still cover the records.
func TestFlushFailureRestoresSideStore(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	be := &rejectWrites{Backend: backend.Memory()}
	fs := newFakeSide()
	e := sideEngine(be, fs)

	b := mkBatch("api", rrec{ts: 100, body: "buffered"})
	b.Side = encodeSide(map[uint64][]byte{1: []byte("a"), 2: []byte("b")})
	ingest(t, e, b)

	be.armed.Store(true)
	require.Error(t, e.Flush(ctx))
	be.armed.Store(false)

	require.Equal(t, 1, fs.restores)
	require.Len(t, fs.acc, 2, "the snapshot is back in the live accumulator")

	// A record appended during the failed flush contributes its own symbols; both sets must land.
	b2 := mkBatch("api", rrec{ts: 200, body: "later"})
	b2.Side = encodeSide(map[uint64][]byte{3: []byte("c")})
	ingest(t, e, b2)

	require.NoError(t, e.Flush(ctx))
	require.Equal(t, []uint64{1, 2, 3}, sideIDs(t, be))
}

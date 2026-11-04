package recordengine_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/backend/faultbackend"
	"github.com/oteldb/storage/recordengine"
	"github.com/oteldb/storage/wal"
)

func TestFlushReusePreservesSnapshotAndReplay(t *testing.T) {
	t.Parallel()
	be := backend.Memory()
	dir := t.TempDir()
	w, err := wal.Create(dir, 0)
	require.NoError(t, err)
	t.Cleanup(func() { _ = w.Close() })
	cfg := recordengine.Config{Schema: testSchema, Backend: be, Prefix: "records", WAL: w}
	e := recordengine.New(cfg)
	ingest(t, e, mkBatch("api", rrec{ts: 1, body: "first"}))
	rows := fetchAll(t, e, req("api"))
	require.NoError(t, e.Flush(t.Context()))
	require.Positive(t, e.Stats().ReusableBytes)
	assert.Zero(t, e.HeadBytes())
	ingest(t, e, mkBatch("api", rrec{ts: 2, body: "second"}))
	assert.Zero(t, e.Stats().ReusableBytes)
	assert.Equal(t, []string{"first"}, bodies(rows[0]), "previously returned rows own their snapshot")
	require.NoError(t, e.CloseWAL())
	cfg.WAL = nil
	reopened := recordengine.New(cfg)
	require.NoError(t, reopened.LoadParts(t.Context()))
	require.NoError(t, reopened.Replay(t.Context(), dir))
	assert.Equal(t, []string{"first", "second"}, streamBodies(t, reopened))
}

func TestFailedFlushDoesNotRecycleRecords(t *testing.T) {
	t.Parallel()
	be := faultbackend.Wrap(backend.Memory())
	e := newEngine(t, be)
	ingest(t, e, mkBatch("api", rrec{ts: 1, body: "first"}))
	rejectWrites(be, "", errWriteRejected)
	require.Error(t, e.Flush(t.Context()))
	assert.Zero(t, e.Stats().ReusableBytes)
	assert.Equal(t, []string{"first"}, streamBodies(t, e))
	be.Reset()
	require.NoError(t, e.Flush(t.Context()))
	require.Positive(t, e.Stats().ReusableBytes)
	ingest(t, e, mkBatch("api", rrec{ts: 2, body: "second"}))
	assert.Equal(t, []string{"first", "second"}, streamBodies(t, e))
	require.NoError(t, e.Close(t.Context()))
	assert.Zero(t, e.Stats().ReusableBytes)
}

func TestFlushReuseWALFailureKeepsIdleCapacity(t *testing.T) {
	t.Parallel()
	fsys, w := newFaultWAL(t)
	e := recordengine.New(recordengine.Config{Schema: testSchema, Backend: backend.Memory(), Prefix: "records", WAL: w})
	ingest(t, e, mkBatch("api", rrec{ts: 1, body: "first"}))
	require.NoError(t, e.Flush(t.Context()))
	retained := e.Stats().ReusableBytes
	require.Positive(t, retained)
	failWrite(fsys, 2)
	_, err := e.AppendBatch(mkBatch("api", rrec{ts: 2, body: "second"}), recordengine.AppendLimits{})
	require.ErrorIs(t, err, errInjectedWrite)
	assert.Equal(t, retained, e.Stats().ReusableBytes, "failed WAL staging does not borrow idle buffers")
	assert.Zero(t, e.HeadBytes())
	ingest(t, e, mkBatch("api", rrec{ts: 2, body: "second"}))
	assert.Zero(t, e.Stats().ReusableBytes)
	assert.Equal(t, []string{"first", "second"}, streamBodies(t, e))
}

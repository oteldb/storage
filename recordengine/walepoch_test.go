package recordengine_test

import (
	"context"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/backend/bucketindex"
	"github.com/oteldb/storage/backend/faultbackend"
	"github.com/oteldb/storage/recordengine"
	"github.com/oteldb/storage/wal"
)

const walEpochPrefix = "t/recs"

// newWALEngine returns an engine with its own WAL directory and node identity over the shared
// record prefix.
func newWALEngine(t *testing.T, be backend.Backend, id string) *recordengine.Engine {
	t.Helper()

	sw, err := wal.Create(t.TempDir(), 0)
	require.NoError(t, err)
	t.Cleanup(func() { _ = sw.Close() })

	return recordengine.New(recordengine.Config{
		Schema: testSchema, WAL: sw, Backend: be, Prefix: walEpochPrefix, WriterID: id,
	})
}

// walEpoch returns the generation a fresh engine writing as id would stamp on its next segment,
// which is its recovered watermark plus one.
func walEpoch(t *testing.T, be backend.Backend, id string) uint64 {
	t.Helper()

	e := newWALEngine(t, be, id)
	require.NoError(t, e.LoadParts(context.Background()))

	_, _, epoch, ok := e.WALState()
	require.True(t, ok)

	return epoch
}

// TestFlushWatermarkIsPerWriter is the invariant behind #397. The watermark is what makes record
// replay exactly-once, and it counts one node's own flushes over one node's own WAL: recovering a
// peer's number would either replay records the parts already hold or skip records only this node
// still has.
func TestFlushWatermarkIsPerWriter(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	be := backend.Memory()

	a := newWALEngine(t, be, "node-a")
	for i := range 3 {
		ingest(t, a, mkBatch("api", rrec{ts: int64(100 + i), body: "a"}))
		require.NoError(t, a.Flush(ctx))
	}

	b := newWALEngine(t, be, "node-b")
	ingest(t, b, mkBatch("web", rrec{ts: 200, body: "b"}))
	require.NoError(t, b.Flush(ctx))

	assert.EqualValues(t, 4, walEpoch(t, be, "node-a"), "three flushes of its own")
	assert.EqualValues(t, 2, walEpoch(t, be, "node-b"), "one flush of its own")
	assert.EqualValues(t, 1, walEpoch(t, be, "node-c"),
		"a writer the index has never seen starts at the first generation and replays everything")
}

// TestRebasedCommitKeepsPeerWatermark states the interleaving rather than racing for it: one
// engine is suspended inside its index commit while the other completes a whole flush, so the
// suspended commit lands on a version that moved under it and rebases. Both slots must survive.
func TestRebasedCommitKeepsPeerWatermark(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	be := faultbackend.Wrap(backend.Memory())

	a := newWALEngine(t, be, "node-a")
	b := newWALEngine(t, be, "node-b")

	// b is two flushes ahead, so a rebasing commit by a would stamp a lower number over b's.
	for i := range 2 {
		ingest(t, b, mkBatch("web", rrec{ts: int64(200 + i), body: "b"}))
		require.NoError(t, b.Flush(ctx))
	}

	ingest(t, a, mkBatch("api", rrec{ts: 100, body: "a"}))
	ingest(t, b, mkBatch("web", rrec{ts: 300, body: "b"}))

	gate := faultbackend.NewGate()
	be.Add(gate.Rule(faultbackend.CompareAndSwap, func(op faultbackend.Op) bool {
		return strings.HasSuffix(op.Key, "/"+bucketindex.Object)
	}))

	var (
		flushErr error
		wg       sync.WaitGroup
	)

	wg.Go(func() { flushErr = a.Flush(ctx) })

	gate.Await(t)
	be.Reset()

	require.NoError(t, b.Flush(ctx))

	gate.Release()
	wg.Wait()
	require.NoError(t, flushErr)

	ix, err := bucketindex.Load(ctx, be, walEpochPrefix+"/"+bucketindex.Object)
	require.NoError(t, err)
	assert.EqualValues(t, 1, ix.WriterEpoch("node-a"), "the rebasing writer records its own")
	assert.EqualValues(t, 3, ix.WriterEpoch("node-b"), "and carries the peer's through untouched")
}

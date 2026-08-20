package engine_test

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
	"github.com/oteldb/storage/engine"
	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/wal"
)

// TestReplaySkipsSegmentsBelowFlushWatermark: a WAL segment survives the flush that superseded it
// whenever the flush happened somewhere else — in a cluster the shard's compaction owner writes the
// part, so a node that logged those same records never checkpoints them away. Recovery must still be
// exactly-once, which the flush watermark in the bucket index decides: segments at or below it hold
// nothing the loaded parts do not.
func TestReplaySkipsSegmentsBelowFlushWatermark(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	dir := t.TempDir()
	be := backend.Memory()
	api := mkSeries("job", "api")

	// This node logs the sample but never flushes it, so its segment stays on disk.
	sw, err := wal.Create(dir, 0)
	require.NoError(t, err)
	logger := engine.New(engine.Config{WAL: sw, Backend: be, Prefix: "t/epoch"})
	mustAppend(t, logger, api, 100, 1)
	require.NoError(t, sw.Close())

	// The shard's compaction owner flushes the same sample into a part, advancing the watermark.
	owner := engine.New(engine.Config{Backend: be, Prefix: "t/epoch"})
	mustAppend(t, owner, api, 100, 1)
	require.NoError(t, owner.Flush(ctx))

	restored := engine.New(engine.Config{Backend: be, Prefix: "t/epoch"})
	require.NoError(t, restored.LoadParts(ctx))
	require.NoError(t, restored.Replay(dir))

	assert.Zero(t, restored.HeadSampleCount(), "the superseded segment is skipped, not replayed into the head")

	got := fetchAll(t, restored, fetch.Request{Start: 0, End: 1000, Matchers: []fetch.Matcher{eqMatcher("job", "api")}})
	require.Len(t, got, 1)
	assert.Equal(t, []int64{100}, got[0].Timestamps, "the flushed sample is served once, from its part")
}

// newWALEngine returns an engine with its own WAL directory and node identity over prefix.
func newWALEngine(t *testing.T, be backend.Backend, prefix, id string) *engine.Engine {
	t.Helper()

	sw, err := wal.Create(t.TempDir(), 0)
	require.NoError(t, err)
	t.Cleanup(func() { _ = sw.Close() })

	return engine.New(engine.Config{WAL: sw, Backend: be, Prefix: prefix, WriterID: id})
}

// walEpoch returns the generation a fresh engine writing as id would stamp on its next segment,
// which is its recovered watermark plus one.
func walEpoch(t *testing.T, be backend.Backend, prefix, id string) uint64 {
	t.Helper()

	e := newWALEngine(t, be, prefix, id)
	require.NoError(t, e.LoadParts(context.Background()))

	_, _, epoch, ok := e.WALState()
	require.True(t, ok)

	return epoch
}

// TestFlushWatermarkIsPerWriter is the invariant behind #397: the watermark counts one node's own
// flushes and indexes one node's own WAL segments, so two writers sharing a prefix must recover
// their own and never each other's — a foreign number is either a replay of records the parts hold
// or a skip of records only this node has.
func TestFlushWatermarkIsPerWriter(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	be := backend.Memory()
	const prefix = "t/slots"

	a := newWALEngine(t, be, prefix, "node-a")
	for i := range 3 {
		mustAppend(t, a, mkSeries("job", "api"), int64(100+i), float64(i))
		require.NoError(t, a.Flush(ctx))
	}

	b := newWALEngine(t, be, prefix, "node-b")
	mustAppend(t, b, mkSeries("job", "web"), 200, 2)
	require.NoError(t, b.Flush(ctx))

	assert.EqualValues(t, 4, walEpoch(t, be, prefix, "node-a"), "three flushes of its own")
	assert.EqualValues(t, 2, walEpoch(t, be, prefix, "node-b"), "one flush of its own")
	assert.EqualValues(t, 1, walEpoch(t, be, prefix, "node-c"),
		"a writer the index has never seen starts at the first generation and replays everything")
}

// TestRebasedCommitKeepsPeerWatermark states the interleaving rather than racing for it: one
// engine is suspended inside its index commit while the other completes a whole flush, so the
// suspended commit lands on a version that moved under it and rebases. Both slots must survive.
func TestRebasedCommitKeepsPeerWatermark(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	be := faultbackend.Wrap(backend.Memory())
	const prefix = "t/rebase-slots"

	a := newWALEngine(t, be, prefix, "node-a")
	b := newWALEngine(t, be, prefix, "node-b")

	// b is two flushes ahead, so a rebasing commit by a would stamp a lower number over b's.
	for i := range 2 {
		mustAppend(t, b, mkSeries("job", "web"), int64(200+i), float64(i))
		require.NoError(t, b.Flush(ctx))
	}

	mustAppend(t, a, mkSeries("job", "api"), 100, 1)
	mustAppend(t, b, mkSeries("job", "web"), 300, 3)

	gate := faultbackend.NewGate()
	be.Add(gate.Rule(faultbackend.CompareAndSwap, func(op faultbackend.Op) bool {
		return strings.HasSuffix(op.Key, bucketindex.Object)
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

	ix, err := bucketindex.Load(ctx, be, prefix+"/"+bucketindex.Object)
	require.NoError(t, err)
	assert.EqualValues(t, 1, ix.WriterEpoch("node-a"), "the rebasing writer records its own")
	assert.EqualValues(t, 3, ix.WriterEpoch("node-b"), "and carries the peer's through untouched")
}

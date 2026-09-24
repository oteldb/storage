package enginetest

import (
	"context"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/backend/bucketindex"
	"github.com/oteldb/storage/backend/faultbackend"
)

// replaySkipsSegmentsBelowFlushWatermark: a WAL segment survives the flush that superseded it
// whenever the flush happened elsewhere. In a cluster the shard's compaction owner writes the part,
// so a node that logged the same rows never checkpoints them away. Recovery must still be
// exactly-once, which the flush watermark in the bucket index decides: segments at or below it hold
// nothing the loaded parts do not.
func replaySkipsSegmentsBelowFlushWatermark(t *testing.T, k Kind) {
	t.Helper()

	ctx := context.Background()
	dir := t.TempDir()
	be := backend.Memory()

	// This node logs the row but never flushes it, so its segment stays on disk.
	w := createWAL(t, dir)
	k.Open(t, Config{Backend: be, WAL: w}).Append(t, api(100, 1))
	require.NoError(t, w.Close())

	// The shard's compaction owner flushes the same row into a part, advancing the watermark.
	owner := k.open(t, be)
	owner.Append(t, api(100, 1))
	require.NoError(t, owner.Flush(ctx))

	restored := k.open(t, be)
	require.NoError(t, restored.LoadParts(ctx))
	require.NoError(t, restored.Replay(t.Context(), dir))

	assert.Zero(t, restored.HeadRows(), "the superseded segment is skipped, not replayed into the head")
	assert.Equal(t, []Row{api(100, 1)}, rows(t, restored, apiStream), "the flushed row is served once, from its part")
}

// openWriter returns an engine with its own WAL directory and writer identity id.
func (k Kind) openWriter(t *testing.T, be backend.Backend, id string) Engine {
	t.Helper()

	return k.Open(t, Config{Backend: be, WAL: createWAL(t, t.TempDir()), WriterID: id})
}

// walEpoch returns the generation a fresh engine writing as id would stamp on its next segment,
// which is its recovered watermark plus one.
func (k Kind) walEpoch(t *testing.T, be backend.Backend, id string) uint64 {
	t.Helper()

	e := k.openWriter(t, be, id)
	require.NoError(t, e.LoadParts(context.Background()))

	_, _, epoch, ok := e.WALState()
	require.True(t, ok)

	return epoch
}

// flushWatermarkIsPerWriter is the invariant behind #397: the watermark counts one node's own
// flushes and indexes one node's own WAL segments, so two writers sharing a prefix must recover
// their own and never each other's. A foreign number is either a replay of rows the parts hold or a
// skip of rows only this node has.
func flushWatermarkIsPerWriter(t *testing.T, k Kind) {
	t.Helper()

	ctx := context.Background()
	be := backend.Memory()

	a := k.openWriter(t, be, "node-a")
	for i := range int64(3) {
		a.Append(t, api(100+i, i))
		require.NoError(t, a.Flush(ctx))
	}

	b := k.openWriter(t, be, "node-b")
	b.Append(t, web(200, 2))
	require.NoError(t, b.Flush(ctx))

	assert.EqualValues(t, 4, k.walEpoch(t, be, "node-a"), "three flushes of its own")
	assert.EqualValues(t, 2, k.walEpoch(t, be, "node-b"), "one flush of its own")
	assert.EqualValues(t, 1, k.walEpoch(t, be, "node-c"),
		"a writer the index has never seen starts at the first generation and replays everything")
}

// rebasedCommitKeepsPeerWatermark states the interleaving rather than racing for it: one engine is
// suspended inside its index commit while the other completes a whole flush, so the suspended commit
// lands on a version that moved under it and rebases. Both slots must survive.
func rebasedCommitKeepsPeerWatermark(t *testing.T, k Kind) {
	t.Helper()

	ctx := context.Background()
	be := faultbackend.Wrap(backend.Memory())

	a := k.openWriter(t, be, "node-a")
	b := k.openWriter(t, be, "node-b")

	// b is two flushes ahead, so a rebasing commit by a would stamp a lower number over b's.
	for i := range int64(2) {
		b.Append(t, web(200+i, i))
		require.NoError(t, b.Flush(ctx))
	}

	a.Append(t, api(100, 1))
	b.Append(t, web(300, 3))

	gate := gateIndexCommit(be)

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

	ix, err := bucketindex.Load(ctx, be, k.indexKey())
	require.NoError(t, err)
	assert.EqualValues(t, 1, ix.WriterEpoch("node-a"), "the rebasing writer records its own")
	assert.EqualValues(t, 3, ix.WriterEpoch("node-b"), "and carries the peer's through untouched")
}

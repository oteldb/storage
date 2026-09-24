package enginetest

import (
	"context"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/backend/faultbackend"
)

func web(ts, val int64) Row { return Row{Stream: "web", Ts: ts, Val: val} }

// flushedApartFromEachOther seeds be with two parts: one holding api up to t=100, a later one holding
// only web at t=100000. The part set's newest time therefore belongs to a stream that says nothing
// about api's durability, the shape the replica trim has to tell apart.
func (k Kind) flushedApartFromEachOther(t *testing.T, be backend.Backend) {
	t.Helper()

	k.flushEach(t, k.open(t, be), be, api(100, 1), web(100000, 2))
}

// refreshReplicaTrimsPerStream: the replica head is trimmed against each stream's own newest flushed
// row, not the newest across the part set. A row for a stream whose own flushed data is older
// survives the refresh; one already durable for its own stream does not.
func refreshReplicaTrimsPerStream(t *testing.T, k Kind) {
	t.Helper()

	ctx := context.Background()
	be := backend.Memory()
	k.flushedApartFromEachOther(t, be)

	replica := k.open(t, be)
	// Replicated, acked, and not yet flushed by the owner: legal under api's own lateness bound (its
	// newest flushed row is t=100), and older than what web has flushed.
	replica.Append(t, api(500, 3))
	// Already durable in web's own part: the trim must still drop this one.
	replica.Append(t, web(100000, 2))
	require.Equal(t, 2, replica.HeadRows())

	require.NoError(t, replica.RefreshReplica(ctx))
	require.Equal(t, 2, replica.PartCount())
	assert.Equal(t, 1, replica.HeadRows(), "web's flushed row is dropped, api's unflushed one is kept")

	assert.Equal(t, []Row{api(100, 1), api(500, 3)}, rows(t, replica, apiStream),
		"the late row another stream's flush does not cover is still served")
	assert.Equal(t, []Row{web(100000, 2)}, rows(t, replica, "web"), "the trimmed row is served from the part, once")
}

// promotedReplicaKeepsLateRow is the loss case: a replica that trimmed a late row and is then
// promoted before the owner's next flush has nowhere to recover it from. Promotion is the replica
// flushing its own head, the promoted node's first flush, and a fresh reader loading the parts.
func promotedReplicaKeepsLateRow(t *testing.T, k Kind) {
	t.Helper()

	ctx := context.Background()
	be := backend.Memory()
	k.flushedApartFromEachOther(t, be)

	replica := k.open(t, be)
	replica.Append(t, api(500, 3))
	require.NoError(t, replica.RefreshReplica(ctx))

	require.NoError(t, replica.Flush(ctx))

	promoted := k.open(t, be)
	require.NoError(t, promoted.LoadParts(ctx))

	assert.Equal(t, []Row{api(100, 1), api(500, 3)}, rows(t, promoted, apiStream),
		"the row the promoted node flushed is durable, not lost with the old owner's head")
}

// midFlushAppendSurvivesCrash is #467: a row appended while a flush writes its part off-lock is in no
// part, so the flush's WAL checkpoint must not discard the segment holding it. The interleaving is
// stated with a gate rather than raced for.
func midFlushAppendSurvivesCrash(t *testing.T, k Kind) {
	t.Helper()

	ctx := context.Background()
	be := faultbackend.Wrap(backend.Memory())
	dir := t.TempDir()

	w := createWAL(t, dir)
	e := k.Open(t, Config{Backend: be, WAL: w})
	e.Append(t, api(100, 1))

	gate := faultbackend.NewGate()
	be.Add(gate.Rule(faultbackend.Write, func(op faultbackend.Op) bool {
		return strings.HasPrefix(op.Key, k.Prefix+"/")
	}))

	var (
		wg       sync.WaitGroup
		flushErr error
	)

	wg.Go(func() { flushErr = e.Flush(ctx) })

	gate.Await(t) // the head is detached and the part objects are being written
	be.Reset()

	e.Append(t, api(200, 2)) // acknowledged mid-flush

	gate.Release()
	wg.Wait()
	require.NoError(t, flushErr)
	require.NoError(t, w.Sync())
	require.NoError(t, w.Close()) // crash: the head is gone, the backend and WAL dir survive

	restored := k.Open(t, Config{Backend: be, WAL: createWAL(t, dir)})
	require.NoError(t, restored.LoadParts(ctx))
	require.NoError(t, restored.Replay(t.Context(), dir))

	assert.Equal(t, []Row{api(100, 1), api(200, 2)}, rows(t, restored, apiStream),
		"the flushed row comes from the part and the mid-flush one from the kept segment, each once")
}

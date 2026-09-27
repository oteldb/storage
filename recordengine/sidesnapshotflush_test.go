package recordengine_test

import (
	"context"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/go-faster/errors"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/backend/faultbackend"
	"github.com/oteldb/storage/recordengine"
)

// snapshotIDs returns the sorted ids a full-window [recordengine.Engine.ReadSide] resolves.
func snapshotIDs(t *testing.T, e *recordengine.Engine) []uint64 {
	t.Helper()

	return readSideIDs(t, e, 0, 0)
}

// readSideIDs returns the sorted ids the head, the in-flight flush and the parts [recordengine.Engine.ReadSide]
// selects for [start, end] hold together.
func readSideIDs(t *testing.T, e *recordengine.Engine, start, end int64) []uint64 {
	t.Helper()

	got := map[uint64][]byte{}

	rd := e.ReadSide(start, end, func(live recordengine.SideStore) {
		require.NoError(t, decodeSide(live.Encode()["table"], got))
	})
	defer rd.Release()

	if data, ok := rd.Flushing["table"]; ok {
		require.NoError(t, decodeSide(data, got))
	}

	for _, p := range rd.Parts {
		tables, err := p.Load(context.Background())
		require.NoError(t, err)

		if data, ok := tables["table"]; ok {
			require.NoError(t, decodeSide(data, got))
		}
	}

	ids := make([]uint64, 0, len(got))
	for id := range got {
		ids = append(ids, id)
	}

	slices.Sort(ids)

	return ids
}

// TestSideSnapshotDuringFlush is #482: a flush snapshots and resets the live side accumulator at
// detach, so for the whole off-lock part write the symbols lived only in a flush-local variable —
// the records stayed fetchable through e.flushing while SideSnapshot resolved them against nothing.
// The window is stated with a gate rather than raced for.
func TestSideSnapshotDuringFlush(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	be := faultbackend.Wrap(backend.Memory())
	e := sideEngine(be, newFakeSide())

	b := mkBatch("api", rrec{ts: 1, body: "x"})
	b.Side = encodeSide(map[uint64][]byte{1: []byte("a"), 2: []byte("b")})
	ingest(t, e, b)

	want := []uint64{1, 2}
	require.Equal(t, want, snapshotIDs(t, e), "resolvable before the flush")

	gate := faultbackend.NewGate()
	be.Add(gate.Rule(faultbackend.Write, func(op faultbackend.Op) bool {
		return strings.HasPrefix(op.Key, "t/recs/")
	}))

	var (
		wg       sync.WaitGroup
		flushErr error
	)

	wg.Go(func() { flushErr = e.Flush(ctx) })

	gate.Await(t) // the head is detached and reset, the part objects are being written

	require.Equal(t, want, snapshotIDs(t, e),
		"the detached records are still fetchable, so their symbols must resolve too")

	gate.Release()
	wg.Wait()
	require.NoError(t, flushErr)

	require.Equal(t, want, snapshotIDs(t, e), "resolvable from the published part's sidecar")
}

// TestSideSnapshotAfterAbortedFlush covers the failure path: a flush that aborts reattaches the head,
// so the in-flight snapshot must be dropped in favor of the restored accumulator — visible exactly
// once, and not stale for the rest of the engine's life.
func TestSideSnapshotAfterAbortedFlush(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	be := faultbackend.Wrap(backend.Memory())
	e := sideEngine(be, newFakeSide())

	b := mkBatch("api", rrec{ts: 1, body: "x"})
	b.Side = encodeSide(map[uint64][]byte{1: []byte("a"), 2: []byte("b")})
	ingest(t, e, b)

	boom := errors.New("boom")
	be.Add(faultbackend.Rule{Kind: faultbackend.Write, Err: boom, Match: func(op faultbackend.Op) bool {
		return strings.HasPrefix(op.Key, "t/recs/")
	}})

	require.ErrorIs(t, e.Flush(ctx), boom)

	want := []uint64{1, 2}
	require.Equal(t, want, snapshotIDs(t, e), "restored into the live accumulator, not lost")

	be.Reset()

	b2 := mkBatch("api", rrec{ts: 2, body: "y"})
	b2.Side = encodeSide(map[uint64][]byte{3: []byte("c")})
	ingest(t, e, b2)

	require.NoError(t, e.Flush(ctx))
	require.Equal(t, []uint64{1, 2, 3}, snapshotIDs(t, e),
		"the retried flush publishes both deltas once; the abort left nothing stale behind")
}

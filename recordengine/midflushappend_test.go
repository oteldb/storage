package recordengine_test

import (
	"context"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/backend/faultbackend"
	"github.com/oteldb/storage/recordengine"
	"github.com/oteldb/storage/wal"
)

// TestMidFlushAppendSurvivesCrash is #467: a record appended while a flush writes its part off-lock
// is in no part, so the flush's WAL checkpoint must not discard the segment holding it. The
// interleaving is stated with a gate rather than raced for.
func TestMidFlushAppendSurvivesCrash(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	be := faultbackend.Wrap(backend.Memory())
	walDir := t.TempDir()

	w, err := wal.Create(walDir, 0)
	require.NoError(t, err)
	t.Cleanup(func() { _ = w.Close() })

	e := recordengine.New(recordengine.Config{Schema: testSchema, Backend: be, Prefix: "t/recs", WAL: w})
	ingest(t, e, mkBatch("api", rrec{ts: 1, body: "b1"}))

	gate := faultbackend.NewGate()
	be.Add(gate.Rule(faultbackend.Write, func(op faultbackend.Op) bool {
		return strings.HasPrefix(op.Key, "t/recs/")
	}))

	var (
		wg       sync.WaitGroup
		flushErr error
	)

	wg.Go(func() { flushErr = e.Flush(ctx) })

	gate.Await(t) // the head is detached and the part objects are being written
	be.Reset()

	ingest(t, e, mkBatch("api", rrec{ts: 2, body: "b2"})) // acknowledged mid-flush

	gate.Release()
	wg.Wait()
	require.NoError(t, flushErr)
	require.NoError(t, w.Sync())
	require.NoError(t, w.Close()) // crash: the head is gone, the backend and WAL dir survive

	w2, err := wal.Create(walDir, 0)
	require.NoError(t, err)
	t.Cleanup(func() { _ = w2.Close() })

	e2 := recordengine.New(recordengine.Config{Schema: testSchema, Backend: be, Prefix: "t/recs", WAL: w2})
	require.NoError(t, e2.LoadParts(ctx))
	require.NoError(t, e2.Replay(walDir))

	require.ElementsMatch(t, []string{"b1", "b2"}, streamBodies(t, e2),
		"the flushed record comes from the part and the mid-flush one from the kept segment, each once")
}

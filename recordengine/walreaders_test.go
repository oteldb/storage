package recordengine_test

import (
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/internal/vfs/faultfs"
	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/recordengine"
)

func TestReadersDuringWALOperation(t *testing.T) {
	t.Parallel()
	for _, op := range []string{"identity", "records", "primary", "sync", "seal", "checkpoint", "close"} {
		t.Run(op, func(t *testing.T) {
			t.Parallel()
			fsys, w := newFaultWAL(t)
			e := recordengine.New(recordengine.Config{
				Schema: testSchema, WAL: w, Backend: backend.Memory(), Prefix: "records", OOOWindow: 10,
			})
			_, err := e.AppendBatch(mkBatch("api", rrec{ts: 1, body: "first"}), recordengine.AppendLimits{})
			require.NoError(t, err)
			_, err = e.AppendBatch(mkBatch("other", rrec{ts: 1, body: "other"}), recordengine.AppendLimits{})
			require.NoError(t, err)
			ioOp := faultfs.OpWrite
			switch op {
			case "sync", "seal", "close":
				ioOp = faultfs.OpSync
			case "checkpoint":
				ioOp = faultfs.OpRemove
			}
			gate := faultfs.NewGate()
			release := sync.OnceFunc(gate.Release)
			t.Cleanup(release)
			fsys.Add(gate.Rule(ioOp, func(c faultfs.Call) bool { return strings.HasSuffix(c.Name, ".wal") }))
			done := make(chan error, 1)
			go func() {
				var err error
				b := mkBatch("api", rrec{ts: 100, body: "pending"})
				switch op {
				case "identity":
					_, err = e.AppendBatch(mkBatch("new", rrec{ts: 100, body: "pending"}), recordengine.AppendLimits{})
				case "records":
					_, err = e.AppendBatch(b, recordengine.AppendLimits{})
				case "primary":
					_, _, err = e.ApplyPrimary(recordengine.EncodeWAL(b), recordengine.AppendLimits{})
				case "sync":
					err = e.SyncWAL()
				case "seal", "checkpoint":
					err = e.Flush(t.Context())
				case "close":
					err = e.CloseWAL()
				}
				done <- err
			}()
			gate.Await(t)
			assertReadableDuringWAL(t, e, 2)
			release()
			require.NoError(t, <-done)
		})
	}
}

func assertReadableDuringWAL(t *testing.T, e *recordengine.Engine, want int) {
	t.Helper()
	type result struct {
		rows int
		err  error
	}
	done := make(chan result, 1)
	go func() {
		_ = e.Stats()
		it, err := e.Fetch(t.Context(), fetch.Request{Start: 0, End: 1000})
		if err != nil {
			done <- result{err: err}
			return
		}
		batches, err := fetch.Drain(t.Context(), it)
		var n int
		for _, b := range batches {
			n += len(b.Timestamps)
		}
		done <- result{rows: n, err: err}
	}()
	select {
	case got := <-done:
		require.NoError(t, got.err)
		assert.Equal(t, want, got.rows, "readers see committed records only")
	case <-time.After(10 * time.Second):
		t.Fatal("Stats or Fetch blocked behind WAL I/O")
	}
}

func TestWALWriterGatePreservesAdmissionOrder(t *testing.T) {
	t.Parallel()
	fsys, w := newFaultWAL(t)
	e := recordengine.New(recordengine.Config{Schema: testSchema, WAL: w, OOOWindow: 10})
	_, err := e.AppendBatch(mkBatch("api", rrec{ts: 1, body: "first"}), recordengine.AppendLimits{})
	require.NoError(t, err)
	gate := faultfs.NewGate()
	release := sync.OnceFunc(gate.Release)
	t.Cleanup(release)
	fsys.Add(gate.Rule(faultfs.OpWrite, func(c faultfs.Call) bool { return strings.HasSuffix(c.Name, ".wal") }))
	first := make(chan error, 1)
	go func() {
		_, err := e.AppendBatch(mkBatch("api", rrec{ts: 100, body: "second"}), recordengine.AppendLimits{})
		first <- err
	}()
	gate.Await(t)
	second := make(chan recordengine.AppendResult, 1)
	go func() {
		res, _ := e.AppendBatch(mkBatch("api", rrec{ts: 2, body: "late"}), recordengine.AppendLimits{})
		second <- res
	}()
	assertReadableDuringWAL(t, e, 1)
	release()
	require.NoError(t, <-first)
	assert.Equal(t, 1, (<-second).RejectedOOO)
	assert.Equal(t, []string{"first", "second"}, streamBodies(t, replayed(t, fsys, nil)))
}

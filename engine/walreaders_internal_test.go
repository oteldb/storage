package engine

import (
	"bytes"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/internal/vfs/faultfs"
	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/signal"
	"github.com/oteldb/storage/wal"
)

func TestReadersDuringWALOperation(t *testing.T) {
	t.Parallel()
	for _, op := range []string{"append", "batch", "primary", "sync", "seal", "checkpoint", "close"} {
		t.Run(op, func(t *testing.T) {
			t.Parallel()
			e, fsys := newFaultWALEngine(t, 10)
			e.cfg.Backend, e.cfg.Prefix = backend.Memory(), "metrics"
			s := stagedTestSeries("known")
			_, err := e.Append(s, 1, 1)
			require.NoError(t, err)
			_, err = e.Append(stagedTestSeries("other"), 1, 2)
			require.NoError(t, err)

			var frames bytes.Buffer
			w := wal.NewWriter(&frames)
			require.NoError(t, w.WriteSeries(s.Hash(), s))
			require.NoError(t, w.WriteSamples(s.Hash(), []int64{100}, []float64{3}))
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
				switch op {
				case "append":
					_, err = e.Append(s, 100, 3)
				case "batch":
					_, err = e.AppendBatch([]signal.SeriesID{s.Hash()}, []int64{100}, []float64{3}, nil,
						func(int) signal.Series { return s }, AppendLimits{})
				case "primary":
					_, _, err = e.ApplyPrimary(frames.Bytes(), AppendLimits{})
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

func assertReadableDuringWAL(t *testing.T, e *Engine, want int) {
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
		assert.Equal(t, want, got.rows, "readers see committed rows only")
	case <-time.After(10 * time.Second):
		t.Fatal("Stats or Fetch blocked behind WAL I/O")
	}
}

func TestWALWriterGatePreservesAdmissionOrder(t *testing.T) {
	t.Parallel()
	e, fsys := newFaultWALEngine(t, 10)
	s := stagedTestSeries("known")
	_, err := e.Append(s, 1, 1)
	require.NoError(t, err)
	gate := faultfs.NewGate()
	release := sync.OnceFunc(gate.Release)
	t.Cleanup(release)
	fsys.Add(gate.Rule(faultfs.OpWrite, func(c faultfs.Call) bool { return strings.HasSuffix(c.Name, ".wal") }))
	first := make(chan error, 1)
	go func() { _, err := e.Append(s, 100, 2); first <- err }()
	gate.Await(t)
	second := make(chan bool, 1)
	go func() { accepted, _ := e.Append(s, 2, 3); second <- accepted }()
	assertReadableDuringWAL(t, e, 1)
	release()
	require.NoError(t, <-first)
	assert.False(t, <-second, "admission must observe the preceding WAL-backed write")
	assert.Equal(t, []int64{1, 100}, headTS(replayedHead(t, fsys), s))
}

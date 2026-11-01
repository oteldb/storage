package engine

import (
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/internal/vfs/faultfs"
	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/signal"
)

func enableMemoryPublicationTest(t *testing.T, e *Engine) {
	t.Helper()
	view := &memoryReadView{}
	view.inherit(nil)
	view.registry = e.head.series.View()
	e.readView = view
	e.mu.EnableSharedWriters()
	e.head.memoryRuns = true
	e.warmPreparations = newBatchPreparations()
	e.reservations.trackNewest = e.cfg.OOOWindow > 0
	for id, timestamp := range e.head.seriesNewest {
		e.reservations.setNewest(id, timestamp)
	}
	e.head.seriesNewest = nil
	e.head.admission = &e.reservations
	e.loggedIdentities.prepare(e.head.series, e.cfg.WAL.Epoch(), e.head.series.Len())
	e.head.logged = &e.loggedIdentities
	t.Cleanup(func() {
		e.viewMu.Lock()
		current := e.readView
		e.readView = nil
		e.viewMu.Unlock()
		current.release()
	})
}

func TestMemoryRunPublicationAfterWALSuccess(t *testing.T) {
	t.Parallel()
	e, fsys := newFaultWALEngine(t, 0)
	enableMemoryPublicationTest(t, e)
	gate := faultfs.NewGate()
	release := sync.OnceFunc(gate.Release)
	t.Cleanup(release)
	fsys.Add(gate.Rule(faultfs.OpWrite, func(call faultfs.Call) bool { return strings.HasSuffix(call.Name, ".wal") }))
	s := stagedTestSeries("new")
	done := make(chan error, 1)
	go func() {
		_, err := e.AppendBatch([]signal.SeriesID{s.Hash(), s.Hash()}, []int64{10, 20}, []float64{1, 2}, nil,
			func(int) signal.Series { return s }, AppendLimits{})
		done <- err
	}()
	gate.Await(t)
	assertReadableDuringWAL(t, e, 0)
	require.Zero(t, e.SeriesCount(), "private identity/data remain invisible before WAL success")
	release()
	require.NoError(t, <-done)
	require.NotContains(t, e.head.samples, s.Hash(), "published rows live in the immutable run")
	require.Equal(t, int64(2), e.readView.active.rows)
	it, err := e.Fetch(t.Context(), fetch.Request{Start: 0, End: 100})
	require.NoError(t, err)
	batch, err := it.Next(t.Context())
	require.NoError(t, err)
	require.Equal(t, []int64{10, 20}, batch.Timestamps)
	require.Equal(t, []float64{1, 2}, batch.Values)
	require.NoError(t, it.Close())
}

func TestMemoryRunFailedWALKeepsReadView(t *testing.T) {
	t.Parallel()
	e, fsys := newFaultWALEngine(t, 0)
	enableMemoryPublicationTest(t, e)
	s := stagedTestSeries("known")
	_, err := e.Append(s, 10, 1)
	require.NoError(t, err)
	before := e.readView
	failNextWALWrite(fsys)
	missing := stagedTestSeries("failed")
	_, err = e.Append(missing, 20, 2)
	require.ErrorIs(t, err, errInjectedWAL)
	require.Same(t, before, e.readView)
	require.Equal(t, 1, e.SeriesCount())
	require.NotContains(t, e.head.samples, missing.Hash())
	require.Equal(t, int64(1), e.readView.active.rows)
}

func TestMemoryRunReaderKeepsPublicationRevision(t *testing.T) {
	t.Parallel()
	e, _ := newFaultWALEngine(t, 0)
	enableMemoryPublicationTest(t, e)
	s := apiSeries()
	_, err := e.Append(s, 10, 1)
	require.NoError(t, err)
	old, err := e.Fetch(t.Context(), fetch.Request{Start: 0, End: 100})
	require.NoError(t, err)
	_, err = e.Append(s, 10, 2)
	require.NoError(t, err)
	batch, err := old.Next(t.Context())
	require.NoError(t, err)
	require.Equal(t, []float64{1}, batch.Values)
	require.NoError(t, old.Close())
	current, err := e.Fetch(t.Context(), fetch.Request{Start: 0, End: 100})
	require.NoError(t, err)
	batch, err = current.Next(t.Context())
	require.NoError(t, err)
	require.Equal(t, []float64{2}, batch.Values)
	require.NoError(t, current.Close())
}

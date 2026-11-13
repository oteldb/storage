package engine

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/backend/bucketindex"
)

func TestAttachSharedSeriesDuringSnapshotPublication(t *testing.T) {
	t.Parallel()
	be := backend.Memory()
	e := separateCatalogEngine(be)
	t.Cleanup(func() { require.NoError(t, e.CloseWAL()) })
	binding := catalogRoot(t, be, "series-catalog/s-attach", 1)
	e.mu.Lock()
	err := e.installSeriesRootsLocked(t.Context(), []bucketindex.SeriesRoot{binding})
	e.mu.Unlock()
	require.NoError(t, err)
	p := &part{}
	defer p.releaseSharedSeries()
	started := make(chan struct{})
	done := make(chan error, 1)
	e.mu.LockState()
	go func() {
		close(started)
		done <- e.attachSharedSeries(p)
	}()
	<-started
	previous := e.seriesTable
	next := newSeriesSnapshot(previous.roots)
	e.seriesTable = next
	previous.release()
	e.mu.UnlockState()
	require.NoError(t, <-done)
	require.Same(t, next, p.sharedSeries.snapshot, "attachment waits for the snapshot publication and owns its lease")
	p.releaseSharedSeries()
	_, err = e.seriesTable.lookup(t.Context(), benchSeries(1).Hash(), func([]byte) error { return nil })
	require.NoError(t, err, "releasing the part must preserve the catalog's run lease")
}

func TestPrepareAttachedSharedPartUnderWriterOwnership(t *testing.T) {
	t.Parallel()
	e := separateCatalogEngine(backend.Memory())
	snapshot := newSeriesSnapshot(nil)
	p := &part{sharedSeries: newSeriesBinding(snapshot)}
	snapshot.release()
	defer p.releaseSharedSeries()
	e.mu.LockWriter()
	err := e.attachSharedSeries(p)
	e.mu.UnlockWriter()
	require.NoError(t, err, "already attached parts must not reacquire state ownership")
	require.Same(t, snapshot, p.sharedSeries.snapshot)
}

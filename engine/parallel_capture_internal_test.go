package engine

import (
	"runtime"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/signal"
)

func TestParallelCapturePinsHistoricalCache(t *testing.T) {
	t.Parallel()
	e, _ := newFaultWALEngine(t, 0)
	e.cfg.DiskSeries = true
	e.head.disk = newDiskHeadState()
	id := stagedTestSeries("cached").Hash()
	e.head.disk.remember(id, 100)
	entered, gate := make(chan struct{}), make(chan struct{})
	resume := sync.OnceFunc(func() { close(gate) })
	defer resume()
	e.parallel.hooks = &parallelHeadHooks{capturing: func() { close(entered); <-gate }}
	done := make(chan error, 1)
	go func() { done <- e.captureParallelDiskSeries([]signal.SeriesID{id}, &preparedBatch{}) }()
	<-entered
	writerAcquired := e.mu.TryLockWriter()
	if writerAcquired {
		e.mu.UnlockWriter()
	}
	resume()
	require.NoError(t, <-done)
	require.False(t, writerAcquired, "in-memory capture must exclude ordered cache mutation")
}

func TestParallelCaptureAndOrderedCacheGrowth(t *testing.T) {
	t.Parallel()
	e, _ := newFaultWALEngine(t, 0)
	e.cfg.DiskSeries = true
	e.head.disk = newDiskHeadState()
	id := stagedTestSeries("cached").Hash()
	e.head.disk.note(id)
	e.head.disk.remember(id, 100)
	started, done := make(chan struct{}), make(chan struct{})
	t.Cleanup(func() { <-done })
	go func() {
		defer close(done)
		e.mu.LockWriter()
		defer e.mu.UnlockWriter()
		_ = e.mu.WithReaders(func() error {
			close(started)
			for i := range 2048 {
				e.head.disk.remember(id, int64(i+100))
				e.head.disk.remember(signal.SeriesID{Lo: uint64(i + 1)}, int64(i))
				runtime.Gosched()
			}
			return nil
		})
	}()
	<-started
	p := &preparedBatch{}
	for range 64 {
		require.NoError(t, e.captureParallelDiskSeries([]signal.SeriesID{id}, p))
	}
	<-done
	require.EqualValues(t, 2147, p.lookup.pending[id].newest)
}

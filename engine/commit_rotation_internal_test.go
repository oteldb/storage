package engine

import (
	"sync"
	"testing"
	"testing/synctest"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/signal"
)

func TestCommitOwnerDrainsPreparedTailBeforeSeal(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		e, fs := newFaultWALEngine(t, 50)
		e.cfg.DiskSeries, e.cfg.AsyncPressureFlush, e.cfg.PressureGenerations = true, true, 2
		e.cfg.ResidentHeadBytes = 1 << 30
		e.head.disk = newDiskHeadState()
		s := stagedTestSeries("rotation")
		_, err := e.Append(s, 1, 1)
		require.NoError(t, err)
		epoch := e.cfg.WAL.Epoch()
		ready := make(chan struct{})
		release := make(chan struct{})
		resume := sync.OnceFunc(func() { close(release) })
		defer resume()
		e.parallel.hooks = &parallelHeadHooks{beforePublish: func() { close(ready); <-release }}
		appended := make(chan parallelAppendResult, 1)
		go func() {
			appended <- appendPreparedTest(e, []signal.SeriesID{s.Hash()}, []int64{2}, []float64{2}, AppendLimits{})
		}()
		<-ready

		type rotationResult struct {
			rotated bool
			err     error
		}
		rotated := make(chan rotationResult, 1)
		go func() {
			ok, err := e.rotateWithCommitOwner(t.Context(), true, false)
			rotated <- rotationResult{ok, err}
		}()
		gateFree := e.parallel.generation.TryLock()
		if gateFree {
			e.parallel.generation.Unlock()
		}
		require.False(t, gateFree, "a prepared tail holds its generation until publication")
		require.Equal(t, epoch, e.cfg.WAL.Epoch())
		resume()
		a := <-appended
		require.NoError(t, a.err)
		require.Equal(t, 1, a.result.Accepted)
		r := <-rotated
		require.NoError(t, r.err)
		require.True(t, r.rotated)
		require.Greater(t, e.cfg.WAL.Epoch(), epoch)
		require.Len(t, e.pressureGenerations, 1)
		require.Equal(t, []int64{1, 2}, headTS(e.pressureGenerations[0].head, s))

		e.parallel.hooks = nil
		_, err = e.Append(s, 3, 3)
		require.NoError(t, err)
		require.Equal(t, []int64{3}, headTS(e.head, s))
		require.Equal(t, []int64{1, 2, 3}, headTS(replayedHead(t, fs), s))
	})
}

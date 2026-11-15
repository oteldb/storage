package engine

import (
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/internal/reproduce"
	"github.com/oteldb/storage/signal"
)

func TestAutomaticPressureSecondGeneration(t *testing.T) {
	t.Parallel()
	reproduce.Unfixed(t, 661, "a frozen half-head leaves too little aggregate room for a second half-head generation")
	synctest.Test(t, func(t *testing.T) {
		const headLimit = 512 << 10
		e, _ := newFaultWALEngine(t, 10)
		be := &generationGateBackend{Backend: backend.Memory(), first: make(chan struct{}), resume: make(chan struct{})}
		resume := sync.OnceFunc(func() { close(be.resume) })
		e.cfg.Backend, e.cfg.Prefix = be, "automatic-generations"
		e.cfg.DiskSeries, e.cfg.AsyncPressureFlush, e.cfg.PressureGenerations = true, true, 2
		e.cfg.ResidentHeadBytes = headLimit
		e.head.disk = newDiskHeadState()
		var maxGrowth atomic.Int64
		done := make(chan error, 1)
		go func() {
			const batch = 16
			for base := 0; base < 8192; base += batch {
				ids := make([]signal.SeriesID, batch)
				series := make([]signal.Series, batch)
				ts, values := make([]int64, batch), make([]float64, batch)
				for i := range ids {
					series[i] = benchSeries(base + i)
					ids[i] = series[i].Hash()
					ts[i] = 100
					values[i] = 1
				}
				before := automaticInputBytes(e)
				result, err := e.AppendBatch(ids, ts, values, nil, func(i int) signal.Series { return series[i] }, AppendLimits{})
				if err != nil {
					done <- err
					return
				}
				if result.Accepted != batch {
					t.Errorf("accepted=%d want=%d", result.Accepted, batch)
				}
				growth := automaticInputBytes(e) - before
				if growth > maxGrowth.Load() {
					maxGrowth.Store(growth)
				}
			}
			done <- nil
		}()
		defer func() { resume(); require.NoError(t, <-done); require.NoError(t, e.Close(t.Context())) }()
		<-be.first
		synctest.Wait()
		require.EqualValues(t, 2, be.identities.Load(), "ordinary admission must build B while A remains blocked, without changing the budget")
		e.mu.RLock()
		frozen := len(e.pressureGenerations)
		e.mu.RUnlock()
		require.Equal(t, 2, frozen)
		require.Zero(t, e.PartCount(), "a later output cannot publish across the blocked generation")
		used := automaticInputBytes(e)
		require.LessOrEqual(t, used, int64(headLimit)+maxGrowth.Load(), "the same aggregate cap allows only one already-admitted batch of overshoot")
		t.Logf("input=%d target=%d largest admission growth=%d", used, headLimit, maxGrowth.Load())
	})
}

func automaticInputBytes(e *Engine) int64 {
	e.mu.RLock()
	defer e.mu.RUnlock()
	n := e.head.residentIdentityBytes() + e.head.inFlightBytes() + e.parallel.pending.Load()
	for _, g := range e.pressureGenerations {
		n -= g.head.buildRetained
	}
	return n
}

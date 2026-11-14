package engine

import (
	"strconv"
	"sync"
	"testing"
	"testing/synctest"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/signal"
)

func TestQueuedPreparationsRecheckResidentPressure(t *testing.T) {
	t.Parallel()
	for _, async := range []bool{false, true} {
		t.Run(strconv.FormatBool(async), func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				e, _ := newFaultWALEngine(t, 0)
				be := &pressureGateBackend{Backend: backend.Memory(), entered: make(chan struct{}), resume: make(chan struct{})}
				resume := sync.OnceFunc(func() { close(be.resume) })
				defer resume()
				e.cfg.Backend, e.cfg.Prefix = be, "queued-pressure"
				e.cfg.DiskSeries, e.cfg.AsyncPressureFlush = true, async
				e.cfg.ResidentHeadBytes = 1
				e.head.disk = newDiskHeadState()
				preparations := e.preparationPool()
				held := make([]*preparedBatch, maxPreparedBatches)
				for i := range held {
					held[i] = preparations.acquireWaiting()
				}
				const calls = 4 * maxPreparedBatches
				done := make(chan parallelAppendResult, calls)
				for i := range calls {
					s := benchSeries(i)
					go func() { done <- appendColdTest(e, s, 100, AppendLimits{}, func() signal.Series { return s }) }()
				}
				synctest.Wait()
				// Leave a single slot available: queued requests must recheck before using it.
				preparations.release(held[0])
				synctest.Wait()
				completed := len(done)
				resume()
				for _, p := range held[1:] {
					preparations.release(p)
				}
				synctest.Wait()
				for range calls {
					r := <-done
					require.NoError(t, r.err)
					require.Equal(t, 1, r.result.Accepted)
				}
				require.NoError(t, e.Close(t.Context()))
				require.LessOrEqual(t, completed, 2, "queued callers cannot bypass full-target pressure")
				require.Positive(t, completed, "already accepted batches must finish")
			})
		})
	}
}

func TestSmallPreparationsDoNotStrandArenaPages(t *testing.T) {
	t.Parallel()
	e, fs := newFaultWALEngine(t, 0)
	e.cfg.Backend, e.cfg.Prefix = backend.Memory(), "small-preparations"
	for round := range 2 {
		for i := range sampleHeaderPageSize {
			s := benchSeries(i)
			id := s.Hash()
			ids := []signal.SeriesID{id}
			ts, values := []int64{int64(round*10 + 1)}, []float64{1}
			if round == 1 {
				ids, ts, values = append(ids, id), append(ts, int64(round*10+2)), append(values, 2)
			}
			res, err := e.AppendBatch(ids, ts, values, nil, func(int) signal.Series { return s }, AppendLimits{})
			require.NoError(t, err)
			require.Equal(t, len(ids), res.Accepted)
		}
		numeric, headers := 0, 0
		for p := e.head.memory.numeric; p != nil; p = p.next {
			numeric++
		}
		for p := e.head.memory.headers; p != nil; p = p.next {
			headers++
		}
		require.LessOrEqual(t, numeric, 1, "private singleton numeric pages must not accumulate")
		require.LessOrEqual(t, headers, 1, "private singleton header pages must not accumulate")
		require.Equal(t, []int64{int64(round*10 + 1)}, headTS(replayedHead(t, fs), benchSeries(0))[:1])
		require.NoError(t, e.Flush(t.Context()))
	}
}

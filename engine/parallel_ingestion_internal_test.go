package engine

import (
	"sync"
	"testing"
	"testing/synctest"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/signal"
)

func appendColdTest(e *Engine, s signal.Series, ts int64, limits AppendLimits, materialize func() signal.Series) parallelAppendResult {
	res, err := e.AppendBatch([]signal.SeriesID{s.Hash()}, []int64{ts}, []float64{float64(ts)}, nil,
		func(int) signal.Series { return materialize() }, limits)
	return parallelAppendResult{result: res, err: err}
}

func TestColdParallelWALFailureReleasesAdmission(t *testing.T) {
	t.Parallel()
	e, fs := newFaultWALEngine(t, 10)
	s := stagedTestSeries("failed-cold")
	failNextWALWrite(fs)
	r := appendColdTest(e, s, 100, AppendLimits{MaxSeries: 1, MaxInFlightBytes: SampleBytes}, func() signal.Series { return s })
	require.ErrorIs(t, r.err, errInjectedWAL)
	require.Equal(t, AppendResult{}, r.result)
	require.Zero(t, e.HeadSampleCount())
	require.False(t, e.head.series.Has(s.Hash()))
	require.Zero(t, e.parallel.pending.Load())
	require.Zero(t, e.parallel.pendingSeries.Load())
	r = appendColdTest(e, s, 100, AppendLimits{MaxSeries: 1, MaxInFlightBytes: SampleBytes}, func() signal.Series { return s })
	require.NoError(t, r.err)
	require.Equal(t, 1, r.result.Accepted)
	require.Equal(t, []int64{100}, headTS(replayedHead(t, fs), s))
}

func TestColdBatchPublishesHeadersAtomically(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		e, _ := newFaultWALEngine(t, 0)
		a, b := stagedTestSeries("atomic-a"), stagedTestSeries("atomic-b")
		ready, gate := make(chan struct{}), make(chan struct{})
		resume := sync.OnceFunc(func() { close(gate) })
		defer resume()
		e.parallel.hooks = &parallelHeadHooks{beforePublish: func() { close(ready); <-gate }}
		done := make(chan parallelAppendResult, 1)
		go func() {
			res, err := e.AppendBatch([]signal.SeriesID{a.Hash(), b.Hash()}, []int64{1, 2}, []float64{10, 20},
				[]float64{2, 1}, func(i int) signal.Series { return []signal.Series{a, b}[i] }, AppendLimits{})
			done <- parallelAppendResult{result: res, err: err}
		}()
		<-ready
		require.Zero(t, e.HeadSampleCount())
		require.Zero(t, e.SeriesCount())
		resume()
		synctest.Wait()
		r := <-done
		require.NoError(t, r.err)
		require.Equal(t, 2, r.result.Accepted)
		require.Equal(t, []float64{2}, e.head.samples[a.Hash()].sf)
		require.Nil(t, e.head.samples[b.Hash()].sf)
	})
}

func TestHistoricalParallelAdmissionPreservesWatermarksAndCardinality(t *testing.T) {
	t.Parallel()
	e, fs := newFaultWALEngine(t, 10)
	e.cfg.Backend, e.cfg.Prefix = backend.Memory(), "historical-parallel"
	e.cfg.DiskSeries, e.cfg.MetricSeriesChunks = true, true
	e.cfg.SeriesCacheBytes = -1
	e.cfg.ResidentHeadBytes = 1 << 30
	e.head.disk = newDiskHeadStateWithCache(-1)
	a, b := stagedTestSeries("historical-a"), stagedTestSeries("historical-b")
	for _, s := range []signal.Series{a, b} {
		r := appendColdTest(e, s, 100, AppendLimits{}, func() signal.Series { return s })
		require.NoError(t, r.err)
	}
	require.NoError(t, e.Flush(t.Context()))
	require.Zero(t, e.head.series.Len())
	for _, s := range []signal.Series{a, b} {
		r := appendColdTest(e, s, 80, AppendLimits{MaxSeries: 2}, func() signal.Series { t.Error("OOO identity materialized"); return s })
		require.NoError(t, r.err)
		require.Equal(t, AppendResult{RejectedOOO: 1}, r.result)
		r = appendColdTest(e, s, 200, AppendLimits{MaxSeries: 2}, func() signal.Series { return s })
		require.NoError(t, r.err)
		require.Equal(t, 1, r.result.Accepted)
	}
	require.Equal(t, 2, e.SeriesCount(), "rehydrating historical identities must not consume cardinality twice")
	require.Equal(t, []int64{200}, headTS(replayedHead(t, fs), a))
}

func TestColdWritesPrepareAndPublishIndependently(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		e, fs := newFaultWALEngine(t, 10)
		a, b := stagedTestSeries("cold-a"), stagedTestSeries("cold-b")
		ordered := make(chan struct{}, 1)
		e.parallel.hooks = &parallelHeadHooks{ordered: func(ids []signal.SeriesID) {
			if ids[0] == b.Hash() {
				ordered <- struct{}{}
			}
		}}
		started, gate := make(chan struct{}), make(chan struct{})
		resume := sync.OnceFunc(func() { close(gate) })
		defer resume()
		first, second := make(chan parallelAppendResult, 1), make(chan parallelAppendResult, 1)
		go func() {
			first <- appendColdTest(e, a, 100, AppendLimits{}, func() signal.Series {
				close(started)
				<-gate
				return a
			})
		}()
		<-started
		go func() { second <- appendColdTest(e, b, 200, AppendLimits{}, func() signal.Series { return b }) }()
		progressed := false
		var r parallelAppendResult
		select {
		case r = <-second:
			progressed = true
			require.NoError(t, r.err)
			e.mu.RLock()
			require.Equal(t, []int64{200}, headTS(e.head, b))
			require.Empty(t, headTS(e.head, a))
			e.mu.RUnlock()
			require.Equal(t, []int64{200}, headTS(replayedHead(t, fs), b), "B must already be durable")
		case <-ordered:
		}
		resume()
		synctest.Wait()
		if !progressed {
			r = <-second
		}
		require.NoError(t, r.err)
		require.NoError(t, (<-first).err)
		require.True(t, progressed, "B must publish and journal while A prepares its private identity")
	})
}

func TestSameSeriesWaitDoesNotBlockOtherSeries(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		e, _ := newFaultWALEngine(t, 10)
		a, b := stagedTestSeries("owner-a"), stagedTestSeries("owner-b")
		ordered, contested := make(chan struct{}, 2), make(chan struct{}, 1)
		e.parallel.hooks = &parallelHeadHooks{ordered: func([]signal.SeriesID) { ordered <- struct{}{} },
			fallback: func() { contested <- struct{}{} }}
		started, gate := make(chan struct{}), make(chan struct{})
		resume := sync.OnceFunc(func() { close(gate) })
		defer resume()
		first, conflict, other := make(chan parallelAppendResult, 1), make(chan parallelAppendResult, 1), make(chan parallelAppendResult, 1)
		go func() {
			first <- appendColdTest(e, a, 100, AppendLimits{}, func() signal.Series { close(started); <-gate; return a })
		}()
		<-started
		select {
		case <-ordered:
		default:
		}
		go func() { conflict <- appendColdTest(e, a, 80, AppendLimits{}, func() signal.Series { return a }) }()
		select {
		case <-ordered:
		case <-contested:
		}
		go func() { other <- appendColdTest(e, b, 200, AppendLimits{}, func() signal.Series { return b }) }()
		progressed := false
		select {
		case r := <-other:
			require.NoError(t, r.err)
			progressed = true
		case <-ordered:
		}
		resume()
		synctest.Wait()
		require.NoError(t, (<-first).err)
		r := <-conflict
		require.NoError(t, r.err)
		require.Equal(t, AppendResult{RejectedOOO: 1}, r.result)
		if !progressed {
			require.NoError(t, (<-other).err)
		}
		require.True(t, progressed, "a same-series waiter must not own a global writer barrier")
	})
}

func TestPendingColdIdentityReservesCardinality(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		e, _ := newFaultWALEngine(t, 0)
		a, b := stagedTestSeries("limited-a"), stagedTestSeries("limited-b")
		ordered := make(chan struct{}, 1)
		e.parallel.hooks = &parallelHeadHooks{ordered: func(ids []signal.SeriesID) {
			if ids[0] == b.Hash() {
				ordered <- struct{}{}
			}
		}}
		limits := AppendLimits{MaxSeries: 1}
		started, gate := make(chan struct{}), make(chan struct{})
		resume := sync.OnceFunc(func() { close(gate) })
		defer resume()
		first, second := make(chan parallelAppendResult, 1), make(chan parallelAppendResult, 1)
		go func() {
			first <- appendColdTest(e, a, 100, limits, func() signal.Series { close(started); <-gate; return a })
		}()
		<-started
		go func() {
			second <- appendColdTest(e, b, 200, limits, func() signal.Series { t.Error("rejected identity materialized"); return b })
		}()
		progressed := false
		var r parallelAppendResult
		select {
		case r = <-second:
			progressed = true
		case <-ordered:
		}
		resume()
		synctest.Wait()
		if !progressed {
			r = <-second
		}
		require.NoError(t, (<-first).err)
		require.NoError(t, r.err)
		require.Equal(t, AppendResult{RejectedCardinality: 1}, r.result)
		require.True(t, progressed, "pending identity reservations must enforce the cap without blocking unrelated admission")
	})
}

package engine

import (
	"sync"
	"testing"
	"testing/synctest"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/internal/reproduce"
	"github.com/oteldb/storage/signal"
)

func appendColdTest(e *Engine, s signal.Series, ts int64, limits AppendLimits, materialize func() signal.Series) parallelAppendResult {
	res, err := e.AppendBatch([]signal.SeriesID{s.Hash()}, []int64{ts}, []float64{float64(ts)}, nil,
		func(int) signal.Series { return materialize() }, limits)
	return parallelAppendResult{result: res, err: err}
}

func TestColdWritesPrepareAndPublishIndependently(t *testing.T) {
	reproduce.Unfixed(t, 661, "a cold identity's private preparation serializes unrelated ingestion")
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
	reproduce.Unfixed(t, 661, "a conflicting request queues unrelated series behind writer ownership")
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
	reproduce.Unfixed(t, 661, "cardinality admission is inseparable from a cold writer's encoding")
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

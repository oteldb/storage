package engine

import (
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/internal/vfs/faultfs"
	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/signal"
)

type parallelAppendResult struct {
	result AppendResult
	err    error
}

func preparedFor(ids []signal.SeriesID) *preparedBatch {
	p := &preparedBatch{batch: newWALBatch()}
	p.prepare(ids)
	return p
}

func appendPreparedTest(e *Engine, ids []signal.SeriesID, ts []int64, sf []float64, limits AppendLimits) parallelAppendResult {
	values := make([]float64, len(ids))
	for i := range values {
		values[i] = float64(ts[i])
	}
	p := preparedFor(ids)
	res, err := e.appendPrepared(ids, ts, values, sf, func(int) signal.Series { panic("known identity materialized") }, limits, p)
	return parallelAppendResult{res, err}
}

func TestIndependentSeriesPrepareTogether(t *testing.T) {
	t.Parallel()
	e, fs := newFaultWALEngine(t, 100)
	a, b := stagedTestSeries("a"), stagedTestSeries("b")
	for _, s := range []signal.Series{a, b} {
		_, err := e.Append(s, 100, 100)
		require.NoError(t, err)
	}
	owned := make(chan struct{}, 2)
	release := make(chan struct{})
	resume := sync.OnceFunc(func() { close(release) })
	t.Cleanup(resume)
	e.parallel.hooks = &parallelHeadHooks{owned: func() { owned <- struct{}{}; <-release }}
	done := make(chan parallelAppendResult, 2)
	for _, id := range []signal.SeriesID{a.Hash(), b.Hash()} {
		go func() { done <- appendPreparedTest(e, []signal.SeriesID{id}, []int64{101}, nil, AppendLimits{}) }()
	}
	<-owned
	<-owned
	require.False(t, e.mu.TryLockWriter(), "lifecycle operations wait for all preparers")
	require.EqualValues(t, 2, e.Stats().HeadSamples)
	resume()
	for range 2 {
		r := <-done
		require.NoError(t, r.err)
		require.Equal(t, 1, r.result.Accepted)
	}
	for _, s := range []signal.Series{a, b} {
		require.Equal(t, []int64{100, 101}, headTS(e.head, s))
		require.Equal(t, headTS(e.head, s), headTS(replayedHead(t, fs), s))
	}
	require.Zero(t, e.parallel.pending.Load())
}

func TestParallelBatchVisibility(t *testing.T) {
	t.Parallel()
	e, _ := newFaultWALEngine(t, 0)
	series := []signal.Series{stagedTestSeries("a"), stagedTestSeries("b")}
	ids := make([]signal.SeriesID, len(series))
	for i, s := range series {
		_, err := e.Append(s, 1, 1)
		require.NoError(t, err)
		ids[i] = s.Hash()
		buf := e.head.samples[ids[i]]
		buf.ts = slices.Grow(buf.ts, 4)
		buf.values = slices.Grow(buf.values, 4)
	}
	before, _ := e.ReadRevision()
	ready, release := make(chan struct{}), make(chan struct{})
	resume := sync.OnceFunc(func() { close(release) })
	t.Cleanup(resume)
	e.parallel.hooks = &parallelHeadHooks{beforePublish: func() { close(ready); <-release }}
	done := make(chan parallelAppendResult, 1)
	go func() { done <- appendPreparedTest(e, ids, []int64{2, 2}, []float64{2, 1}, AppendLimits{}) }()
	<-ready
	require.EqualValues(t, 2, e.Stats().HeadSamples)
	now, _ := e.ReadRevision()
	require.Equal(t, before, now)
	for _, s := range series {
		require.Equal(t, []int64{1}, headTS(e.head, s))
	}
	resume()
	r := <-done
	require.NoError(t, r.err)
	require.Equal(t, 2, r.result.Accepted)
	after, _ := e.ReadRevision()
	require.NotEqual(t, before, after)
	for _, s := range series {
		require.Equal(t, []int64{1, 2}, headTS(e.head, s))
	}
	require.Equal(t, []float64{1, 2}, e.head.samples[ids[0]].sf)
	require.Nil(t, e.head.samples[ids[1]].sf)
}

func TestParallelWALFailureAndFreshIdentity(t *testing.T) {
	t.Parallel()
	e, fs := newFaultWALEngine(t, 50)
	known, fresh := stagedTestSeries("known"), stagedTestSeries("registered")
	_, err := e.Append(known, 100, 100)
	require.NoError(t, err)
	e.head.registerSeries(fresh)
	ids := []signal.SeriesID{known.Hash(), fresh.Hash()}
	before := snapshotHead(e.head)
	failNextWALWrite(fs)
	r := appendPreparedTest(e, ids, []int64{1000, 1000}, nil, AppendLimits{})
	require.ErrorIs(t, r.err, errInjectedWAL)
	require.Equal(t, AppendResult{}, r.result)
	require.Equal(t, before, snapshotHead(e.head))
	require.Zero(t, e.parallel.pending.Load())
	r = appendPreparedTest(e, ids, []int64{1000, 1000}, nil, AppendLimits{})
	require.NoError(t, r.err)
	require.Equal(t, 2, r.result.Accepted)
	replayed := replayedHead(t, fs)
	for _, s := range []signal.Series{known, fresh} {
		require.Equal(t, headTS(e.head, s), headTS(replayed, s))
	}
}

func TestConflictingParallelAdmissionUsesCommittedWatermark(t *testing.T) {
	t.Parallel()
	e, _ := newFaultWALEngine(t, 50)
	s := stagedTestSeries("same")
	_, err := e.Append(s, 100, 100)
	require.NoError(t, err)
	ready, release, fallback := make(chan struct{}), make(chan struct{}), make(chan struct{}, 1)
	resume := sync.OnceFunc(func() { close(release) })
	t.Cleanup(resume)
	e.parallel.hooks = &parallelHeadHooks{owned: sync.OnceFunc(func() { close(ready); <-release }), fallback: func() { fallback <- struct{}{} }}
	done := make(chan parallelAppendResult, 1)
	go func() { done <- appendPreparedTest(e, []signal.SeriesID{s.Hash()}, []int64{1000}, nil, AppendLimits{}) }()
	<-ready
	second := make(chan parallelAppendResult, 1)
	go func() {
		second <- appendPreparedTest(e, []signal.SeriesID{s.Hash()}, []int64{900}, nil, AppendLimits{})
	}()
	<-fallback
	resume()
	first := <-done
	require.NoError(t, first.err)
	r := <-second
	require.NoError(t, r.err)
	require.Equal(t, AppendResult{RejectedOOO: 1}, r.result)
	require.Equal(t, []int64{100, 1000}, headTS(e.head, s))
}

func TestParallelMemoryReservation(t *testing.T) {
	t.Parallel()
	e, _ := newFaultWALEngine(t, 0)
	a, b := stagedTestSeries("a"), stagedTestSeries("b")
	e.head.registerSeries(a)
	e.head.registerSeries(b)
	ready, release, fallback := make(chan struct{}), make(chan struct{}), make(chan struct{}, 1)
	resume := sync.OnceFunc(func() { close(release) })
	t.Cleanup(resume)
	e.parallel.hooks = &parallelHeadHooks{beforeWAL: func() { close(ready); <-release }, fallback: func() { fallback <- struct{}{} }}
	limits := AppendLimits{MaxInFlightBytes: SampleBytes}
	done := make(chan parallelAppendResult, 1)
	go func() { done <- appendPreparedTest(e, []signal.SeriesID{a.Hash()}, []int64{1}, nil, limits) }()
	<-ready
	second := make(chan parallelAppendResult, 1)
	go func() { second <- appendPreparedTest(e, []signal.SeriesID{b.Hash()}, []int64{1}, nil, limits) }()
	<-fallback
	resume()
	r := <-done
	require.NoError(t, r.err)
	require.Equal(t, 1, r.result.Accepted)
	r = <-second
	require.NoError(t, r.err)
	require.Equal(t, AppendResult{RejectedBytes: 1}, r.result)
	require.EqualValues(t, SampleBytes, e.HeadBytes())
	require.Zero(t, e.parallel.pending.Load())
}

func TestParallelFlushAndReplay(t *testing.T) {
	t.Parallel()
	e, fs := newFaultWALEngine(t, 0)
	e.cfg.Backend = backend.Memory()
	e.cfg.Prefix = "parallel"
	const writers = 4
	ids := make([]signal.SeriesID, writers)
	for i := range ids {
		s := stagedTestSeries(strconv.Itoa(i))
		e.head.registerSeries(s)
		ids[i] = s.Hash()
	}
	for round := range 4 {
		ready := make(chan struct{}, writers)
		release := make(chan struct{})
		resume := sync.OnceFunc(func() { close(release) })
		t.Cleanup(resume)
		e.parallel.hooks = &parallelHeadHooks{beforeWAL: func() { ready <- struct{}{}; <-release }}
		// The WAL owner blocks, while the other prepared batches hold lifecycle leases.
		done := make(chan parallelAppendResult, writers)
		for _, id := range ids {
			go func() {
				done <- appendPreparedTest(e, []signal.SeriesID{id}, []int64{int64(round)}, nil, AppendLimits{})
			}()
		}
		<-ready
		flushed := make(chan error, 1)
		go func() { flushed <- e.Flush(t.Context()) }()
		resume()
		for range writers {
			r := <-done
			require.NoError(t, r.err)
			require.Equal(t, 1, r.result.Accepted)
		}
		require.NoError(t, <-flushed)
	}
	e.parallel.hooks = nil
	require.NoError(t, e.Flush(t.Context()))
	require.NoError(t, e.SyncWAL())
	recovered := New(Config{Backend: e.cfg.Backend, Prefix: e.cfg.Prefix})
	require.NoError(t, recovered.LoadParts(t.Context()))
	// The persisted part rows include every acknowledged round, even when a seal races writers.
	it, err := recovered.Fetch(t.Context(), fetch.Request{Start: -1, End: 10})
	require.NoError(t, err)
	batches, err := fetch.Drain(t.Context(), it)
	require.NoError(t, err)
	require.Len(t, batches, writers)
	for _, b := range batches {
		require.Equal(t, []int64{0, 1, 2, 3}, b.Timestamps)
	}
	_ = replayedHead(t, fs)
	require.Zero(t, e.parallel.pending.Load())
}

func TestParallelScopeAndCopyBound(t *testing.T) {
	t.Parallel()
	e, _ := newFaultWALEngine(t, 0)
	known := stagedTestSeries("known")
	e.head.registerSeries(known)
	p := preparedFor([]signal.SeriesID{known.Hash()})
	p.batch.order = append(p.batch.order, known.Hash())
	p.batch.accs[0] = walSeriesAcc{buffer: &sampleBuf{
		ts: make([]int64, maxParallelHeadWork/16), values: make([]float64, maxParallelHeadWork/16),
	}, ts: []int64{1}, values: []float64{1}}
	require.False(t, p.parallelWorkFits(e.head.series.View()), "large copies use serialized admission")
	for _, cfg := range []Config{{}, {WAL: e.cfg.WAL, Term: func() uint64 { return 1 }}, {WAL: e.cfg.WAL, WriterID: "node"}} {
		require.Nil(t, New(cfg).parallel, "cluster and non-WAL engines retain their write path")
	}
}

func TestConcurrentPreparedReplayWithReaders(t *testing.T) {
	t.Parallel()
	e, fs := newFaultWALEngine(t, 0)
	identities := make([]signal.Series, 16)
	for i := range identities {
		identities[i] = stagedTestSeries("s" + strconv.Itoa(i))
		_, err := e.Append(identities[i], 0, 0)
		require.NoError(t, err)
	}
	stop, readerDone := make(chan struct{}), make(chan struct{})
	readerErr := make(chan error, 1)
	finish := sync.OnceFunc(func() { close(stop) })
	t.Cleanup(func() { finish(); <-readerDone })
	go func() {
		defer close(readerDone)
		for {
			select {
			case <-stop:
				readerErr <- nil
				return
			default:
			}
			it, err := e.Fetch(t.Context(), fetch.Request{Start: 0, End: 1 << 50, Matchers: []fetch.Matcher{{
				Name: []byte("__name__"), Match: func(v signal.Value) bool { return string(v.Str()) == "s0" },
			}}})
			if err != nil {
				readerErr <- err
				return
			}
			if _, err = fetch.Drain(t.Context(), it); err != nil {
				readerErr <- err
				return
			}
		}
	}()
	done := make(chan parallelAppendResult, 8)
	for worker := range 8 {
		go func() {
			ids := make([]signal.SeriesID, 1024)
			ts, values, sf := make([]int64, len(ids)), make([]float64, len(ids)), make([]float64, len(ids))
			for round := range 16 {
				for i := range ids {
					ids[i] = identities[(worker+round+i%2)%len(identities)].Hash()
					ts[i] = int64(worker*1000 + round + 1)
					values[i], sf[i] = float64(worker*1000+round), float64(1+round%3)
				}
				res, err := e.AppendBatch(ids, ts, values, sf, func(int) signal.Series { panic("known identity materialized") }, AppendLimits{})
				if err != nil || res.Accepted != len(ids) {
					done <- parallelAppendResult{res, err}
					return
				}
			}
			done <- parallelAppendResult{AppendResult{Accepted: 1024}, nil}
		}()
	}
	for range 8 {
		r := <-done
		require.NoError(t, r.err)
		require.Equal(t, 1024, r.result.Accepted)
	}
	finish()
	<-readerDone
	require.NoError(t, <-readerErr)
	require.Equal(t, snapshotHead(replayedHead(t, fs)), snapshotHead(e.head))
	require.Zero(t, e.parallel.pending.Load())
}

func TestParallelSyncFailureDoesNotPublish(t *testing.T) {
	t.Parallel()
	e, fs := newFaultWALEngine(t, 0)
	e.cfg.WAL.SetSync(true)
	s := stagedTestSeries("synced")
	_, err := e.Append(s, 1, 1)
	require.NoError(t, err)
	before := snapshotHead(e.head)
	fs.Add(faultfs.Rule{Op: faultfs.OpSync, Match: func(c faultfs.Call) bool { return strings.HasSuffix(c.Name, ".wal") }, Err: errInjectedWAL, Times: 1})
	r := appendPreparedTest(e, []signal.SeriesID{s.Hash()}, []int64{2}, nil, AppendLimits{})
	require.ErrorIs(t, r.err, errInjectedWAL)
	require.Equal(t, before, snapshotHead(e.head))
	require.Zero(t, e.parallel.pending.Load())
	r = appendPreparedTest(e, []signal.SeriesID{s.Hash()}, []int64{2}, nil, AppendLimits{})
	require.NoError(t, r.err)
	require.Equal(t, []int64{1, 2}, headTS(e.head, s))
}

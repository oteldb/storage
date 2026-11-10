package engine

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/internal/vfs/faultfs"
	"github.com/oteldb/storage/signal"
	"github.com/oteldb/storage/wal"
)

func enableHeadModes(t *testing.T, f *writableHeads, shared, queued bool, hooks *headDispatchHooks) {
	t.Helper()
	require.NoError(t, f.close())
	f.sharedMetadata, f.queued, f.prepared, f.hooks = shared, queued, true, hooks
	require.NoError(t, f.open(t.Context(), false))
}

func TestHeadDispatchCombinedAccounting(t *testing.T) {
	t.Parallel()
	for _, fail := range []bool{false, true} {
		name := "success"
		if fail {
			name = "WAL failure"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			f := newWritableHeads(t, 1)
			release := make(chan struct{})
			resume := sync.OnceFunc(func() { close(release) })
			queued, groups := make(chan int, 2), make(chan int, 2)
			enableHeadModes(t, f, true, true, &headDispatchHooks{
				beforeReceive: func(int) { <-release }, queued: func(i int) { queued <- i }, beforeWrite: func(_ int, n int) { groups <- n },
			})
			t.Cleanup(resume)
			if fail {
				fsys := faultfs.New()
				w, err := wal.CreateFS(fsys, 0)
				require.NoError(t, err)
				require.NoError(t, f.heads[0].cfg.WAL.Close())
				f.heads[0].cfg.WAL = w
				failNextWALWrite(fsys)
			}
			a, b := benchSeries(0), benchSeries(1)
			first, second := make(chan parallelAppendResult, 1), make(chan parallelAppendResult, 1)
			var wg sync.WaitGroup
			wg.Go(func() {
				var route headRouteBatch
				src := []signal.Series{a, b}
				r, err := route.append(t.Context(), f, []signal.SeriesID{a.Hash(), b.Hash()}, []int64{100, 100}, []float64{1, 2}, []float64{2, 1}, func(i int) signal.Series { return src[i] })
				first <- parallelAppendResult{r, err}
			})
			<-queued
			wg.Go(func() {
				var route headRouteBatch
				src := []signal.Series{a, b, a}
				r, err := route.append(t.Context(), f, []signal.SeriesID{a.Hash(), b.Hash(), a.Hash()}, []int64{101, 101, 80}, []float64{3, 4, 5}, []float64{3, 1, 9}, func(i int) signal.Series { return src[i] })
				second <- parallelAppendResult{r, err}
			})
			t.Cleanup(func() { resume(); wg.Wait() })
			<-queued
			resume()
			one, two := awaitDiskAppend(t, first), awaitDiskAppend(t, second)
			require.Equal(t, 2, <-groups)
			if fail {
				require.ErrorIs(t, one.err, errInjectedWAL)
				require.ErrorIs(t, two.err, errInjectedWAL)
				require.Equal(t, AppendResult{}, one.result)
				require.Equal(t, AppendResult{}, two.result)
				require.Zero(t, f.heads[0].Stats().HeadSamples)
				return
			}
			require.NoError(t, one.err)
			require.NoError(t, two.err)
			require.Equal(t, AppendResult{Accepted: 2}, one.result)
			require.Equal(t, AppendResult{Accepted: 2, RejectedOOO: 1}, two.result)
			rows := snapshotWritableHeads(t, f)
			require.Equal(t, []int64{100, 101}, rows[a.Hash()].ts)
			require.Equal(t, []float64{2, 3}, rows[a.Hash()].weights)
			require.Equal(t, []float64{1, 1}, rows[b.Hash()].weights)
			require.NoError(t, f.close())
			require.NoError(t, f.open(t.Context(), true))
			require.Equal(t, rows, snapshotWritableHeads(t, f))
		})
	}
}

func TestHeadDispatchCloseAndBounds(t *testing.T) {
	t.Parallel()
	f := newWritableHeads(t, 1)
	gate, entered := make(chan struct{}), make(chan struct{}, 1)
	release := sync.OnceFunc(func() { close(gate) })
	enableHeadModes(t, f, false, true, &headDispatchHooks{beforeWrite: func(int, int) { entered <- struct{}{}; <-gate }})
	t.Cleanup(release)
	s := benchSeries(1)
	done := make(chan parallelAppendResult, 1)
	finished := make(chan struct{})
	ctx, cancel := context.WithCancel(t.Context())
	go func() {
		defer close(finished)
		var route headRouteBatch
		r, err := route.append(ctx, f, []signal.SeriesID{s.Hash()}, []int64{100}, []float64{1}, nil, func(int) signal.Series { return s })
		done <- parallelAppendResult{r, err}
	}()
	t.Cleanup(func() { release(); <-finished })
	<-entered
	cancel()
	require.False(t, f.dispatch.mu.TryLock(), "admitted callers pin dispatcher lifetime")
	closed := make(chan struct{})
	go func() { f.dispatch.close(); close(closed) }()
	release()
	r := awaitDiskAppend(t, done)
	require.NoError(t, r.err)
	require.Equal(t, 1, r.result.Accepted)
	select {
	case <-closed:
	case <-time.After(30 * time.Second):
		t.Fatal("dispatcher did not drain")
	}
	var route headRouteBatch
	_, err := route.append(t.Context(), f, []signal.SeriesID{s.Hash()}, []int64{101}, []float64{2}, nil, func(int) signal.Series { return s })
	require.ErrorContains(t, err, "closed")
	require.Empty(t, f.dispatch.slots)
	require.Equal(t, dispatchRequests, cap(f.dispatch.slots))
	for _, q := range f.dispatch.queues {
		require.Equal(t, dispatchRequests, cap(q))
		require.Empty(t, q)
	}
}

func TestHeadDispatchConcurrentRecovery(t *testing.T) {
	t.Parallel()
	for _, variant := range []string{"shared", "queued", "combined"} {
		t.Run(variant, func(t *testing.T) {
			t.Parallel()
			f := newWritableHeads(t, 4)
			enableHeadModes(t, f, variant != "queued", variant != "shared", nil)
			const workers, points, rounds = 8, 16, 8
			failures := make(chan error, workers)
			var wg sync.WaitGroup
			for worker := range workers {
				wg.Go(func() {
					var route headRouteBatch
					ids, ts, values := make([]signal.SeriesID, points), make([]int64, points), make([]float64, points)
					for i := range ids {
						ids[i] = benchSeries(worker*points + i).Hash()
					}
					for round := range rounds {
						for i := range ts {
							ts[i], values[i] = int64(100+round), float64(round)
						}
						_, err := route.append(t.Context(), f, ids, ts, values, nil, func(i int) signal.Series { return benchSeries(worker*points + i) })
						if err != nil {
							failures <- err
							return
						}
					}
				})
			}
			wg.Wait()
			close(failures)
			for err := range failures {
				require.NoError(t, err)
			}
			want := snapshotWritableHeads(t, f)
			require.Len(t, want, workers*points)
			for _, row := range want {
				require.Len(t, row.ts, rounds)
			}
			if f.contexts != nil {
				require.LessOrEqual(t, f.contexts.SizeBytes(), int64(32<<20))
			}
			require.NoError(t, f.heads[0].Flush(t.Context()))
			require.NoError(t, f.close())
			require.NoError(t, f.open(t.Context(), true))
			require.Equal(t, want, snapshotWritableHeads(t, f))
			require.NoError(t, f.flush(t.Context()))
			require.NoError(t, f.close())
			require.NoError(t, f.open(t.Context(), true))
			require.Equal(t, want, snapshotWritableHeads(t, f))
		})
	}
}

func TestHeadDispatchCancelledAdmission(t *testing.T) {
	t.Parallel()
	f := newWritableHeads(t, 2)
	enableHeadModes(t, f, true, true, nil)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	var route headRouteBatch
	s := benchSeries(0)
	_, err := route.append(ctx, f, []signal.SeriesID{s.Hash()}, []int64{100}, []float64{1}, nil, func(int) signal.Series { return s })
	require.ErrorIs(t, err, context.Canceled)
	require.Empty(t, f.dispatch.slots)
	require.Empty(t, snapshotWritableHeads(t, f))
}

func TestHeadDispatchBoundedAdmission(t *testing.T) {
	t.Parallel()
	f := newWritableHeads(t, 1)
	gate := make(chan struct{})
	release := sync.OnceFunc(func() { close(gate) })
	queued := make(chan int, dispatchRequests)
	enableHeadModes(t, f, false, true, &headDispatchHooks{queued: func(i int) { queued <- i }, beforeWrite: func(int, int) { <-gate }})
	t.Cleanup(release)
	var wg sync.WaitGroup
	results := make(chan parallelAppendResult, dispatchRequests)
	for i := range dispatchRequests {
		wg.Go(func() {
			var route headRouteBatch
			s := benchSeries(i)
			r, err := route.append(t.Context(), f, []signal.SeriesID{s.Hash()}, []int64{100}, []float64{1}, nil, func(int) signal.Series { return s })
			results <- parallelAppendResult{r, err}
		})
	}
	t.Cleanup(func() { release(); wg.Wait() })
	for range dispatchRequests {
		<-queued
	}
	require.Len(t, f.dispatch.slots, dispatchRequests)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	route := headRouteBatch{}
	_, err := route.append(ctx, f, []signal.SeriesID{{Lo: 99}}, []int64{100}, []float64{1}, nil, func(int) signal.Series { panic("canceled materializer") })
	require.ErrorIs(t, err, context.Canceled)
	require.Len(t, f.dispatch.slots, dispatchRequests)
	release()
	wg.Wait()
	for range dispatchRequests {
		r := <-results
		require.NoError(t, r.err)
		require.Equal(t, 1, r.result.Accepted)
	}
	require.Empty(t, f.dispatch.slots)
	oversized := make([]signal.SeriesID, maxPreparedBatchSamples+1)
	_, err = route.append(ctx, f, oversized, nil, nil, nil, func(int) signal.Series { panic("oversized materializer") })
	require.ErrorContains(t, err, "at most 4096")
}

func TestHeadDispatchAllLanesBeforeWaiting(t *testing.T) {
	t.Parallel()
	f := newWritableHeads(t, 2)
	other := make(chan struct{}, 1)
	enableHeadModes(t, f, true, true, &headDispatchHooks{completed: func(lane int) {
		if lane == 1 {
			other <- struct{}{}
		}
	}})
	f.heads[0].mu.LockWriter()
	release := sync.OnceFunc(f.heads[0].mu.UnlockWriter)
	t.Cleanup(release)
	src := []signal.Series{seriesForLane(0, 2), seriesForLane(1, 2)}
	done, finished := make(chan parallelAppendResult, 1), make(chan struct{})
	go func() {
		defer close(finished)
		var route headRouteBatch
		r, err := route.append(t.Context(), f, []signal.SeriesID{src[0].Hash(), src[1].Hash()}, []int64{100, 100}, []float64{1, 2}, nil, func(i int) signal.Series { return src[i] })
		done <- parallelAppendResult{r, err}
	}()
	t.Cleanup(func() { release(); <-finished })
	select {
	case <-other:
	case <-time.After(30 * time.Second):
		t.Fatal("request waited on head zero before submitting head one")
	}
	require.EqualValues(t, 1, f.heads[1].Stats().HeadSamples)
	select {
	case <-done:
		t.Fatal("caller returned before all admitted lanes finished")
	default:
	}
	release()
	r := awaitDiskAppend(t, done)
	require.NoError(t, r.err)
	require.Equal(t, 2, r.result.Accepted)
}

func TestHeadDispatchGroupPointLimit(t *testing.T) {
	t.Parallel()
	f := newWritableHeads(t, 1)
	gate := make(chan struct{})
	release := sync.OnceFunc(func() { close(gate) })
	queued, groups := make(chan int, 3), make(chan int, 3)
	enableHeadModes(t, f, false, true, &headDispatchHooks{beforeReceive: func(int) { <-gate }, queued: func(i int) { queued <- i }, beforeWrite: func(_ int, n int) { groups <- n }})
	t.Cleanup(release)
	s := benchSeries(0)
	results := make([]chan parallelAppendResult, 3)
	var wg sync.WaitGroup
	for request, points := range []int{2048, 2048, 1} {
		results[request] = make(chan parallelAppendResult, 1)
		wg.Go(func() {
			var route headRouteBatch
			ids, ts, values := make([]signal.SeriesID, points), make([]int64, points), make([]float64, points)
			stamp := int64(100 + request)
			if request == 2 {
				stamp = 80
			}
			for i := range ids {
				ids[i], ts[i], values[i] = s.Hash(), stamp, 1
			}
			r, err := route.append(t.Context(), f, ids, ts, values, nil, func(int) signal.Series { return s })
			results[request] <- parallelAppendResult{r, err}
		})
		<-queued
	}
	t.Cleanup(func() { release(); wg.Wait() })
	release()
	for i, done := range results {
		r := awaitDiskAppend(t, done)
		require.NoError(t, r.err)
		if i < 2 {
			require.Equal(t, 2048, r.result.Accepted)
		} else {
			require.Equal(t, 1, r.result.RejectedOOO)
		}
	}
	require.Equal(t, 2, <-groups)
	require.Equal(t, 1, <-groups)
	require.EqualValues(t, maxPreparedBatchSamples, f.heads[0].Stats().HeadSamples)
}

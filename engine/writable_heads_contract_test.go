package engine

import (
	"context"
	"math"
	"slices"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/go-faster/errors"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/internal/vfs/faultfs"
	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/signal"
	"github.com/oteldb/storage/wal"
)

func TestWritableHeadsSharedWorkspace(t *testing.T) {
	t.Parallel()
	f := newWritableHeads(t, 2)
	release, err := f.cfgs[0].FlushAdmission(t.Context(), 64<<20)
	require.NoError(t, err)
	release = sync.OnceFunc(release)
	defer release()
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	_, err = f.cfgs[1].FlushAdmission(ctx, 64<<20)
	require.ErrorIs(t, err, context.Canceled, "workspace belongs to the group, not each head")
	_, err = f.cfgs[1].FlushAdmission(t.Context(), 256<<20+1)
	require.ErrorContains(t, err, "shared cap")
	release()
	mergeRelease, ok, err := f.cfgs[0].MergeAdmission(t.Context(), 256<<20, true)
	require.NoError(t, err)
	require.True(t, ok)
	defer mergeRelease()
	_, ok, err = f.cfgs[1].MergeAdmission(t.Context(), 1, false)
	require.NoError(t, err)
	require.False(t, ok)
}

type headSamples struct {
	ts      []int64
	bits    []uint64
	weights []float64
}

func snapshotWritableHeads(t *testing.T, f *writableHeads) map[signal.SeriesID]headSamples {
	t.Helper()
	it, err := f.fetcher().Fetch(t.Context(), fetch.Request{Start: 0, End: 1000})
	require.NoError(t, err)
	rows, err := fetch.Drain(t.Context(), it)
	require.NoError(t, err)
	result := make(map[signal.SeriesID]headSamples, len(rows))
	for _, row := range rows {
		require.Equal(t, row.ID, row.Series.Hash())
		out := headSamples{ts: slices.Clone(row.Timestamps)}
		for i, v := range row.Values {
			out.bits = append(out.bits, math.Float64bits(v))
			out.weights = append(out.weights, row.ScaleFactor(i))
		}
		result[row.ID] = out
	}
	return result
}

func TestWritableHeadsRecovery(t *testing.T) {
	t.Parallel()
	for _, n := range []int{1, 4, 8} {
		t.Run(strconv.Itoa(n), func(t *testing.T) {
			t.Parallel()
			f := newWritableHeads(t, n)
			f.prepared = true
			const count = 32
			ids, ts, values, weights := make([]signal.SeriesID, count*2), make([]int64, count*2), make([]float64, count*2), make([]float64, count*2)
			for i := range ids {
				ids[i], ts[i], values[i], weights[i] = benchSeries(i%count).Hash(), 100+int64(i/count), float64(i), float64(i%3+1)
			}
			values[0] = math.Float64frombits(1 << 63)
			values[1] = math.Float64frombits(0x7ff8000000000042)
			values[2] = math.Inf(-1)
			values[3] = math.SmallestNonzeroFloat64
			var route headRouteBatch
			r, err := route.append(t.Context(), f, ids, ts, values, weights, func(i int) signal.Series { return benchSeries(i % count) })
			require.NoError(t, err)
			require.Equal(t, len(ids), r.Accepted)
			want := snapshotWritableHeads(t, f)
			require.Len(t, want, count)
			for i := range count {
				row := want[ids[i]]
				require.Equal(t, []int64{100, 101}, row.ts)
				require.Equal(t, []uint64{math.Float64bits(values[i]), math.Float64bits(values[i+count])}, row.bits)
				require.Equal(t, []float64{weights[i], weights[i+count]}, row.weights)
			}
			// One lane checkpoints independently while every other lane remains WAL-only.
			require.NoError(t, f.heads[0].Flush(t.Context()))
			require.NoError(t, f.close())
			require.NoError(t, f.open(t.Context(), true))
			require.Equal(t, want, snapshotWritableHeads(t, f))
			for i := range count {
				ts[i], values[i] = 102, float64(i)
			}
			r, err = route.append(t.Context(), f, ids[:count], ts[:count], values[:count], nil, benchSeries)
			require.NoError(t, err)
			require.Equal(t, count, r.Accepted)
			want = snapshotWritableHeads(t, f)
			require.NoError(t, f.flush(t.Context()))
			for _, h := range f.heads {
				require.NoError(t, h.MergeWith(t.Context(), MergeOptions{Force: true}))
			}
			require.Equal(t, want, snapshotWritableHeads(t, f))
			for i, h := range f.heads {
				for id := range h.head.samples {
					require.Equal(t, i, int(id.Lo%uint64(n)))
				}
			}
			for i := range count {
				ts[i] = 80
			}
			r, err = route.append(t.Context(), f, ids[:count], ts[:count], values[:count], nil, benchSeries)
			require.NoError(t, err)
			require.Equal(t, AppendResult{RejectedOOO: count}, r)
			require.NoError(t, f.flush(t.Context()))
			require.NoError(t, f.close())
			require.NoError(t, f.open(t.Context(), true))
			require.Equal(t, want, snapshotWritableHeads(t, f))
			var resident, cache int64
			for _, cfg := range f.cfgs {
				resident += cfg.ResidentHeadBytes
				cache += cfg.SeriesCacheBytes
				require.EqualValues(t, 64<<20, cfg.FlushMemoryBytes)
				require.EqualValues(t, 256<<20, cfg.MergeMemoryBytes)
			}
			require.EqualValues(t, 128<<20, resident)
			require.EqualValues(t, 512<<20, cache)
		})
	}
}

func TestWritableHeadsConcurrentRecovery(t *testing.T) {
	t.Parallel()
	f := newWritableHeads(t, 4)
	f.prepared = true
	const workers, series, rounds = 8, 16, 8
	failures := make(chan error, workers)
	var wg sync.WaitGroup
	for worker := range workers {
		wg.Go(func() {
			var route headRouteBatch
			ids, ts, values := make([]signal.SeriesID, series), make([]int64, series), make([]float64, series)
			for i := range ids {
				ids[i] = benchSeries(worker*series + i).Hash()
			}
			for round := range rounds {
				for i := range ts {
					ts[i], values[i] = int64(100+round), float64(round)
				}
				r, err := route.append(t.Context(), f, ids, ts, values, nil, func(i int) signal.Series { return benchSeries(worker*series + i) })
				if err != nil {
					failures <- err
					return
				}
				if r.Accepted != series {
					failures <- errors.Errorf("accepted %d, want %d", r.Accepted, series)
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
	require.Len(t, want, workers*series)
	for _, row := range want {
		require.Len(t, row.ts, rounds)
	}
	require.NoError(t, f.close())
	require.NoError(t, f.open(t.Context(), true))
	require.Equal(t, want, snapshotWritableHeads(t, f))
}

func seriesForLane(lane, count int) signal.Series {
	for i := 0; ; i++ {
		s := benchSeries(i)
		if int(s.Hash().Lo%uint64(count)) == lane {
			return s
		}
	}
}

func TestWritableHeadsIndependentWriters(t *testing.T) {
	t.Parallel()
	f := newWritableHeads(t, 2)
	first, second := seriesForLane(0, 2), seriesForLane(1, 2)
	f.heads[0].mu.LockWriter()
	release := sync.OnceFunc(f.heads[0].mu.UnlockWriter)
	t.Cleanup(release)
	done := make(chan parallelAppendResult, 1)
	finished := make(chan struct{})
	go func() {
		defer close(finished)
		var route headRouteBatch
		r, err := route.append(t.Context(), f, []signal.SeriesID{second.Hash()}, []int64{100}, []float64{1}, nil, func(int) signal.Series { return second })
		done <- parallelAppendResult{r, err}
	}()
	t.Cleanup(func() { release(); <-finished })
	select {
	case r := <-done:
		require.NoError(t, r.err)
		require.Equal(t, 1, r.result.Accepted)
	case <-time.After(30 * time.Second):
		t.Fatal("independent lane waited for unrelated writer")
	}
	release()
	var route headRouteBatch
	r, err := route.append(t.Context(), f, []signal.SeriesID{first.Hash()}, []int64{100}, []float64{1}, nil, func(int) signal.Series { return first })
	require.NoError(t, err)
	require.Equal(t, 1, r.Accepted)
}

func TestWritableHeadsPartialWALFailure(t *testing.T) {
	t.Parallel()
	f := newWritableHeads(t, 2)
	fsys := faultfs.New()
	w, err := wal.CreateFS(fsys, 0)
	require.NoError(t, err)
	require.NoError(t, f.heads[1].cfg.WAL.Close())
	f.heads[1].cfg.WAL = w
	failNextWALWrite(fsys)
	src := []signal.Series{seriesForLane(0, 2), seriesForLane(1, 2)}
	ids := []signal.SeriesID{src[0].Hash(), src[1].Hash()}
	var route headRouteBatch
	r, err := route.append(t.Context(), f, ids, []int64{100, 100}, []float64{1, 2}, nil, func(i int) signal.Series { return src[i] })
	require.ErrorIs(t, err, errInjectedWAL)
	require.Equal(t, 1, r.Accepted, "the prototype reports committed earlier lanes; it is not batch-atomic")
	require.EqualValues(t, 1, f.heads[0].Stats().HeadSamples)
	require.Zero(t, f.heads[1].Stats().HeadSamples)
}

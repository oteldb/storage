package engine

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/internal/vfs/faultfs"
	"github.com/oteldb/storage/signal"
	"github.com/oteldb/storage/wal"
)

func TestHeadDispatchResidentFailureAccounting(t *testing.T) {
	t.Parallel()
	f := newWritableHeads(t, 1)
	h := f.heads[0]
	fsys := faultfs.New()
	w, err := wal.CreateFS(fsys, 0)
	require.NoError(t, err)
	require.NoError(t, h.cfg.WAL.Close())
	h.cfg.WAL = w
	s := benchSeries(0)
	_, err = h.Append(s, 50, 1)
	require.NoError(t, err)
	require.NoError(t, h.Flush(t.Context()))
	_, err = h.Append(s, 60, 2)
	require.NoError(t, err)
	require.True(t, residentDispatchHistory(h))
	gate := make(chan struct{})
	release := sync.OnceFunc(func() { close(gate) })
	queued := make(chan int, 2)
	f.dispatch = newHeadDispatcher(t.Context(), f.heads, true, &headDispatchHooks{beforeReceive: func(int) { <-gate }, queued: func(i int) { queued <- i }})
	t.Cleanup(release)
	failNextWALWrite(fsys)
	results := make([]chan parallelAppendResult, 2)
	var wg sync.WaitGroup
	for i := range results {
		results[i] = make(chan parallelAppendResult, 1)
		wg.Go(func() {
			var route headRouteBatch
			r, err := route.append(t.Context(), f, []signal.SeriesID{s.Hash()}, []int64{int64(100 + i)}, []float64{3}, nil, func(int) signal.Series { return s })
			results[i] <- parallelAppendResult{r, err}
		})
		<-queued
	}
	t.Cleanup(func() { release(); wg.Wait() })
	release()
	first, second := awaitDiskAppend(t, results[0]), awaitDiskAppend(t, results[1])
	require.ErrorIs(t, first.err, errInjectedWAL)
	require.Equal(t, AppendResult{}, first.result)
	require.NoError(t, second.err)
	require.Equal(t, AppendResult{Accepted: 1}, second.result)
	require.Equal(t, []int64{60, 101}, headTS(h.head, s))
	require.Equal(t, headTS(h.head, s), headTS(replayedHead(t, fsys), s))
}

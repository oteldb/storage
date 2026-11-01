package engine

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/signal"
)

type warmWriteResult struct {
	result AppendResult
	err    error
}

func TestMemoryWarmTransferWithRejectedGaps(t *testing.T) {
	t.Parallel()
	e, _ := newFaultWALEngine(t, 5)
	first, second := stagedTestSeries("first"), stagedTestSeries("second")
	e.head.registerSeries(first)
	e.head.registerSeries(second)
	enableMemoryPublicationTest(t, e)
	_, err := e.Append(first, 10, 1)
	require.NoError(t, err)
	_, err = e.Append(second, 10, 2)
	require.NoError(t, err)
	ids := []signal.SeriesID{first.Hash(), second.Hash(), first.Hash(), second.Hash()}
	result, err := e.AppendBatch(ids, []int64{0, 5, 20, 6}, []float64{0, 3, 4, 5}, []float64{1, 2, 3, 4},
		func(int) signal.Series { panic("known identity materialized") }, AppendLimits{})
	require.NoError(t, err)
	require.Equal(t, 3, result.Accepted)
	iterator, err := e.Fetch(t.Context(), fetch.Request{Start: 0, End: 30})
	require.NoError(t, err)
	defer func() { require.NoError(t, iterator.Close()) }()
	for range 2 {
		batch, err := iterator.Next(t.Context())
		require.NoError(t, err)
		if batch.Series.Hash() == first.Hash() {
			require.Equal(t, []int64{10, 20}, batch.Timestamps)
			require.Equal(t, []float64{1, 4}, batch.Values)
			require.Equal(t, []float64{1, 3}, batch.ScaleFactors)
		} else {
			require.Equal(t, []int64{5, 6, 10}, batch.Timestamps)
			require.Equal(t, []float64{3, 5, 2}, batch.Values)
			require.Equal(t, []float64{2, 4, 1}, batch.ScaleFactors)
		}
	}
}

func TestEightWarmWritersOverlapAndPublishAtomically(t *testing.T) {
	t.Parallel()
	const writers, points = 8, 4096
	e, _ := newFaultWALEngine(t, 5)
	ids := make([][]signal.SeriesID, writers)
	for writer := range writers {
		ids[writer] = make([]signal.SeriesID, points)
		for point := range points {
			series := signal.Series{Attributes: signal.NewAttributes(signal.KeyValue{
				Key: []byte("id"), Value: signal.IntValue(int64(writer*points + point)),
			})}
			e.head.registerSeries(series)
			ids[writer][point] = series.Hash()
		}
	}
	enableMemoryPublicationTest(t, e)
	reserved, published := make(chan struct{}), make(chan struct{})
	resumePreparation, resumeFinalization := make(chan struct{}), make(chan struct{})
	releasePreparation := sync.OnceFunc(func() { close(resumePreparation) })
	releaseFinalization := sync.OnceFunc(func() { close(resumeFinalization) })
	t.Cleanup(releasePreparation)
	t.Cleanup(releaseFinalization)
	e.warmHooks = &memoryWarmHooks{
		reserved: func(batch []signal.SeriesID) {
			if len(batch) == points && batch[0] == ids[0][0] {
				close(reserved)
				<-resumePreparation
			}
		},
		published: func(batch []signal.SeriesID) {
			if len(batch) == points && batch[0] == ids[0][0] {
				close(published)
				<-resumeFinalization
			}
		},
	}
	queuedSuccessor := make(chan struct{}, 1)
	e.reservations.onWait = func(id signal.SeriesID) {
		if id == ids[0][0] {
			queuedSuccessor <- struct{}{}
		}
	}
	write := func(batch []signal.SeriesID, timestamp int64) warmWriteResult {
		ts, values := make([]int64, len(batch)), make([]float64, len(batch))
		for i := range batch {
			ts[i], values[i] = timestamp, float64(timestamp)
		}
		result, err := e.AppendBatch(batch, ts, values, nil,
			func(int) signal.Series { panic("warm path materialized an identity") }, AppendLimits{})
		return warmWriteResult{result, err}
	}
	first := make(chan warmWriteResult, 1)
	go func() { first <- write(ids[0], 10) }()
	<-reserved
	others := make(chan warmWriteResult, writers-1)
	for writer := 1; writer < writers; writer++ {
		go func() { others <- write(ids[writer], 10) }()
	}
	for range writers - 1 {
		select {
		case done := <-others:
			require.NoError(t, done.err)
			require.Equal(t, points, done.result.Accepted)
		case <-time.After(5 * time.Second):
			t.Fatal("disjoint writers were blocked by the paused reservation owner")
		}
	}
	releasePreparation()
	<-published
	count, err := e.Count(t.Context(), fetch.Request{Start: 0, End: 100})
	require.NoError(t, err)
	require.Equal(t, writers*points, count, "the entire run is visible before local finalization")
	successor := make(chan warmWriteResult, 1)
	go func() { successor <- write(ids[0][:1], 0) }()
	<-queuedSuccessor
	disjoint := write(ids[1], 20)
	require.NoError(t, disjoint.err)
	require.Equal(t, points, disjoint.result.Accepted)
	releaseFinalization()
	require.NoError(t, (<-first).err)
	next := <-successor
	require.NoError(t, next.err)
	require.Zero(t, next.result.Accepted, "the successor observes the owner's finalized OOO watermark")
}

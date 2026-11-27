package engine

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/signal"
)

func TestPressureGenerationsWithoutWAL(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	e := New(Config{
		Backend: backend.Memory(), Prefix: "unlogged",
		DiskSeries: true, MetricSeriesChunks: true, SeparateSeries: true,
		AsyncPressureFlush: true, PressureGenerations: 2,
		ResidentHeadBytes: 1 << 30, HeadRotationBytes: 1 << 28,
	})
	require.True(t, e.fixedHeadRotation())
	s := benchSeries(1)
	for i := range 2 {
		accepted, err := e.Append(s, int64(i+1), 1)
		require.NoError(t, err)
		require.True(t, accepted)
		e.mu.Lock()
		err = e.freezePressureGenerationLocked(ctx)
		e.mu.Unlock()
		require.NoError(t, err)
	}
	require.Len(t, e.pressureGenerations, 2)
	for i, g := range e.pressureGenerations {
		require.Equal(t, uint64(i+1), g.epoch)
		require.Equal(t, -1, g.sequence)
	}

	// An appended batch can prepare and publish an encoded returning descriptor even without
	// creating a WAL frame. Include more than one series to exercise grouping.
	s2 := benchSeries(2)
	ids := make([]signal.SeriesID, minPreparedBatchSamples)
	times := make([]int64, len(ids))
	values := make([]float64, len(ids))
	for i := range ids {
		ids[i] = s.Hash()
		if i%2 != 0 {
			ids[i] = s2.Hash()
		}
		times[i] = int64(i + 3)
		values[i] = 1
	}
	result, err := e.AppendBatch(ids, times, values, nil, func(i int) signal.Series {
		if i%2 != 0 {
			return s2
		}
		return s
	}, AppendLimits{})
	require.NoError(t, err)
	require.Equal(t, len(ids), result.Accepted)
	require.NotEmpty(t, e.head.encodedRefs)
	require.NoError(t, e.Close(ctx))
}

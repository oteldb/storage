package engine

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/signal"
	"github.com/oteldb/storage/wal"
)

func TestMemoryReplayBatchesAcrossSeriesFrames(t *testing.T) {
	t.Parallel()
	e, _ := newFaultWALEngine(t, 0)
	enableMemoryPublicationTest(t, e)
	var data bytes.Buffer
	w := wal.NewWriter(&data)
	const points = maxPreparedBatchSamples + 17
	for i := range points {
		s := signal.Series{Attributes: signal.NewAttributes(signal.KeyValue{Key: []byte("host"), Value: signal.IntValue(int64(i))})}
		require.NoError(t, w.WriteSeries(s.Hash(), s))
		require.NoError(t, w.WriteSamples(s.Hash(), []int64{10}, []float64{float64(i)}))
	}
	require.NoError(t, e.ApplyReplicated(data.Bytes()))
	require.Equal(t, points, e.HeadSampleCount())
	require.Equal(t, 2, e.readView.active.runs)
	count, err := e.Count(t.Context(), fetch.Request{Start: 0, End: 20})
	require.NoError(t, err)
	require.Equal(t, points, count)
	require.True(t, e.walB.empty())
}

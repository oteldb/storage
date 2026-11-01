package engine

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend/backendtest"
	backendfile "github.com/oteldb/storage/backend/file"
	"github.com/oteldb/storage/signal"
)

func TestCountStreamingEstimateReadsNoIndex(t *testing.T) {
	t.Parallel()
	files, err := backendfile.New(t.TempDir())
	require.NoError(t, err)
	counted := backendtest.NewSizedByteCounter(files)
	e := New(Config{Backend: counted, Prefix: "stream-count", MetricBlockRows: 4})
	s := apiSeries()
	for ts := range 64 {
		_, err := e.Append(s, int64(ts), float64(ts))
		require.NoError(t, err)
	}
	require.NoError(t, e.Flush(t.Context()))
	require.Len(t, e.parts, 1)
	e.parts[0].acquire()
	p := &enginePlan{engine: e, ids: []signal.SeriesID{s.Hash()}, liveParts: e.parts}
	p.prepareTimestampReader(e.parts[0])
	defer p.releaseParts()
	counted.Reset()
	bytes, bounded := p.countStreamingEstimate()
	require.True(t, bounded)
	require.Positive(t, bytes)
	require.Zero(t, counted.Bytes(), "admission must use manifests, not probe candidate row ranges")
	require.Empty(t, p.partRanges)
	require.Empty(t, p.partPositions)
	require.True(t, p.blockReaders[e.parts[0]].countStreaming)
}

package engine

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/backend/backendtest"
)

func TestChunkGeometryDoesNotCloneOnNumericHits(t *testing.T) {
	t.Parallel()
	counter := backendtest.NewByteCounter(backend.Memory())
	e := New(Config{Backend: counter, Prefix: "geometry", MetricSeriesChunks: true, MetricBlockRows: 4, DecodeCacheBytes: 1 << 20})
	for i := range 10 {
		_, err := e.Append(apiSeries(), int64(i+1), float64(i))
		require.NoError(t, err)
	}
	require.NoError(t, e.Flush(t.Context()))
	p := e.parts[0]
	warm := e.newSeriesBlockReader(p)
	require.NoError(t, warm.warm(t.Context(), []int{0}))
	warm.releasePins()
	counter.Reset()
	r := e.newSeriesBlockReader(p)
	defer r.releasePins()
	ts, err := r.tsBlock(t.Context(), 0)
	require.NoError(t, err)
	values, err := r.valBlock(t.Context(), 0)
	require.NoError(t, err)
	require.Equal(t, []int64{1, 2, 3, 4}, ts)
	require.Equal(t, []float64{0, 1, 2, 3}, values)
	require.Nil(t, r.tsDec)
	require.Nil(t, r.valDec)
	require.NotNil(t, r.decoderPins[colTsID])
	require.NotNil(t, r.decoderPins[colValID])
	require.Zero(t, counter.Reads())

	e.blockCache.evictPrefix(p.prefix)
	lo, hi := r.geometry.BlockSpan(1)
	require.Equal(t, 4, lo)
	require.Equal(t, 8, hi)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	_, err = r.partDecoder(ctx, colTs)
	require.ErrorIs(t, err, context.Canceled)
	ts, err = r.tsBlock(t.Context(), 1)
	require.NoError(t, err)
	require.Equal(t, []int64{5, 6, 7, 8}, ts)
	require.NotNil(t, r.tsDec)
	require.NotSame(t, r.metadata[colTsID], r.tsDec)
	r.releasePins()
	require.Nil(t, r.geometry)
	require.Empty(t, r.metadata)
	require.Empty(t, r.decoderPins)
}

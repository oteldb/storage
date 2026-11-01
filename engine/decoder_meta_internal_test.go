package engine

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/backend/backendtest"
)

func TestDecoderMetadataCacheAvoidsRepeatedReads(t *testing.T) {
	t.Parallel()
	counter := backendtest.NewByteCounter(backend.Memory())
	e := New(Config{Backend: counter, Prefix: "decoder-meta", DecodeCacheBytes: 1 << 20, MetricBlockRows: 32})
	for i := range 256 {
		_, err := e.Append(apiSeries(), int64(i), float64(i))
		require.NoError(t, err)
	}
	require.NoError(t, e.Flush(t.Context()))
	p := e.parts[0]
	ctx, cancel := context.WithCancel(t.Context())
	counter.Reset()
	first, firstPin, err := e.partBlockDecoder(ctx, p, colTs, colTsID)
	require.NoError(t, err)
	require.Positive(t, counter.Reads())
	e.blockCache.release(firstPin)
	cancel()
	counter.Reset()
	second, pin, err := e.partBlockDecoder(t.Context(), p, colTs, colTsID)
	require.NoError(t, err)
	defer e.blockCache.release(pin)
	require.NotSame(t, first, second)
	require.Zero(t, counter.Reads(), "metadata cache hit must not reopen the directory")
	values, err := second.DecodeInt64(0)
	require.NoError(t, err)
	require.Equal(t, int64(0), values[0])
	e.blockCache.mu.Lock()
	bytes, budget := e.blockCache.bytes, e.blockCache.maxBytes
	e.blockCache.mu.Unlock()
	require.LessOrEqual(t, bytes, budget)
	e.blockCache.evictPrefix(p.prefix)
	values, err = second.DecodeInt64(second.NumBlocks() - 1)
	require.NoError(t, err, "eviction cannot invalidate a pinned decoder")
	require.Equal(t, int64(255), values[len(values)-1])
}

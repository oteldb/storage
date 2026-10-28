package engine

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/backend/backendtest"
	"github.com/oteldb/storage/encoding/chunk"
	"github.com/oteldb/storage/internal/obs"
	"github.com/oteldb/storage/signal"
)

func TestSeriesIndexPageReads(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	const n = indexPageEntries * 8
	col := make([]chunk.U128, n)
	for i := range col {
		col[i] = chunk.U128{Lo: uint64((i + 1) * 2)}
	}
	data := encodeSeriesIndex(col)
	inner := backend.Memory()
	require.NoError(t, inner.Write(ctx, sidxKey("p"), data))
	count := backendtest.NewByteCounter(inner)
	cached := backend.Cached(count, 64<<10)
	paged, ok := openPagedIndex(ctx, cached, "p", n, obs.NewNop().Corruption)
	require.True(t, ok)
	require.Len(t, paged.fences, 8)
	paged.drop()
	count.Reset()
	idx := partIndex{paged: paged}
	for _, i := range []int{0, indexPageEntries - 1, indexPageEntries, n - 1} {
		r, found, err := idx.lookup(ctx, signal.SeriesID{Lo: uint64((i + 1) * 2)})
		require.NoError(t, err)
		require.True(t, found)
		assert.Equal(t, rowRange{start: i, end: i + 1}, r)
	}
	assert.Less(t, count.Bytes(), int64(len(data)/2), count.Report())
	assert.Nil(t, paged.view.Load(), "point probes must not pin the whole index")
	before := count.Bytes()
	_, _, err := idx.lookup(ctx, signal.SeriesID{Lo: uint64(n * 2)})
	require.NoError(t, err)
	assert.Equal(t, before, count.Bytes(), "repeated page hits share the backend cache")

	ids := []signal.SeriesID{{Lo: 0}, {Lo: 2}, {Lo: 3}, {Lo: uint64(n * 2)}, {Hi: 1}}
	active := []bool{false, false, false, false, true}
	require.NoError(t, idx.intersectMark(ctx, ids, active))
	assert.Equal(t, []bool{false, true, false, true, true}, active)
}

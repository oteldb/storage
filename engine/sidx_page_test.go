package engine

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/backend/backendtest"
	"github.com/oteldb/storage/backend/file"
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
	assert.Nil(t, paged.view.Load(), "opening a paged index must not pin the validation buffer")
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

func TestPagedIndexDoesNotRetainValidationBuffer(t *testing.T) {
	t.Parallel()
	for _, cached := range []bool{false, true} {
		name := "file"
		if cached {
			name = "cached-file"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			const n = indexPageEntries * 4
			col := make([]chunk.U128, n)
			for i := range col {
				col[i] = chunk.U128{Lo: uint64(i + 1)}
			}
			data := encodeSeriesIndex(col)
			files, err := file.New(t.TempDir())
			require.NoError(t, err)
			require.NoError(t, files.Write(t.Context(), sidxKey("p"), data))
			counter := backendtest.NewByteCounter(files)
			var be backend.Backend = counter
			if cached {
				be = backend.Cached(be, 256<<10)
			}
			index, ok := openPagedIndex(t.Context(), be, "p", n, obs.NewNop().Corruption)
			require.True(t, ok)
			require.Len(t, index.fences, 4)
			require.Nil(t, index.view.Load(), "no first query is required to release whole-index bytes")
			if cached {
				stats := be.(interface{ Stats() backend.CacheStats }).Stats()
				assert.Zero(t, stats.Items, "validation must not populate the cache with the whole index")
			}
			counter.Reset()
			got, found, err := index.lookup(t.Context(), signal.SeriesID{Lo: n})
			require.NoError(t, err)
			require.True(t, found)
			assert.Equal(t, rowRange{start: n - 1, end: n}, got)
			assert.LessOrEqual(t, counter.Bytes(), int64((indexPageEntries+1)*sidxEntryW))
			assert.Nil(t, index.view.Load(), "a point lookup retains no full-index view")
		})
	}
}

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

func TestPagedLookupMatchesResidentRanges(t *testing.T) {
	t.Parallel()
	const count = indexPageEntries*3 + 7
	var col []chunk.U128
	for i := range count {
		id := chunk.U128{Hi: uint64(i / 64), Lo: uint64(i%64+1) * 2}
		for range 1 + i%3 {
			col = append(col, id)
		}
	}
	data := encodeSeriesIndex(col)
	want := buildPartIndex(col)
	for _, resident := range []bool{false, true} {
		name := "pages"
		if resident {
			name = "resident"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			be := backend.Memory()
			require.NoError(t, be.Write(t.Context(), sidxKey("p"), data))
			counter := backendtest.NewByteCounter(be)
			idx, ok := openPagedIndex(t.Context(), backend.Cached(counter, 256<<10), "p", len(col), obs.NewNop().Corruption)
			require.True(t, ok)
			require.Len(t, idx.fences, 4)
			if resident {
				_, err := idx.entries(t.Context())
				require.NoError(t, err)
			}
			counter.Reset()
			for i := range count {
				for _, delta := range []uint64{0, 1} {
					id := signal.SeriesID{Hi: uint64(i / 64), Lo: uint64(i%64+1)*2 + delta}
					expected, found, err := want.lookup(t.Context(), id)
					require.NoError(t, err)
					got, actual, err := idx.lookup(t.Context(), id)
					require.NoError(t, err)
					require.Equal(t, found, actual)
					require.Equal(t, expected, got)
				}
			}
			for _, id := range []signal.SeriesID{{}, {Hi: ^uint64(0), Lo: ^uint64(0)}} {
				_, found, err := idx.lookup(t.Context(), id)
				require.NoError(t, err)
				require.False(t, found)
			}
			if resident {
				assert.Zero(t, counter.Bytes(), "resident lookup borrows pages without backend reads")
			}
		})
	}
}

func FuzzPagedIndexLookup(f *testing.F) {
	for _, count := range []uint16{0, 1, 2047, 2048, 3079, 4096, 4099} {
		f.Add(count, uint64(17), true)
		f.Add(count, uint64(23), false)
	}
	f.Fuzz(func(t *testing.T, size uint16, probe uint64, resident bool) {
		count := int(size % 4100)
		col := make([]chunk.U128, count)
		for i := range col {
			col[i] = chunk.U128{Hi: uint64(i+1) * 2, Lo: probe ^ uint64(i)}
		}
		be := backend.Memory()
		require.NoError(t, be.Write(t.Context(), sidxKey("p"), encodeSeriesIndex(col)))
		idx, ok := openPagedIndex(t.Context(), be, "p", count, obs.NewNop().Corruption)
		require.True(t, ok)
		if resident {
			_, err := idx.entries(t.Context())
			require.NoError(t, err)
		}
		id := signal.SeriesID{Hi: probe % uint64(count*2+3)}
		if count > 0 && probe&1 == 0 {
			id = signal.SeriesID(col[probe%uint64(count)])
		}
		expected, found, err := buildPartIndex(col).lookup(t.Context(), id)
		require.NoError(t, err)
		got, actual, err := idx.lookup(t.Context(), id)
		require.NoError(t, err)
		require.Equal(t, found, actual)
		require.Equal(t, expected, got)
	})
}

package engine

import (
	"context"
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/go-faster/errors"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/backend/backendtest"
	"github.com/oteldb/storage/backend/file"
	"github.com/oteldb/storage/encoding/chunk"
	"github.com/oteldb/storage/internal/obs"
	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/signal"
)

type adaptiveIndexReadCounter struct {
	*backendtest.ByteCounter

	indexBytes atomic.Int64
}

func (c *adaptiveIndexReadCounter) ReadAt(ctx context.Context, key string, off, n int64) ([]byte, error) {
	data, err := c.ByteCounter.ReadAt(ctx, key, off, n)
	if strings.HasSuffix(key, "/sidx") {
		c.indexBytes.Add(int64(len(data)))
	}
	return data, err
}

func adaptiveTestIndex(t *testing.T, count int, cacheBytes int64) (*pagedIndex, *backendtest.ByteCounter) {
	t.Helper()
	column := make([]chunk.U128, count*3)
	for i := range count {
		for row := range 3 {
			column[i*3+row] = chunk.U128{Hi: 100, Lo: uint64(i+1) * 2}
		}
	}
	files, err := file.New(t.TempDir())
	require.NoError(t, err)
	require.NoError(t, files.Write(t.Context(), sidxKey("p"), encodeSeriesIndex(column)))
	counter := backendtest.NewByteCounter(files)
	index, ok := openPagedIndex(t.Context(), backend.Cached(counter, cacheBytes), "p", len(column), obs.NewNop().Corruption)
	require.True(t, ok)
	require.NotEmpty(t, index.fences)
	require.Nil(t, index.view.Load())
	counter.Reset()
	return index, counter
}

func TestPagedIndexFineAndCoarse(t *testing.T) {
	t.Parallel()
	const count = 3079
	index, counter := adaptiveTestIndex(t, count, 0)
	for _, pageEntries := range []int{256, 1024} {
		for _, position := range []int{0, 254, 255, 256, 257, 1022, 1023, 1024, 1025, count - 2, count - 1} {
			id := signal.SeriesID{Hi: 100, Lo: uint64(position+1) * 2}
			counter.Reset()
			got, found, err := index.lookupPage(t.Context(), id, pageEntries)
			require.NoError(t, err)
			require.True(t, found)
			require.Equal(t, rowRange{position * 3, (position + 1) * 3}, got)
			start := position / pageEntries * pageEntries
			require.EqualValues(t, min(pageEntries+1, count-start)*20, counter.Bytes())
			id.Lo++
			_, found, err = index.lookupPage(t.Context(), id, pageEntries)
			require.NoError(t, err)
			require.False(t, found)
		}
	}
	counter.Reset()
	_, found, err := index.lookup(t.Context(), signal.SeriesID{Hi: 100, Lo: 2})
	require.NoError(t, err)
	require.True(t, found)
	require.EqualValues(t, 1025*20, counter.Bytes(), "an unhinted lookup retains the original read size")
}

func TestPlannedIndexPageDensity(t *testing.T) {
	t.Parallel()
	const count = 65536
	for _, tt := range []struct {
		name     string
		selected int
		memoize  bool
		pageRows int
		outside  int
	}{
		{"sparse", 4, true, 256, 0},
		{"sparse32", 32, true, 256, 0},
		{"below threshold", 63, true, 256, 0},
		{"at threshold", 64, true, 1024, 0},
		{"dense", 512, true, 1024, 0},
		{"not memoized", 4, false, 1024, 0},
		{"outside part bounds", 4, true, 256, 64},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			index, counter := adaptiveTestIndex(t, count, 2<<20)
			ids := make([]signal.SeriesID, 0, tt.selected+tt.outside)
			pages := map[int]bool{}
			for i := range tt.selected {
				position := i * count / tt.selected
				ids = append(ids, signal.SeriesID{Hi: 100, Lo: uint64(position+1) * 2})
				pages[position/tt.pageRows] = true
			}
			for i := range tt.outside {
				ids = append(ids, signal.SeriesID{Hi: 101, Lo: uint64(i)})
			}
			pt := &part{index: partIndex{paged: index}}
			plan := enginePlan{ids: ids}
			ranges, positions, err := plan.matchingRanges(t.Context(), pt, tt.memoize)
			require.NoError(t, err)
			require.Len(t, ranges, tt.selected)
			for i, got := range ranges {
				position := i * count / tt.selected
				require.Equal(t, rowRange{position * 3, (position + 1) * 3}, got)
			}
			if tt.memoize {
				require.Len(t, positions, tt.selected)
				for i, position := range positions {
					require.EqualValues(t, i, position)
				}
			} else {
				require.Nil(t, positions)
			}
			var bytes int
			for page := range pages {
				bytes += min(tt.pageRows+1, count-page*tt.pageRows) * 20
			}
			require.EqualValues(t, bytes, counter.Bytes(), counter.Report())
			require.EqualValues(t, len(pages), counter.Reads())
		})
	}
}

func TestConcurrentFineAndCoarseIndexLookups(t *testing.T) {
	t.Parallel()
	const count = 3079
	index, _ := adaptiveTestIndex(t, count, 2<<20)
	var wg sync.WaitGroup
	var failures [8]error
	for worker := range failures {
		wg.Go(func() {
			pageRows := 1024
			if worker%2 == 0 {
				pageRows = 256
			}
			for _, position := range []int{0, 255, 256, 1023, 1024, count - 1} {
				id := signal.SeriesID{Hi: 100, Lo: uint64(position+1) * 2}
				got, found, err := index.lookupPage(t.Context(), id, pageRows)
				if err != nil || !found || got != (rowRange{position * 3, (position + 1) * 3}) {
					failures[worker] = errors.Errorf("lookup=%v,%v,%v for %v", got, found, err, id)
					return
				}
			}
		})
	}
	wg.Wait()
	for _, err := range failures {
		require.NoError(t, err)
	}
}

func TestAdaptiveIndexFetch(t *testing.T) {
	t.Parallel()
	files, err := file.New(t.TempDir())
	require.NoError(t, err)
	counter := &adaptiveIndexReadCounter{ByteCounter: backendtest.NewByteCounter(files)}
	e := New(Config{Backend: counter, Prefix: "adaptive", DecodeCacheBytes: 1 << 20, DecodeMemoryBytes: 8 << 20})
	t.Cleanup(func() { require.NoError(t, e.Close(context.WithoutCancel(t.Context()))) })
	for i := range 4096 {
		for ts := range 3 {
			_, err := e.Append(orderBenchSeries(i), int64(ts+1), float64(i*100+ts))
			require.NoError(t, err)
		}
	}
	require.NoError(t, e.Flush(t.Context()))
	require.Len(t, e.parts, 1)
	counter.Reset()
	counter.indexBytes.Store(0)
	it, err := e.Fetch(t.Context(), fetch.Request{Start: 1, End: 3, Matchers: []fetch.Matcher{{
		Name: []byte("order"), Match: func(v signal.Value) bool { return v.Int() == 7 },
	}}})
	require.NoError(t, err)
	defer func() { require.NoError(t, it.Close()) }()
	plan := it.(*fetchIter).plan
	require.Len(t, plan.partPositions[e.parts[0]], 1)
	indexBytes := counter.indexBytes.Load()
	require.Positive(t, indexBytes)
	require.LessOrEqual(t, indexBytes, int64(257*20))
	batch, err := it.Next(t.Context())
	require.NoError(t, err)
	require.Equal(t, orderBenchSeries(7).Hash(), batch.ID)
	require.Equal(t, []int64{1, 2, 3}, batch.Timestamps)
	require.Equal(t, []float64{700, 701, 702}, batch.Values)
	_, err = it.Next(t.Context())
	require.ErrorIs(t, err, io.EOF)
	require.Equal(t, indexBytes, counter.indexBytes.Load(), "collection must reuse the fine-page plan")
}

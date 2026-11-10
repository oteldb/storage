package engine

import (
	"math"
	"strconv"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/backend/backendtest"
	"github.com/oteldb/storage/internal/memsize"
	"github.com/oteldb/storage/signal"
)

func TestDiskSeriesCache(t *testing.T) {
	t.Parallel()
	base := memsize.Of[diskSeriesCache]() + memsize.Of[diskCacheShard]()
	bucket := memsize.Of[diskCacheBucket]()
	for _, budget := range []int64{-1, 0, 1, base + bucket - 1, base + bucket, 1024, 1 << 20} {
		t.Run(strconv.FormatInt(budget, 10), func(t *testing.T) {
			t.Parallel()
			c := newDiskSeriesCache(budget)
			for i := range 20000 {
				id := signal.SeriesID{Lo: uint64(i), Hi: uint64(i % 7)}
				_, hit := c.get(id)
				require.False(t, hit)
				c.put(id, int64(i))
				entry, ok := c.get(id)
				if c != nil {
					require.True(t, ok)
					require.Equal(t, diskHeadEntry{newest: int64(i), exists: true}, entry)
					require.LessOrEqual(t, c.sizeBytes(), budget)
				}
			}
			c.clear()
			_, hit := c.get(signal.SeriesID{Lo: 19999, Hi: 19999 % 7})
			require.False(t, hit)
		})
	}
}

func TestDiskSeriesCacheEvictionAndFullIDs(t *testing.T) {
	t.Parallel()
	budget := memsize.Of[diskSeriesCache]() + memsize.Of[diskCacheShard]() + memsize.Of[diskCacheBucket]()
	c := newDiskSeriesCache(budget)
	for i := range diskCacheWays {
		c.put(signal.SeriesID{Hi: uint64(i)}, math.MinInt64+int64(i))
	}
	for i := range diskCacheWays {
		entry, ok := c.get(signal.SeriesID{Hi: uint64(i)})
		require.True(t, ok)
		require.Equal(t, math.MinInt64+int64(i), entry.newest)
	}
	zero := signal.SeriesID{}
	c.put(zero, math.MaxInt64)
	c.put(zero, 1)
	entry, ok := c.get(zero)
	require.True(t, ok)
	require.EqualValues(t, math.MaxInt64, entry.newest)
	c.put(signal.SeriesID{Hi: diskCacheWays}, 100)
	_, ok = c.get(zero)
	require.False(t, ok, "full buckets evict rather than growing past their byte budget")
	require.Equal(t, budget, c.sizeBytes())
}

func FuzzDiskSeriesCache(f *testing.F) {
	f.Add([]byte{0, 0, 0, 0, 0, 255, 1, 0, 128, 2, 7, 64})
	f.Add([]byte{1, 1, 1, 2, 2, 2, 3, 3, 3, 4, 4, 4})
	f.Fuzz(func(t *testing.T, data []byte) {
		c := newDiskSeriesCache(4096)
		for i := 0; i+2 < len(data); i += 3 {
			id := signal.SeriesID{Lo: uint64(data[i]), Hi: uint64(data[i+1])}
			ts := int64(int8(data[i+2]))
			previous, hit := c.get(id)
			c.put(id, ts)
			if hit {
				ts = max(ts, previous.newest)
			}
			entry, ok := c.get(id)
			require.True(t, ok)
			require.Equal(t, diskHeadEntry{newest: ts, exists: true}, entry)
			require.LessOrEqual(t, c.sizeBytes(), int64(4096))
			if data[i+2] == 0 {
				c.clear()
				_, ok = c.get(id)
				require.False(t, ok)
			}
		}
	})
}

func TestDiskSeriesCacheWorkingSet(t *testing.T) {
	t.Parallel()
	be := backendtest.NewByteCounter(backend.Memory())
	e := New(Config{Backend: be, Prefix: "cache-working-set", DiskSeries: true, SeriesCacheBytes: 8 << 20})
	const n = 16384
	ids, ts, values := make([]signal.SeriesID, n), make([]int64, n), make([]float64, n)
	for i := range ids {
		ids[i], ts[i] = benchSeries(i).Hash(), 100
	}
	result, err := e.AppendBatch(ids, ts, values, nil, benchSeries, AppendLimits{})
	require.NoError(t, err)
	require.Equal(t, n, result.Accepted)
	require.NoError(t, e.Flush(t.Context()))
	require.Zero(t, e.head.series.Len())
	cache := e.head.disk.cache
	var warm []signal.SeriesID
	var refs []int
	for i, id := range ids {
		if _, hit := cache.get(id); hit {
			warm = append(warm, id)
			refs = append(refs, i)
		}
	}
	require.Greater(t, len(warm), n*95/100)
	be.Reset()
	result, err = e.AppendBatch(warm, ts[:len(warm)], values[:len(warm)], nil,
		func(i int) signal.Series { return benchSeries(refs[i]) }, AppendLimits{MaxSeries: n})
	require.NoError(t, err)
	require.Equal(t, len(warm), result.Accepted)
	require.Zero(t, be.Reads(), be.Report())
	require.EqualValues(t, n, e.Stats().Series)
	require.NoError(t, e.Flush(t.Context()))
	require.Same(t, cache, e.head.disk.cache)
}

func TestDiskSeriesCacheDoesNotTriggerFlush(t *testing.T) {
	t.Parallel()
	e := New(Config{Backend: backend.Memory(), Prefix: "cache-pressure", DiskSeries: true,
		ResidentHeadBytes: 128 << 10, SeriesCacheBytes: 2 << 20})
	for i := range 10000 {
		e.head.disk.remember(signal.SeriesID{Lo: uint64(i)}, 100)
	}
	require.Greater(t, e.IdentityBytes(), e.cfg.ResidentHeadBytes)
	for i := range 2 {
		ok, err := e.Append(benchSeries(i), 100, 1)
		require.NoError(t, err)
		require.True(t, ok)
	}
	require.Zero(t, e.PartCount())
	require.NoError(t, e.replayResidentGuard(1))
	require.False(t, e.residentHeadOverBudgetLocked())
	require.NoError(t, e.Reset(t.Context()))
	_, hit := e.head.disk.cache.get(signal.SeriesID{})
	require.False(t, hit)
	require.Equal(t, e.cfg.SeriesCacheBytes, int64(2<<20))
}

func TestDiskSeriesCacheCatalogHydratesActiveWatermarks(t *testing.T) {
	t.Parallel()
	be := backend.Memory()
	cfg := Config{Backend: be, Prefix: "cache-adopt", DiskSeries: true, OOOWindow: 10}
	local, rival := New(cfg), New(cfg)
	s := benchSeries(1)
	ok, err := local.Append(s, 100, 1)
	require.NoError(t, err)
	require.True(t, ok)
	ok, err = rival.Append(s, 200, 2)
	require.NoError(t, err)
	require.True(t, ok)
	require.NoError(t, rival.Flush(t.Context()))
	require.True(t, local.head.disk.complete)
	local.mu.Lock()
	err = local.adoptIndexLocked(t.Context())
	local.mu.Unlock()
	require.NoError(t, err)
	newest, seen := local.head.watermark(s.Hash())
	require.True(t, seen)
	require.EqualValues(t, 200, newest)
	ok, err = local.Append(s, 150, 3)
	require.NoError(t, err)
	require.False(t, ok)
	require.NoError(t, local.Flush(t.Context()))
	entry, hit := local.head.disk.cache.get(s.Hash())
	require.True(t, hit)
	require.EqualValues(t, 200, entry.newest)
}

func TestDiskSeriesCacheConcurrentAccounting(t *testing.T) {
	t.Parallel()
	e := New(Config{Backend: backend.Memory(), Prefix: "cache-accounting", DiskSeries: true})
	var wg sync.WaitGroup
	wg.Go(func() {
		for range 10000 {
			_ = e.IdentityBytes()
			_ = e.Stats()
		}
	})
	e.mu.LockWriter()
	err := e.mu.WithReaders(func() error {
		for i := range 10000 {
			e.head.disk.remember(signal.SeriesID{Lo: uint64(i)}, 100)
		}
		return nil
	})
	e.mu.UnlockWriter()
	require.NoError(t, err)
	wg.Wait()
}

func BenchmarkDiskSeriesCacheHit(b *testing.B) {
	c := newDiskSeriesCache(8 << 20)
	const count = 65536
	ids := make([]signal.SeriesID, count)
	for i := range ids {
		ids[i] = signal.SeriesID{Lo: uint64(i)}
		c.put(ids[i], int64(i))
	}
	b.ReportAllocs()
	var hits int
	for b.Loop() {
		for _, id := range ids {
			if _, hit := c.get(id); hit {
				hits++
			}
		}
	}
	b.ReportMetric(float64(hits)/float64(b.N*count), "hit-ratio")
}

func TestDiskSeriesCachePruneForgetsExpiredSeries(t *testing.T) {
	t.Parallel()
	e := New(Config{Backend: backend.Memory(), Prefix: "cache-retention", DiskSeries: true, OOOWindow: 10})
	s := benchSeries(1)
	ok, err := e.Append(s, 100, 1)
	require.NoError(t, err)
	require.True(t, ok)
	require.NoError(t, e.Flush(t.Context()))
	_, hit := e.head.disk.cache.get(s.Hash())
	require.True(t, hit)
	require.NoError(t, e.Merge(t.Context(), 101))
	_, err = e.PruneIdentitiesWith(t.Context(), PruneOptions{Force: true})
	require.NoError(t, err)
	require.Zero(t, e.Stats().Series)
	_, hit = e.head.disk.cache.get(s.Hash())
	require.False(t, hit)
	result, err := e.AppendBatch([]signal.SeriesID{s.Hash()}, []int64{90}, []float64{1}, nil,
		func(int) signal.Series { return s }, AppendLimits{MaxSeries: 1})
	require.NoError(t, err)
	require.Equal(t, 1, result.Accepted)
	require.EqualValues(t, 1, e.Stats().Series)
}

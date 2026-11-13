package engine

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/signal"
)

func TestDiskSeriesCacheSharesBucketCapacity(t *testing.T) {
	t.Parallel()
	c := &diskSeriesCache{shards: []diskCacheShard{{buckets: make([]diskCacheBucket, 4)}}, maxBuckets: 4}
	var ids []signal.SeriesID
	for i := uint64(0); len(ids) < 3*diskCacheWays; i++ {
		id := signal.SeriesID{Hi: i / 7, Lo: i}
		if diskCacheHash(id)%4 == 0 {
			ids = append(ids, id)
			c.put(id, -100)
		}
	}
	for _, id := range ids {
		entry, hit := c.get(id)
		require.True(t, hit, "alternate buckets have room for colliding primary buckets")
		require.EqualValues(t, -100, entry.newest)
		c.put(id, 200)
		c.put(id, -200)
		entry, hit = c.get(id)
		require.True(t, hit)
		require.EqualValues(t, 200, entry.newest)
	}
	require.Equal(t, len(ids), c.shards[0].count, "updates must not insert a second copy")
}

func TestDiskSeriesCacheHighOccupancy(t *testing.T) {
	t.Parallel()
	const count = 16000
	c := newDiskSeriesCache(512 << 10)
	ids := make([]signal.SeriesID, count)
	for i := range ids {
		ids[i] = signal.SeriesID{Hi: mixDiskBloom(uint64(i)), Lo: mixDiskBloom(uint64(i) + 1)}
		c.put(ids[i], 100)
	}
	for pass := range 3 {
		hits := 0
		for _, id := range ids {
			if _, hit := c.get(id); hit {
				hits++
			}
			c.put(id, 101+int64(pass))
		}
		require.GreaterOrEqual(t, hits, count*99/100)
		require.LessOrEqual(t, c.sizeBytes(), int64(512<<10))
	}
}

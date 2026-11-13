package engine

import (
	"sync/atomic"

	"github.com/oteldb/storage/internal/memsize"
	"github.com/oteldb/storage/signal"
)

const defaultSeriesCacheBytes = 512 << 20
const diskCacheWays = 16
const diskCacheShards = 256

type diskCacheEntry struct {
	id     signal.SeriesID
	newest int64
}

type diskCacheBucket struct {
	entries [diskCacheWays]diskCacheEntry
	used    uint8
	next    uint8
}

type diskCacheShard struct {
	buckets []diskCacheBucket
	count   int
}

// diskSeriesCache is advisory state protected by engine writer ownership.
// Numeric buckets retain no descriptors or registry references and need no GC scanning.
type diskSeriesCache struct {
	shards     []diskCacheShard
	maxBuckets int
	bytes      atomic.Int64
}

func newDiskSeriesCache(budget int64) *diskSeriesCache {
	base := memsize.Of[diskSeriesCache]()
	shardBytes, bucketBytes := memsize.Of[diskCacheShard](), memsize.Of[diskCacheBucket]()
	if budget < base+shardBytes+bucketBytes {
		return nil
	}
	n := 1
	for n < diskCacheShards && int64(n*2)*(shardBytes+bucketBytes) <= budget-base {
		n *= 2
	}
	c := &diskSeriesCache{
		shards:     make([]diskCacheShard, n),
		maxBuckets: int((budget - base - int64(n)*shardBytes) / (int64(n) * bucketBytes)),
	}
	c.bytes.Store(base + memsize.Slice(c.shards))
	return c
}

func diskCacheHash(id signal.SeriesID) uint64 {
	h, _ := diskBloomHashes(id)
	return h
}

func diskCacheSecond(hash uint64, first, n int) int {
	second := int(mixDiskBloom(hash) % uint64(n))
	if second == first {
		second = (second + 1) % n
	}
	return second
}

func (b *diskCacheBucket) lookup(id signal.SeriesID) *diskCacheEntry {
	for i := range int(b.used) {
		if b.entries[i].id == id {
			return &b.entries[i]
		}
	}
	return nil
}

func (c *diskSeriesCache) lookup(s *diskCacheShard, hash uint64, id signal.SeriesID) *diskCacheEntry {
	first := int((hash / uint64(len(c.shards))) % uint64(len(s.buckets)))
	if entry := s.buckets[first].lookup(id); entry != nil {
		return entry
	}
	if len(s.buckets) == 1 {
		return nil
	}
	return s.buckets[diskCacheSecond(hash, first, len(s.buckets))].lookup(id)
}

func (c *diskSeriesCache) get(id signal.SeriesID) (diskHeadEntry, bool) {
	if c == nil {
		return diskHeadEntry{}, false
	}
	h := diskCacheHash(id)
	s := &c.shards[h&uint64(len(c.shards)-1)]
	if len(s.buckets) == 0 {
		return diskHeadEntry{}, false
	}
	if entry := c.lookup(s, h, id); entry != nil {
		return diskHeadEntry{newest: entry.newest, exists: true}, true
	}
	return diskHeadEntry{}, false
}

func (c *diskSeriesCache) put(id signal.SeriesID, newest int64) {
	if c == nil {
		return
	}
	h := diskCacheHash(id)
	s := &c.shards[h&uint64(len(c.shards)-1)]
	if len(s.buckets) > 0 {
		if entry := c.lookup(s, h, id); entry != nil {
			entry.newest = max(entry.newest, newest)
			return
		}
	}
	if len(s.buckets) == 0 || (s.count >= len(s.buckets)*diskCacheWays*3/4 && len(s.buckets) < c.maxBuckets) {
		c.grow(s)
	}
	c.insert(s, id, newest)
}

func (c *diskSeriesCache) insert(s *diskCacheShard, id signal.SeriesID, newest int64) {
	hash := diskCacheHash(id)
	first := int((hash / uint64(len(c.shards))) % uint64(len(s.buckets)))
	b, second := &s.buckets[first], &s.buckets[diskCacheSecond(hash, first, len(s.buckets))]
	if second.used < b.used {
		b = second
	}
	pos := b.next
	if b.used < diskCacheWays {
		pos = b.used
		b.used++
		s.count++
	} else {
		b.next = (b.next + 1) % diskCacheWays
	}
	b.entries[pos] = diskCacheEntry{id: id, newest: newest}
}

func (c *diskSeriesCache) grow(s *diskCacheShard) {
	old := s.buckets
	s.buckets = make([]diskCacheBucket, min(c.maxBuckets, max(1, len(old)*2)))
	c.bytes.Add(memsize.Slice(s.buckets) - memsize.Slice(old))
	s.count = 0
	for i := range old {
		for j := range int(old[i].used) {
			entry := old[i].entries[j]
			c.insert(s, entry.id, entry.newest)
		}
	}
}

func (c *diskSeriesCache) clear() {
	if c == nil {
		return
	}
	clear(c.shards)
	c.bytes.Store(memsize.Of[diskSeriesCache]() + memsize.Slice(c.shards))
}

func (c *diskSeriesCache) sizeBytes() int64 {
	if c == nil {
		return 0
	}
	return c.bytes.Load()
}

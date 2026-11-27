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
	entries   [diskCacheWays]diskCacheEntry
	lineage   uint64
	published uint16 // One confirmed membership bit per way for lineage.
	used      uint8
	next      uint8
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

func (b *diskCacheBucket) position(id signal.SeriesID) int {
	for i := range int(b.used) {
		if b.entries[i].id == id {
			return i
		}
	}
	return -1
}

func (c *diskSeriesCache) lookup(s *diskCacheShard, hash uint64, id signal.SeriesID) (*diskCacheBucket, int) {
	first := int((hash / uint64(len(c.shards))) % uint64(len(s.buckets)))
	b := &s.buckets[first]
	if pos := b.position(id); pos >= 0 {
		return b, pos
	}
	if len(s.buckets) == 1 {
		return nil, -1
	}
	b = &s.buckets[diskCacheSecond(hash, first, len(s.buckets))]
	pos := b.position(id)
	return b, pos
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
	if b, pos := c.lookup(s, h, id); pos >= 0 {
		var proof uint64
		if b.published>>pos&1 != 0 {
			proof = b.lineage
		}
		return diskHeadEntry{newest: b.entries[pos].newest, descriptorLineage: proof, exists: true}, true
	}
	return diskHeadEntry{}, false
}

func (c *diskSeriesCache) put(id signal.SeriesID, newest int64) {
	c.remember(id, newest, 0)
}

func (c *diskSeriesCache) remember(id signal.SeriesID, newest int64, lineage uint64) {
	if c == nil {
		return
	}
	h := diskCacheHash(id)
	s := &c.shards[h&uint64(len(c.shards)-1)]
	if len(s.buckets) > 0 {
		if b, pos := c.lookup(s, h, id); pos >= 0 {
			entry := &b.entries[pos]
			entry.newest = max(entry.newest, newest)
			b.certify(pos, lineage)
			return
		}
	}
	if len(s.buckets) == 0 || (s.count >= len(s.buckets)*diskCacheWays*3/4 && len(s.buckets) < c.maxBuckets) {
		c.grow(s)
	}
	c.insert(s, diskCacheEntry{id: id, newest: newest}, lineage)
}

func (c *diskSeriesCache) insert(s *diskCacheShard, entry diskCacheEntry, lineage uint64) {
	hash := diskCacheHash(entry.id)
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
	b.entries[pos] = entry
	b.published &^= 1 << pos
	b.certify(int(pos), lineage)
}

func (b *diskCacheBucket) certify(pos int, lineage uint64) {
	if lineage == 0 || lineage < b.lineage {
		return
	}
	if lineage > b.lineage {
		b.lineage, b.published = lineage, 0
	}
	b.published |= 1 << pos
}

func (c *diskSeriesCache) grow(s *diskCacheShard) {
	old := s.buckets
	s.buckets = make([]diskCacheBucket, min(c.maxBuckets, max(1, len(old)*2)))
	c.bytes.Add(memsize.Slice(s.buckets) - memsize.Slice(old))
	s.count = 0
	for i := range old {
		for j := range int(old[i].used) {
			entry := old[i].entries[j]
			var proof uint64
			if old[i].published>>j&1 != 0 {
				proof = old[i].lineage
			}
			c.insert(s, entry, proof)
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

package engine

import (
	"container/list"
	"hash/maphash"
	"slices"
	"sync"
	"sync/atomic"

	"github.com/oteldb/storage/block"
)

// blockCache is a byte-bounded sharded LRU over decoded column blocks, keyed by (part prefix, column, block
// index). It replaces the whole-part decode cache: a fetch decodes only the blocks its matched series
// touch (series-skip) and caches those, so the resident set is the *useful* blocks across live parts
// rather than every column of every touched part — and a later query sharing series/time reuses the
// already-decoded blocks. Columns cache independently, so a timestamp-only count and a value-reading
// fetch over the same part share the ts blocks without the value column ever being decoded.
//
// A cached block is an immutable decoded slice; a fetch either copies the blocks it needs into a
// (pooled, per-fetch) decodedPart, or (on the block-slice path) holds them as views into its merge
// until collect copies the samples out. Either way the cache entries are never mutated, so they stay
// valid across concurrent fetches. Entries are dropped when their part is reclaimed (evictPrefix) or
// the byte budget evicts the coldest.
//
// Recycling: an evicted block's decoded slice is pointer-free garbage that the next miss re-mints —
// the dominant query-path allocation under a pinned cache. To cut that alloc rate (not the resident
// size) the cache pools evicted slices in a bounded, GC-stable freelist that a later decode draws its
// destination from. Because a fetch may still be reading a block when the byte budget evicts it, each
// entry is reference-counted: get/insert pin it, the reader releases when done, and a buffer returns
// to the pool only once its entry is both evicted and unpinned. Recycling on eviction is thus
// budget-neutral — it never enlarges the resident set (the freelist is small and bounded), it only
// stops the churn.
type blockCache struct {
	maxBytes     int64
	hits, misses atomic.Int64
	inflight     atomic.Int64
	bytes        atomic.Int64 // Linked entries and in-flight insertion reservations.
	marksBytes   atomic.Int64
	seed         maphash.Seed
	shards       []blockCacheShard
	trimMu       sync.Mutex
	trimming     atomic.Bool
	trimNext     int
	i64Free      bufFreeList[int64]
	f64Free      bufFreeList[float64]
}

type blockCacheShard struct {
	mu       sync.Mutex
	ll       list.List
	marks    list.List
	items    map[blockKey]*list.Element
	byPrefix map[string]map[blockKey]struct{}
}

func (s *blockCacheShard) lru(key blockKey) *list.List {
	if key.col == colMarksID {
		return &s.marks
	}
	return &s.ll
}

// colID identifies which metric column a cached block belongs to (part of the cache key).
type colID uint8

const (
	colTsID colID = iota
	colValID
	colSFID
	colMarksID
)

// blockKey identifies a decoded block; index -1 names the column's immutable decoder metadata.
type blockKey struct {
	prefix string
	col    colID
	blk    int
}

// blockEntry holds one decoded block or immutable decoder prototype. refs
// and evicted are guarded by the owning shard's mu: refs counts the fetches currently reading the
// block (each get/insert pins, each release unpins), and evicted marks that the entry has left the
// LRU. The buffer returns to the freelist only when refs hits 0 after evicted is set, so a reader
// holding a view of an evicted block never sees its buffer recycled underneath it.
type blockEntry struct {
	owner    *blockCacheShard
	key      blockKey
	i64      []int64
	f64      []float64
	decoder  *block.Decoder
	granules []block.Granule
	bytes    int64
	refs     int
	evicted  bool
}

func newBlockCache(maxBytes int64) *blockCache {
	shards := min(8, max(1, int(maxBytes/(1<<20))))
	return newBlockCacheWithShards(maxBytes, shards)
}

func newBlockCacheWithShards(maxBytes int64, count int) *blockCache {
	c := &blockCache{maxBytes: maxBytes, seed: maphash.MakeSeed(), shards: make([]blockCacheShard, max(count, 1))}
	for i := range c.shards {
		shard := &c.shards[i]
		shard.items = make(map[blockKey]*list.Element)
		shard.byPrefix = make(map[string]map[blockKey]struct{})
	}
	capNow := func() int { return blockBufCap * (1 + int(c.inflight.Load())) }
	c.i64Free.capNow, c.f64Free.capNow = capNow, capNow
	return c
}

func (c *blockCache) shard(key blockKey) *blockCacheShard {
	return &c.shards[maphash.Comparable(c.seed, key)%uint64(len(c.shards))]
}

// Fetch ownership scales the one shared pair of decode-buffer freelists.
func (c *blockCache) fetchStart() { c.inflight.Add(1) }
func (c *blockCache) fetchEnd()   { c.inflight.Add(-1) }

func (c *blockCache) get(key blockKey) (*blockEntry, bool) {
	shard := c.shard(key)
	shard.mu.Lock()
	defer shard.mu.Unlock()
	el, ok := shard.items[key]
	if !ok {
		if key.col != colMarksID {
			c.misses.Add(1)
		}
		return nil, false
	}
	shard.lru(key).MoveToFront(el)
	if key.col != colMarksID {
		c.hits.Add(1)
	}
	e := el.Value.(*blockEntry)
	e.refs++
	return e, true
}

func (c *blockCache) insert(e *blockEntry) *blockEntry {
	shard := c.shard(e.key)
	e.owner = shard
	shard.mu.Lock()
	if el, ok := shard.items[e.key]; ok {
		result := c.redundantInsert(shard, el, e)
		shard.mu.Unlock()
		return result
	}
	shard.mu.Unlock()
	e.refs = 1
	if e.bytes > c.maxBytes || !c.reserve(e.bytes) {
		e.evicted = true
		return e
	}
	shard.mu.Lock()
	defer shard.mu.Unlock()
	if el, ok := shard.items[e.key]; ok {
		c.bytes.Add(-e.bytes)
		return c.redundantInsert(shard, el, e)
	}
	el := shard.lru(e.key).PushFront(e)
	shard.items[e.key] = el
	if e.key.col == colMarksID {
		c.marksBytes.Add(e.bytes)
	}
	set := shard.byPrefix[e.key.prefix]
	if set == nil {
		set = make(map[blockKey]struct{})
		shard.byPrefix[e.key.prefix] = set
	}
	set[e.key] = struct{}{}
	return e
}

// The caller holds the owning shard; the redundant entry remains private.
func (c *blockCache) redundantInsert(shard *blockCacheShard, el *list.Element, e *blockEntry) *blockEntry {
	shard.lru(e.key).MoveToFront(el)
	current := el.Value.(*blockEntry)
	current.refs++
	c.recycle(e)
	return current
}

func (c *blockCache) tryReserve(n int64) bool {
	for {
		used := c.bytes.Load()
		if used > c.maxBytes-n {
			return false
		}
		if c.bytes.CompareAndSwap(used, used+n) {
			return true
		}
	}
}

// Credits include in-progress insertions. Never wait for an unlinked reservation while trimming.
func (c *blockCache) reserve(n int64) bool {
	if c.tryReserveAvailable(n) {
		return true
	}
	c.trimMu.Lock()
	defer c.trimMu.Unlock()
	c.trimming.Store(true)
	defer c.trimming.Store(false)
	for {
		if c.tryReserve(n) {
			return true
		}
		if !c.evictOldestShardEntry(n) {
			return false
		}
	}
}

// Freed credits belong to an active trimmer before newer cache misses can use them.
func (c *blockCache) tryReserveAvailable(n int64) bool {
	return !c.trimming.Load() && c.tryReserve(n)
}

func (c *blockCache) evictOldestShardEntry(required int64) bool {
	for range c.shards {
		shard := &c.shards[c.trimNext]
		c.trimNext = (c.trimNext + 1) % len(c.shards)
		shard.mu.Lock()
		back := shard.ll.Back()
		if c.marksBytes.Load() > c.maxBytes/marksCacheFraction || required > c.maxBytes-c.maxBytes/marksCacheFraction {
			if mark := shard.marks.Back(); mark != nil {
				back = mark
			}
		}
		if back != nil {
			c.removeElem(shard, back)
			shard.mu.Unlock()
			return true
		}
		shard.mu.Unlock()
	}
	return false
}

func (c *blockCache) release(e *blockEntry) {
	if e == nil {
		return
	}
	shard := e.owner
	shard.mu.Lock()
	e.refs--
	if e.refs == 0 && e.evicted {
		c.recycle(e)
	}
	shard.mu.Unlock()
}

// The entry is private or evicted/unpinned under its shard; only the shared pool locks are needed.
func (c *blockCache) recycle(e *blockEntry) {
	e.decoder = nil
	e.granules = nil
	if e.i64 != nil {
		c.i64Free.put(e.i64)
		e.i64 = nil
	}
	if e.f64 != nil {
		c.f64Free.put(e.f64)
		e.f64 = nil
	}
}

// getI64Buf draws an int64 decode buffer with room for n rows from the freelist (or allocates one).
func (c *blockCache) getI64Buf(n int) []int64 { return c.i64Free.get(n) }

// getF64Buf is [blockCache.getI64Buf] for float64 columns.
func (c *blockCache) getF64Buf(n int) []float64 { return c.f64Free.get(n) }

// evictPrefix drops every block of the part at prefix (called when the part is reclaimed). The part is
// reclaimed only after every fetch that acquired it has released it, so no live fetch is reading these
// blocks; their buffers recycle immediately (refs == 0).
func (c *blockCache) evictPrefix(prefix string) {
	for i := range c.shards {
		shard := &c.shards[i]
		shard.mu.Lock()
		for key := range shard.byPrefix[prefix] {
			if el, ok := shard.items[key]; ok {
				c.removeElemKeepPrefix(shard, el)
			}
		}
		delete(shard.byPrefix, prefix)
		shard.mu.Unlock()
	}
}

func (c *blockCache) removeElem(shard *blockCacheShard, el *list.Element) {
	e := el.Value.(*blockEntry)
	if set := shard.byPrefix[e.key.prefix]; set != nil {
		delete(set, e.key)
		if len(set) == 0 {
			delete(shard.byPrefix, e.key.prefix)
		}
	}
	c.removeElemKeepPrefix(shard, el)
}

func (c *blockCache) removeElemKeepPrefix(shard *blockCacheShard, el *list.Element) {
	e := el.Value.(*blockEntry)
	c.bytes.Add(-e.bytes)
	if e.key.col == colMarksID {
		c.marksBytes.Add(-e.bytes)
	}
	delete(shard.items, e.key)
	shard.lru(e.key).Remove(el)
	e.evicted = true
	if e.refs == 0 {
		c.recycle(e)
	}
}

// bufFreeList is a bounded, GC-stable recycler of decode-output column buffers. It does not zero on
// get (the decoder overwrites every row it returns) and bounds by buffer count, so the pooled-but-free
// buffers add at most blockBufCap×blockBytes to the resident set. It is a rooted freelist rather than
// a sync.Pool for the same reason as the engine's other pools: a sync.Pool is emptied at every GC, and
// the query load these buffers serve drives allocation-triggered GCs constantly, so a sync.Pool would
// drop the buffers before they were ever reused.
type bufFreeList[T any] struct {
	mu   sync.Mutex
	free [][]T
	// capNow returns the current entry bound; nil ⇒ the static blockBufCap. The block cache wires
	// a bound scaled by in-flight fetches, so a concurrency burst's returned buffers are kept for
	// the next fetch instead of dropped.
	capNow func() int
}

// blockBufCap is the freelist's baseline bound (its resident floor, a few MiB of buffers): the
// per-fetch decode parallelism times a small burst factor. Under concurrent fetches the effective
// bound scales up with the in-flight count (see blockCache.inflight) and falls back here at rest.
const blockBufCap = prefetchConcurrency * 4

// get returns a recycled buffer with room for n rows, or a fresh one when none fits. It scans for
// a fitting buffer instead of popping blindly: a too-small buffer (a smaller-granule part's) stays
// pooled for a later smaller request rather than being discarded.
func (p *bufFreeList[T]) get(n int) []T {
	p.mu.Lock()

	for i, s := range slices.Backward(p.free) {
		if cap(s) < n {
			continue
		}

		last := len(p.free) - 1
		p.free[i] = p.free[last]
		p.free[last] = nil
		p.free = p.free[:last]
		p.mu.Unlock()

		return s[:n]
	}

	p.mu.Unlock()

	return make([]T, n)
}

func (p *bufFreeList[T]) put(s []T) {
	if cap(s) == 0 {
		return
	}

	limit := blockBufCap
	if p.capNow != nil {
		limit = p.capNow()
	}

	p.mu.Lock()
	if len(p.free) < limit {
		p.free = append(p.free, s)
	}
	p.mu.Unlock()
}

// DecodeCacheStats is a snapshot of the decoded-block cache's effectiveness.
type DecodeCacheStats struct {
	Hits, Misses int64
	Bytes        int64
	Items        int // cached blocks
}

func (c *blockCache) stats() DecodeCacheStats {
	items := 0
	for i := range c.shards {
		shard := &c.shards[i]
		shard.mu.Lock()
		items += len(shard.items)
		shard.mu.Unlock()
	}
	return DecodeCacheStats{Hits: c.hits.Load(), Misses: c.misses.Load(), Bytes: c.bytes.Load(), Items: items}
}

// DecodeCacheStats returns the engine's decoded-block cache statistics and whether a cache is
// configured ([Config.DecodeCacheBytes] > 0). For operator visibility into cache effectiveness.
func (e *Engine) DecodeCacheStats() (DecodeCacheStats, bool) {
	if e.blockCache == nil {
		return DecodeCacheStats{}, false
	}

	return e.blockCache.stats(), true
}

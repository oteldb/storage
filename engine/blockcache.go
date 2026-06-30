package engine

import (
	"container/list"
	"sync"
	"sync/atomic"
)

// blockCache is a byte-bounded LRU over decoded column blocks, keyed by (part prefix, column, block
// index). It replaces the whole-part decode cache: a fetch decodes only the blocks its matched series
// touch (series-skip) and caches those, so the resident set is the *useful* blocks across live parts
// rather than every column of every touched part — and a later query sharing series/time reuses the
// already-decoded blocks. Columns cache independently, so a timestamp-only count and a value-reading
// fetch over the same part share the ts blocks without the value column ever being decoded.
//
// A cached block is an immutable decoded slice; assembling a fetch's columns copies the blocks it
// needs into a (pooled, per-fetch) decodedPart, so the cache entries are never mutated and stay valid
// across concurrent fetches. Entries are dropped when their part is reclaimed (evictPrefix) or the
// byte budget evicts the coldest.
type blockCache struct {
	maxBytes int64

	hits, misses atomic.Int64

	mu       sync.Mutex
	ll       *list.List                       // front = most-recently used; Value is *blockEntry
	items    map[blockKey]*list.Element       // key → element
	byPrefix map[string]map[blockKey]struct{} // prefix → its keys, for evictPrefix
	bytes    int64
}

// colID identifies which metric column a cached block belongs to (part of the cache key).
type colID uint8

const (
	colTsID colID = iota
	colValID
	colSFID
)

// blockKey identifies one decoded block: a part's prefix, the column, and the block index.
type blockKey struct {
	prefix string
	col    colID
	blk    int
}

// blockEntry is one cached decoded block: exactly one of i64/f64 is set (by the key's column).
type blockEntry struct {
	key   blockKey
	i64   []int64
	f64   []float64
	bytes int64
}

func newBlockCache(maxBytes int64) *blockCache {
	return &blockCache{
		maxBytes: maxBytes,
		ll:       list.New(),
		items:    make(map[blockKey]*list.Element),
		byPrefix: make(map[string]map[blockKey]struct{}),
	}
}

// get returns the cached block for key (shared, read-only), marking it most-recently-used.
func (c *blockCache) get(key blockKey) (*blockEntry, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	el, ok := c.items[key]
	if !ok {
		c.misses.Add(1)

		return nil, false
	}

	c.ll.MoveToFront(el)
	c.hits.Add(1)

	return el.Value.(*blockEntry), true
}

// put caches e under e.key, evicting the coldest blocks to stay within budget. A block larger than
// the whole budget is not cached; a key already present (a concurrent decode raced us) is kept.
func (c *blockCache) put(e *blockEntry) {
	if e.bytes > c.maxBytes {
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	if el, ok := c.items[e.key]; ok {
		c.ll.MoveToFront(el)

		return
	}

	el := c.ll.PushFront(e)
	c.items[e.key] = el
	c.bytes += e.bytes

	set := c.byPrefix[e.key.prefix]
	if set == nil {
		set = make(map[blockKey]struct{})
		c.byPrefix[e.key.prefix] = set
	}

	set[e.key] = struct{}{}

	for c.bytes > c.maxBytes {
		back := c.ll.Back()
		if back == nil {
			break
		}

		c.removeElem(back)
	}
}

// evictPrefix drops every block of the part at prefix (called when the part is reclaimed).
func (c *blockCache) evictPrefix(prefix string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	for key := range c.byPrefix[prefix] {
		if el, ok := c.items[key]; ok {
			c.removeElemKeepPrefix(el)
		}
	}

	delete(c.byPrefix, prefix)
}

// removeElem drops el from the list, index, prefix set, and byte total. Caller holds c.mu.
func (c *blockCache) removeElem(el *list.Element) {
	ent := el.Value.(*blockEntry)
	if set := c.byPrefix[ent.key.prefix]; set != nil {
		delete(set, ent.key)
		if len(set) == 0 {
			delete(c.byPrefix, ent.key.prefix)
		}
	}

	c.removeElemKeepPrefix(el)
}

// removeElemKeepPrefix drops el from the list, index, and byte total, but leaves the prefix set to the
// caller (evictPrefix clears the whole set at once). Caller holds c.mu.
func (c *blockCache) removeElemKeepPrefix(el *list.Element) {
	ent := el.Value.(*blockEntry)
	c.bytes -= ent.bytes
	delete(c.items, ent.key)
	c.ll.Remove(el)
}

// DecodeCacheStats is a snapshot of the decoded-block cache's effectiveness.
type DecodeCacheStats struct {
	Hits, Misses int64
	Bytes        int64
	Items        int // cached blocks
}

func (c *blockCache) stats() DecodeCacheStats {
	c.mu.Lock()
	bytes, items := c.bytes, len(c.items)
	c.mu.Unlock()

	return DecodeCacheStats{Hits: c.hits.Load(), Misses: c.misses.Load(), Bytes: bytes, Items: items}
}

// DecodeCacheStats returns the engine's decoded-block cache statistics and whether a cache is
// configured ([Config.DecodeCacheBytes] > 0). For operator visibility into cache effectiveness.
func (e *Engine) DecodeCacheStats() (DecodeCacheStats, bool) {
	if e.blockCache == nil {
		return DecodeCacheStats{}, false
	}

	return e.blockCache.stats(), true
}

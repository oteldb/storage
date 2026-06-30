package engine

import (
	"container/list"
	"sync"
	"sync/atomic"
)

// decodeCache is a byte-bounded LRU over decoded part columns, keyed by part prefix. A decoded part
// is immutable — the merge path only reads its column slices — so entries are shared across fetches
// without copying and stay valid until the part is retired (reclaim evicts its prefix) or the budget
// evicts them. It eliminates the re-decode that the object-store read cache cannot: even a cached
// backend read still re-decodes the columns on every query, whereas a decode-cache hit returns the
// already-decoded arrays.
type decodeCache struct {
	maxBytes int64

	hits, misses atomic.Int64

	mu    sync.Mutex
	ll    *list.List               // front = most-recently used; Value is *decodeEntry
	items map[string]*list.Element // prefix → element
	bytes int64
}

type decodeEntry struct {
	prefix string
	dp     *decodedPart
	bytes  int64
}

func newDecodeCache(maxBytes int64) *decodeCache {
	return &decodeCache{maxBytes: maxBytes, ll: list.New(), items: make(map[string]*list.Element)}
}

// get returns the cached decode for prefix (shared, read-only), marking it most-recently-used.
func (c *decodeCache) get(prefix string) (*decodedPart, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	el, ok := c.items[prefix]
	if !ok {
		c.misses.Add(1)

		return nil, false
	}

	c.ll.MoveToFront(el)
	c.hits.Add(1)

	return el.Value.(*decodeEntry).dp, true
}

// put caches dp under prefix, evicting LRU entries to stay within budget. A decode larger than the
// whole budget is not cached.
func (c *decodeCache) put(prefix string, dp *decodedPart) {
	b := dp.bytes()
	if b > c.maxBytes {
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	if el, ok := c.items[prefix]; ok {
		ent := el.Value.(*decodeEntry)
		c.bytes += b - ent.bytes
		ent.dp, ent.bytes = dp, b
		c.ll.MoveToFront(el)
	} else {
		c.items[prefix] = c.ll.PushFront(&decodeEntry{prefix: prefix, dp: dp, bytes: b})
		c.bytes += b
	}

	for c.bytes > c.maxBytes {
		back := c.ll.Back()
		if back == nil {
			break
		}

		c.removeElem(back)
	}
}

// evict drops prefix's entry (called when its part is reclaimed) and returns its decoded columns, or
// nil if absent. The caller may recycle the returned buffers: evict fires only for a reclaimed part
// (refs == 0, already out of the live set), so no in-flight fetch can still be reading them — unlike
// the LRU eviction in put, whose entry may belong to a live, in-flight part.
func (c *decodeCache) evict(prefix string) *decodedPart {
	c.mu.Lock()
	defer c.mu.Unlock()

	el, ok := c.items[prefix]
	if !ok {
		return nil
	}

	dp := el.Value.(*decodeEntry).dp
	c.removeElem(el)

	return dp
}

// removeElem drops el from the list and index and decrements the byte total. Caller holds c.mu.
func (c *decodeCache) removeElem(el *list.Element) {
	ent := el.Value.(*decodeEntry)
	c.bytes -= ent.bytes
	delete(c.items, ent.prefix)
	c.ll.Remove(el)
}

// DecodeCacheStats is a snapshot of the decoded-column cache's effectiveness.
type DecodeCacheStats struct {
	Hits, Misses int64
	Bytes        int64
	Items        int
}

func (c *decodeCache) stats() DecodeCacheStats {
	c.mu.Lock()
	bytes, items := c.bytes, len(c.items)
	c.mu.Unlock()

	return DecodeCacheStats{Hits: c.hits.Load(), Misses: c.misses.Load(), Bytes: bytes, Items: items}
}

// DecodeCacheStats returns the engine's decoded-column cache statistics and whether a cache is
// configured ([Config.DecodeCacheBytes] > 0). For operator visibility into cache effectiveness.
func (e *Engine) DecodeCacheStats() (DecodeCacheStats, bool) {
	if e.decodeCache == nil {
		return DecodeCacheStats{}, false
	}

	return e.decodeCache.stats(), true
}

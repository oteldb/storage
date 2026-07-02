package engine

import (
	"container/list"
	"slices"
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
	maxBytes int64

	hits, misses atomic.Int64

	// inflight counts the fetches currently drawing decode buffers (fetchStart/fetchEnd). The
	// freelists scale their bound with it: a fetch's pins hold evicted buffers back from the pool
	// until they are released, so under N concurrent fetches the pool must absorb roughly N fetches'
	// worth of returned buffers at once — a fixed cap sized for the steady state drops most of a
	// burst's returns on the floor and the next fetch re-allocates them all.
	inflight atomic.Int64

	mu       sync.Mutex
	ll       *list.List                       // front = most-recently used; Value is *blockEntry
	items    map[blockKey]*list.Element       // key → element
	byPrefix map[string]map[blockKey]struct{} // prefix → its keys, for evictPrefix
	bytes    int64

	// Recycled decode-output buffers, drawn by a miss's decode and refilled by eviction. Their own
	// locks are always taken while c.mu is held (recycle) or not at all (get), never the reverse.
	i64Free bufFreeList[int64]
	f64Free bufFreeList[float64]
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

// blockEntry is one cached decoded block: exactly one of i64/f64 is set (by the key's column). refs
// and evicted are guarded by the owning cache's mu: refs counts the fetches currently reading the
// block (each get/insert pins, each release unpins), and evicted marks that the entry has left the
// LRU. The buffer returns to the freelist only when refs hits 0 after evicted is set, so a reader
// holding a view of an evicted block never sees its buffer recycled underneath it.
type blockEntry struct {
	key     blockKey
	i64     []int64
	f64     []float64
	bytes   int64
	refs    int
	evicted bool
}

func newBlockCache(maxBytes int64) *blockCache {
	c := &blockCache{
		maxBytes: maxBytes,
		ll:       list.New(),
		items:    make(map[blockKey]*list.Element),
		byPrefix: make(map[string]map[blockKey]struct{}),
	}

	// Both freelists share one bound, scaled by the fetches currently in flight: the resident
	// contribution stays a few MiB at rest (blockBufCap buffers) and grows only while a burst is
	// actually holding that many buffers in play.
	capNow := func() int { return blockBufCap * (1 + int(c.inflight.Load())) }
	c.i64Free.capNow = capNow
	c.f64Free.capNow = capNow

	return c
}

// fetchStart registers a fetch that will draw decode buffers; fetchEnd unregisters it. The pair
// scales the freelist bound with concurrency (see blockCache.inflight).
func (c *blockCache) fetchStart() { c.inflight.Add(1) }
func (c *blockCache) fetchEnd()   { c.inflight.Add(-1) }

// get returns the cached block for key (shared, read-only), marking it most-recently-used and pinning
// it against recycling. On a hit the caller MUST call release once it is done reading the block.
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

	e := el.Value.(*blockEntry)
	e.refs++

	return e, true
}

// insert caches e (pinned for the caller) and returns the entry the caller should read and later
// release. If e.key is already present (a concurrent decode raced us) the resident entry is returned
// pinned and e's freshly-decoded buffer is recycled. A block larger than the whole budget is not
// cached but is still returned pinned, so release recycles its buffer. The returned entry always
// carries the canonical buffer to read (e's own, or the winner's on a race).
func (c *blockCache) insert(e *blockEntry) *blockEntry {
	c.mu.Lock()
	defer c.mu.Unlock()

	if el, ok := c.items[e.key]; ok {
		c.ll.MoveToFront(el)
		cur := el.Value.(*blockEntry)
		cur.refs++
		c.recycle(e) // our decode was redundant; hand its buffer back to the pool

		return cur
	}

	e.refs = 1 // pinned for the caller

	if e.bytes > c.maxBytes {
		e.evicted = true // never linked; release recycles the buffer

		return e
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
		if back == nil || back == el { // never evict the entry we just inserted and pinned
			break
		}

		c.removeElem(back)
	}

	return e
}

// release drops one pin taken by get or insert; once an entry's last pin is dropped after it has been
// evicted, its buffer returns to the freelist. Calling release exactly once per get/insert keeps the
// refcount balanced.
func (c *blockCache) release(e *blockEntry) {
	if e == nil {
		return
	}

	c.mu.Lock()
	e.refs--
	if e.refs == 0 && e.evicted {
		c.recycle(e)
	}
	c.mu.Unlock()
}

// recycle returns e's decoded buffer to the matching freelist and clears it, so a later miss decodes
// into it instead of minting a fresh slice. Caller holds c.mu; e must be evicted and unpinned.
func (c *blockCache) recycle(e *blockEntry) {
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
// caller (evictPrefix clears the whole set at once). It marks the entry evicted and recycles its
// buffer if no fetch is currently reading it; a pinned entry recycles later, on its last release.
// Caller holds c.mu.
func (c *blockCache) removeElemKeepPrefix(el *list.Element) {
	ent := el.Value.(*blockEntry)
	c.bytes -= ent.bytes
	delete(c.items, ent.key)
	c.ll.Remove(el)

	ent.evicted = true
	if ent.refs == 0 {
		c.recycle(ent)
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

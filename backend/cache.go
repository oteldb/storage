package backend

import (
	"container/list"
	"context"
	"slices"
	"sync"
	"sync/atomic"
)

// Cached wraps a [Backend] with a bounded in-memory LRU over read objects — the object-store read
// cache. It targets the cold tier (file/S3), where a part column is otherwise re-read over the
// network on every query: because part objects are write-once immutable, a cached value is never
// stale, so the only invalidation is eviction (by byte budget) and an explicit Write/Delete of the
// same key (manifest/index objects, which the wrapper keeps coherent). List and PutIfAbsent are
// passed through.
//
// maxBytes is the cache's total value-byte budget; objects larger than it are not cached (they
// would evict everything else). maxBytes ≤ 0 disables caching (the inner backend is returned
// unchanged). The wrapper preserves the [Backend] copy semantics: stored and returned slices are
// private copies, so a caller may retain or mutate them freely.
func Cached(inner Backend, maxBytes int64) Backend {
	if maxBytes <= 0 {
		return inner
	}

	return &cachedBackend{
		inner:    inner,
		maxBytes: maxBytes,
		items:    make(map[string]*list.Element),
		ll:       list.New(),
	}
}

// CacheStats is a snapshot of a cached backend's effectiveness.
type CacheStats struct {
	Hits, Misses int64
	Bytes        int64 // resident value bytes
	Items        int   // resident objects
}

type cachedBackend struct {
	inner    Backend
	maxBytes int64

	hits, misses atomic.Int64

	mu    sync.Mutex
	items map[string]*list.Element // key → element in ll
	ll    *list.List               // front = most-recently used; Value is *cacheEntry
	bytes int64
}

type cacheEntry struct {
	key string
	val []byte
}

func (c *cachedBackend) IsEphemeral() bool { return c.inner.IsEphemeral() }

func (c *cachedBackend) Read(ctx context.Context, key string) ([]byte, error) {
	if v, ok := c.load(key); ok {
		c.hits.Add(1)

		return v, nil
	}

	c.misses.Add(1)

	v, err := c.inner.Read(ctx, key)
	if err != nil {
		return nil, err
	}

	c.store(key, v) // store a copy; v is already a fresh copy owned by us, returned to the caller

	return v, nil
}

func (c *cachedBackend) Write(ctx context.Context, key string, data []byte) error {
	if err := c.inner.Write(ctx, key, data); err != nil {
		return err
	}

	c.store(key, data) // keep the cache coherent with the newly written value

	return nil
}

func (c *cachedBackend) PutIfAbsent(ctx context.Context, key string, data []byte) (bool, error) {
	ok, err := c.inner.PutIfAbsent(ctx, key, data)
	if err != nil {
		return false, err
	}

	if ok {
		c.store(key, data)
	}

	return ok, nil
}

func (c *cachedBackend) Delete(ctx context.Context, key string) error {
	err := c.inner.Delete(ctx, key)
	c.evict(key) // drop the entry whether or not the object existed

	return err
}

func (c *cachedBackend) List(ctx context.Context, prefix string) ([]string, error) {
	// Listings change as parts are added/removed; caching them would go stale. Pass through.
	return c.inner.List(ctx, prefix)
}

// Stats returns a snapshot of cache effectiveness (for benchmarks and operator visibility).
func (c *cachedBackend) Stats() CacheStats {
	c.mu.Lock()
	bytes, items := c.bytes, len(c.items)
	c.mu.Unlock()

	return CacheStats{Hits: c.hits.Load(), Misses: c.misses.Load(), Bytes: bytes, Items: items}
}

// load returns a private copy of the cached value for key and marks it most-recently-used.
func (c *cachedBackend) load(key string) ([]byte, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	el, ok := c.items[key]
	if !ok {
		return nil, false
	}

	c.ll.MoveToFront(el)

	return slices.Clone(el.Value.(*cacheEntry).val), true
}

// store caches a private copy of data under key, evicting LRU entries to stay within budget. An
// object larger than the whole budget is not cached.
func (c *cachedBackend) store(key string, data []byte) {
	if int64(len(data)) > c.maxBytes {
		c.evict(key) // a stale smaller entry must not linger under this key

		return
	}

	cp := slices.Clone(data)
	if cp == nil {
		cp = []byte{}
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	if el, ok := c.items[key]; ok {
		ent := el.Value.(*cacheEntry)
		c.bytes += int64(len(cp)) - int64(len(ent.val))
		ent.val = cp
		c.ll.MoveToFront(el)
	} else {
		c.items[key] = c.ll.PushFront(&cacheEntry{key: key, val: cp})
		c.bytes += int64(len(cp))
	}

	for c.bytes > c.maxBytes {
		back := c.ll.Back()
		if back == nil {
			break
		}

		c.removeElem(back)
	}
}

func (c *cachedBackend) evict(key string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if el, ok := c.items[key]; ok {
		c.removeElem(el)
	}
}

// removeElem drops el from the list and index and decrements the byte total. Caller holds c.mu.
func (c *cachedBackend) removeElem(el *list.Element) {
	ent := el.Value.(*cacheEntry)
	c.bytes -= int64(len(ent.val))
	delete(c.items, ent.key)
	c.ll.Remove(el)
}

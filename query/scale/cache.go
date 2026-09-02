package scale

import (
	"container/list"
	"context"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/oteldb/storage/query/fetch"
)

// Cache stores fetch results keyed by an opaque string (built by the [CacheFetcher] from the
// request's tenant, window, and equality specs). Implementations must be safe for concurrent
// use. [MemoryCache] is the built-in bounded-LRU implementation.
//
// Stored batches are treated as immutable snapshots: a [CacheFetcher] hands the cache an
// independent copy on Put, and callers of Get must not mutate the returned batches' sample
// columns in place (they may freely reslice/reorder the returned slice).
type Cache interface {
	Get(key string) ([]*fetch.Batch, bool)
	Put(key string, batches []*fetch.Batch)
}

// CacheFetcher memoizes the results of **fully-pushable** requests: a request is cacheable only
// when every matcher carries a serializable equality [fetch.EqualMatcher] spec, so the cache key
// is exact and a hit can never drop a matching series. Requests with a non-equality matcher
// (an opaque predicate that cannot be keyed) bypass the cache and hit Inner directly.
//
// The results cache does not auto-invalidate, so a request touching the **recent** window — where
// new samples may still arrive — must not be cached. The Freshness guard enforces that: a request
// whose window ends within Freshness of now bypasses the cache. Composed under a [SplitFetcher],
// this caches the settled sub-windows and always re-fetches the most recent one (the standard
// query-frontend behavior).
type CacheFetcher struct {
	Inner fetch.Fetcher
	Cache Cache

	// Freshness is the recent-window guard in nanoseconds: a request whose End is within
	// Freshness of now is not cached. 0 ⇒ no guard (every cacheable request is cached).
	Freshness int64

	// Now returns the current time as unix nanoseconds, for the Freshness guard. nil ⇒
	// [time.Now]. Tests inject a fixed clock.
	Now func() int64
}

var _ fetch.Fetcher = CacheFetcher{}

// Fetch returns the cached batches for r when present, otherwise fetches from Inner and stores
// the result. A non-cacheable request (or a nil Cache) is a transparent pass-through to Inner.
func (f CacheFetcher) Fetch(ctx context.Context, r fetch.Request) (fetch.Iterator, error) {
	key, ok := cacheKey(r)
	if !ok || f.Cache == nil || !f.settled(r) {
		return f.Inner.Fetch(ctx, r)
	}

	if cached, hit := f.Cache.Get(key); hit {
		return fetch.NewSliceIterator(cached), nil
	}

	it, err := f.Inner.Fetch(ctx, r)
	if err != nil {
		return nil, err
	}

	batches, derr := fetch.Drain(ctx, it)
	cerr := it.Close()

	if derr != nil {
		return nil, derr
	}

	if cerr != nil {
		return nil, cerr
	}

	f.Cache.Put(key, batches)

	return fetch.NewSliceIterator(batches), nil
}

// Unwrap exposes the cached fetcher so [fetch.CounterOf] can reach the engine's Count for the
// count() pushdown (count is not cached — it reads through to Inner).
func (f CacheFetcher) Unwrap() fetch.Fetcher { return f.Inner }

// settled reports whether r's window has aged past the Freshness horizon — i.e. its End is old
// enough that no new sample will land in it, so the result is safe to cache. With Freshness ≤ 0
// the guard is off and every (otherwise cacheable) request is settled.
func (f CacheFetcher) settled(r fetch.Request) bool {
	if f.Freshness <= 0 {
		return true
	}

	return r.End <= f.now()-f.Freshness
}

func (f CacheFetcher) now() int64 {
	if f.Now != nil {
		return f.Now()
	}

	return time.Now().UnixNano()
}

// cacheKey builds a deterministic key for r, reporting false when the request is not cacheable
// (any matcher lacks an equality spec). The key folds tenant, window, and the equality specs
// sorted by (name, value) so matcher order does not change the key. A zero-matcher request
// (select-all in the window) is cacheable.
func cacheKey(r fetch.Request) (string, bool) {
	specs := make([]string, 0, len(r.Matchers))

	for i := range r.Matchers {
		m := r.Matchers[i].Spec
		if m == nil {
			return "", false
		}

		specs = append(specs, m.Name+"\x00"+m.Value)
	}

	sort.Strings(specs)

	var sb strings.Builder

	sb.WriteString(string(r.Tenant))
	sb.WriteByte(0x1f)
	sb.WriteString(strconv.FormatInt(r.Start, 10))
	sb.WriteByte(0x1f)
	sb.WriteString(strconv.FormatInt(r.End, 10))

	for _, s := range specs {
		sb.WriteByte(0x1f)
		sb.WriteString(s)
	}

	return sb.String(), true
}

// MemoryCache is a bounded, LRU [Cache] held entirely in memory. It is safe for concurrent use.
// On Put it stores a deep copy of the batches (independent of the producer's reused buffers);
// on Get it returns a fresh slice of those snapshots so a caller's reslicing never disturbs the
// cache's order.
type MemoryCache struct {
	mu  sync.Mutex
	max int // maximum entries; ≤ 0 ⇒ unbounded
	ll  *list.List
	m   map[string]*list.Element
}

type cacheEntry struct {
	key     string
	batches []*fetch.Batch
}

// NewMemoryCache returns an LRU cache holding at most maxEntries results (≤ 0 ⇒ unbounded).
func NewMemoryCache(maxEntries int) *MemoryCache {
	return &MemoryCache{max: maxEntries, ll: list.New(), m: make(map[string]*list.Element)}
}

// Get returns a fresh slice of the cached batches and marks the entry most-recently-used.
func (c *MemoryCache) Get(key string) ([]*fetch.Batch, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	el, ok := c.m[key]
	if !ok {
		return nil, false
	}

	c.ll.MoveToFront(el)

	return slices.Clone(el.Value.(*cacheEntry).batches), true
}

// Put stores a deep copy of batches under key, evicting the least-recently-used entry when the
// cache exceeds its bound.
func (c *MemoryCache) Put(key string, batches []*fetch.Batch) {
	c.mu.Lock()
	defer c.mu.Unlock()

	snapshot := cloneBatches(batches)

	if el, ok := c.m[key]; ok {
		el.Value.(*cacheEntry).batches = snapshot
		c.ll.MoveToFront(el)

		return
	}

	c.m[key] = c.ll.PushFront(&cacheEntry{key: key, batches: snapshot})

	if c.max > 0 && c.ll.Len() > c.max {
		back := c.ll.Back()
		if back != nil {
			c.ll.Remove(back)
			delete(c.m, back.Value.(*cacheEntry).key)
		}
	}
}

// Len returns the number of cached entries (testing/introspection).
func (c *MemoryCache) Len() int {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.ll.Len()
}

// cloneBatches deep-copies a batch slice — every per-sample buffer is cloned so the snapshot is
// independent of any buffer the producer reuses. The series identity is shared (immutable).
//
// Every payload field must be copied, not just the metric ones: dropping [fetch.Batch.Columns]
// would serve a record (log/trace) hit as the right number of rows with no data, and dropping
// [fetch.Batch.ScaleFactors] would serve a sampled metric hit as if every sample weighed 1, biasing
// the embedder's counts and rates. A nil field stays nil — that is the "not present" signal for
// both, and [slices.Clone] preserves it.
func cloneBatches(batches []*fetch.Batch) []*fetch.Batch {
	out := make([]*fetch.Batch, len(batches))
	for i, b := range batches {
		out[i] = &fetch.Batch{
			ID:           b.ID,
			Series:       b.Series,
			Timestamps:   slices.Clone(b.Timestamps),
			Values:       slices.Clone(b.Values),
			ScaleFactors: slices.Clone(b.ScaleFactors),
			Columns:      cloneColumns(b.Columns),
		}
	}

	return out
}

// cloneColumns deep-copies materialized record columns, including each byte cell: the producing
// engine hands out cells that are views into a pooled per-part blob, so retaining them would let a
// later fetch overwrite a cached result in place.
func cloneColumns(cols []fetch.NamedColumn) []fetch.NamedColumn {
	if cols == nil {
		return nil
	}

	out := make([]fetch.NamedColumn, len(cols))

	for i, c := range cols {
		out[i] = fetch.NamedColumn{
			Name:    c.Name,
			Kind:    c.Kind,
			Int64:   slices.Clone(c.Int64),
			Float64: slices.Clone(c.Float64),
		}

		if c.Bytes == nil {
			continue
		}

		// One backing blob per column: the cells are contiguous in the snapshot, so a cached
		// result costs two allocations instead of one per row.
		var n int
		for _, v := range c.Bytes {
			n += len(v)
		}

		blob := make([]byte, 0, n)
		cells := make([][]byte, len(c.Bytes))

		for j, v := range c.Bytes {
			if v == nil { // an absent cell is not an empty one
				continue
			}

			start := len(blob)
			blob = append(blob, v...)
			cells[j] = blob[start:len(blob):len(blob)]
		}

		out[i].Bytes = cells
	}

	return out
}

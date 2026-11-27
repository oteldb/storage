package seriestable

import (
	"container/list"
	"sync"

	"github.com/oteldb/storage/internal/memsize"
	"github.com/oteldb/storage/signal"
)

// IDCache retains byte-bounded routing filters and validated ID arrays.
type IDCache struct {
	mu                      sync.Mutex
	limit, used             int64
	filterLimit, filterUsed int64
	mapSlots                int
	runs                    map[*Run]*cachedIDs
	ready                   list.List
}

type cachedIDs struct {
	owner  *Run
	ids    []signal.SeriesID
	elem   *list.Element
	charge int64
	pins   int
}

// NewIDCache creates an empty cache; a nonpositive limit disables retention.
func NewIDCache(limit int64) *IDCache {
	return newIDCache(limit, limit/2)
}

func newIDCache(limit, filterLimit int64) *IDCache {
	base := memsize.Of[IDCache]() + memsize.MapBase
	if limit < base {
		return nil
	}
	filterLimit = min(filterLimit, limit-base)
	return &IDCache{limit: limit, used: base, filterLimit: filterLimit, runs: make(map[*Run]*cachedIDs)}
}

// SizeBytes includes filters, arrays, pending construction and cache bookkeeping.
func (c *IDCache) SizeBytes() int64 {
	if c == nil {
		return 0
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.used + c.filterUsed
}

// Filters keep their share even when an exact array is evicted. Arrays can use any spare bytes.
func (c *IDCache) reserveFilter(count uint32) *idRoutingFilter {
	if c == nil || c.filterLimit == 0 {
		return nil
	}
	words := routingFilterWords(count)
	charge := memsize.Of[idRoutingFilter]() + int64(words)*8
	c.mu.Lock()
	defer c.mu.Unlock()
	if charge > c.filterLimit-c.filterUsed {
		return nil
	}
	for c.used+c.filterUsed+charge > c.limit {
		if !c.evictOldestLocked() {
			return nil
		}
	}
	c.filterUsed += charge
	return &idRoutingFilter{words: make([]uint64, words)}
}

func (c *IDCache) releaseFilter(filter *idRoutingFilter) {
	if filter != nil {
		c.mu.Lock()
		c.filterUsed -= memsize.Of[idRoutingFilter]() + memsize.Slice(filter.words)
		c.mu.Unlock()
	}
}

func (c *IDCache) reserve(r *Run) *cachedIDs {
	if c == nil {
		return nil
	}
	charge := int64(r.Count())*memsize.Of[signal.SeriesID]() + memsize.Of[cachedIDs]() + memsize.Of[list.Element]()
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.runs[r] != nil {
		return nil
	}
	if charge > c.limit-c.filterUsed-memsize.Of[IDCache]()-memsize.MapBase {
		return nil
	}
	mapCharge := func() int64 {
		if len(c.runs)+1 > c.mapSlots {
			return 2 * memsize.MapEntry[*Run, *cachedIDs]()
		}
		return 0
	}
	for c.used+c.filterUsed+charge+mapCharge() > c.limit {
		if !c.evictOldestLocked() {
			return nil
		}
	}
	if extra := mapCharge(); extra > 0 {
		c.used += extra
		c.mapSlots++
	}

	entry := &cachedIDs{owner: r, ids: make([]signal.SeriesID, int(r.Count())), charge: charge}
	c.runs[r] = entry
	c.used += charge
	return entry
}

func (c *IDCache) publish(r *Run) {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if entry := c.runs[r]; entry != nil && entry.elem == nil {
		entry.elem = c.ready.PushFront(entry)
	}
}

func (c *IDCache) remove(r *Run) {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.removeLocked(r)
}

func (c *IDCache) evictOldestLocked() bool {
	for elem := c.ready.Back(); elem != nil; elem = elem.Prev() {
		if entry := elem.Value.(*cachedIDs); entry.pins == 0 {
			c.removeLocked(entry.owner)
			return true
		}
	}
	return false
}

func (c *IDCache) removeLocked(r *Run) {
	entry := c.runs[r]
	if entry == nil {
		return
	}
	if entry.elem != nil {
		c.ready.Remove(entry.elem)
	}
	delete(c.runs, r)
	c.used -= entry.charge
	if len(c.runs) == 0 {
		c.runs = make(map[*Run]*cachedIDs)
		c.mapSlots = 0
		c.used = memsize.Of[IDCache]() + memsize.MapBase
	}
}

func (c *cachedIDs) set(ref uint32, id signal.SeriesID) {
	if c != nil {
		c.ids[ref] = id
	}
}

func (c *IDCache) acquire(r *Run) *cachedIDs {
	if c == nil {
		return nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	entry := c.runs[r]
	if entry == nil || entry.elem == nil {
		return nil
	}
	entry.pins++
	c.ready.MoveToFront(entry.elem)
	return entry
}

func (c *IDCache) release(entry *cachedIDs) {
	if c == nil || entry == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	entry.pins--
}

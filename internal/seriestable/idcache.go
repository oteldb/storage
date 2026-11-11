package seriestable

import (
	"container/list"
	"sync"

	"github.com/oteldb/storage/internal/memsize"
	"github.com/oteldb/storage/signal"
)

// IDCache retains byte-bounded, validated ID arrays; descriptors stay on disk.
type IDCache struct {
	mu          sync.Mutex
	limit, used int64
	mapSlots    int
	runs        map[*Run]*cachedIDs
	ready       list.List
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
	base := memsize.Of[IDCache]() + memsize.MapBase
	if limit < base {
		return nil
	}
	return &IDCache{limit: limit, used: base, runs: make(map[*Run]*cachedIDs)}
}

// SizeBytes includes retained arrays, pending construction and cache bookkeeping.
func (c *IDCache) SizeBytes() int64 {
	if c == nil {
		return 0
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.used
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
	if charge > c.limit-memsize.Of[IDCache]()-memsize.MapBase {
		return nil
	}
	mapCharge := func() int64 {
		if len(c.runs)+1 > c.mapSlots {
			return 2 * memsize.MapEntry[*Run, *cachedIDs]()
		}
		return 0
	}
	for c.used+charge+mapCharge() > c.limit {
		oldest := c.ready.Back()
		for oldest != nil && oldest.Value.(*cachedIDs).pins > 0 {
			oldest = oldest.Prev()
		}
		if oldest == nil {
			return nil
		}
		c.removeLocked(oldest.Value.(*cachedIDs).owner)
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

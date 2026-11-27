package engine

import (
	"sync"

	"github.com/oteldb/storage/internal/memsize"
	"github.com/oteldb/storage/signal"
)

type verifiedID struct {
	high uint64
	root uint64
}

// A cache entry proves full-ID membership in one pinned immutable root. Using
// a token keeps retired run handles out of the cache's object graph.
type verifiedIdentityCache struct {
	mu           sync.Mutex
	limit, used  int64
	entries      map[uint64]verifiedID
	collisions   map[signal.SeriesID]uint64
	hits, misses uint64
}

// VerifiedIdentityStats reports positive ID cache reuse, separate from page-cache hits.
type VerifiedIdentityStats struct {
	Bytes        int64
	Hits, Misses uint64
}

// VerifiedIdentityCacheStats returns cumulative probes and charged resident bytes.
func (e *Engine) VerifiedIdentityCacheStats() VerifiedIdentityStats {
	c := e.verifiedIdentities
	if c == nil {
		return VerifiedIdentityStats{}
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return VerifiedIdentityStats{Bytes: c.used, Hits: c.hits, Misses: c.misses}
}

func newVerifiedIdentityCache(limit int64) *verifiedIdentityCache {
	base := memsize.Of[verifiedIdentityCache]() + memsize.MapBase
	if limit < base {
		return nil
	}
	return &verifiedIdentityCache{limit: limit, used: base, entries: make(map[uint64]verifiedID)}
}

func (c *verifiedIdentityCache) sizeBytes() int64 {
	if c == nil {
		return 0
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.used
}

func (c *verifiedIdentityCache) clear() {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.entries = make(map[uint64]verifiedID)
	c.collisions = nil
	c.used = memsize.Of[verifiedIdentityCache]() + memsize.MapBase
}

func (c *verifiedIdentityCache) match(snapshot *seriesSnapshot, entries identitySnapshot, known []bool, proven int) (hits int) {
	if c == nil {
		return 0
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.entries) == 0 {
		c.misses += uint64(len(known) - proven)
		return 0
	}
	for i, ref := range entries.refs {
		if known[i] {
			continue
		}
		id := entries.view.ID(int(ref))
		entry, ok := c.entries[id.Lo]
		if !ok {
			continue
		}
		root := entry.root
		if entry.high != id.Hi {
			root = c.collisions[id]
		}
		if _, live := snapshot.tokens[root]; live {
			known[i] = true
			hits++
		}
	}
	c.hits += uint64(hits)
	c.misses += uint64(len(known) - proven - hits)
	return hits
}

func (c *verifiedIdentityCache) seed(root uint64, entries identitySnapshot, known []bool) {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	for i, ref := range entries.refs {
		if !known[i] && !c.putLocked(root, entries.view.ID(int(ref))) {
			return
		}
	}
}

func (c *verifiedIdentityCache) putIDs(root uint64, ids []signal.SeriesID) {
	if c == nil || len(ids) == 0 {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, id := range ids {
		c.putLocked(root, id)
	}
}

func (c *verifiedIdentityCache) putLocked(root uint64, id signal.SeriesID) bool {
	entry, ok := c.entries[id.Lo]
	if !ok {
		charge := 2 * memsize.MapEntry[uint64, verifiedID]()
		if charge > c.limit-c.used {
			return false
		}
		c.entries[id.Lo] = verifiedID{high: id.Hi, root: root}
		c.used += charge
		return true
	}
	// A slow lookup from an older snapshot must not overwrite a newer root.
	if entry.high == id.Hi {
		if root > entry.root {
			c.entries[id.Lo] = verifiedID{high: id.Hi, root: root}
		}
		return true
	}
	if cached, exists := c.collisions[id]; exists {
		c.collisions[id] = max(root, cached)
		return true
	}
	charge := 2 * memsize.MapEntry[signal.SeriesID, uint64]()
	if c.collisions == nil {
		charge += memsize.MapBase
	}
	if charge > c.limit-c.used {
		return false
	}
	if c.collisions == nil {
		c.collisions = make(map[signal.SeriesID]uint64)
	}
	c.collisions[id] = root
	c.used += charge
	return true
}

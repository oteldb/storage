package engine

import (
	"bytes"
	"sync"

	"github.com/go-faster/errors"

	"github.com/oteldb/storage/internal/memsize"
	"github.com/oteldb/storage/signal"
)

// Entries prove exact canonical bytes in one immutable catalog generation, never ID existence alone.
// A full generation stops admitting entries instead of allocating on every miss during a large scrape.
type verifiedIdentityCache struct {
	mu           sync.Mutex
	generation   uint64
	limit, used  int64
	entries      map[signal.SeriesID][]byte
	hits, misses uint64
}

// VerifiedIdentityStats reports canonical verification reuse, separate from page-cache hits.
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
	return &verifiedIdentityCache{limit: limit, used: base, entries: make(map[signal.SeriesID][]byte)}
}

func (c *verifiedIdentityCache) sizeBytes() int64 {
	if c == nil {
		return 0
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.used
}

func (c *verifiedIdentityCache) clearLocked() {
	c.entries = make(map[signal.SeriesID][]byte)
	c.used = memsize.Of[verifiedIdentityCache]() + memsize.MapBase
}

func (c *verifiedIdentityCache) clear() {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.clearLocked()
}

func (c *verifiedIdentityCache) get(generation uint64, id signal.SeriesID) ([]byte, bool) {
	if c == nil {
		return nil, false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.generation != generation {
		return nil, false
	}
	raw, ok := c.entries[id]
	return raw, ok
}

func (c *verifiedIdentityCache) match(generation uint64, entries identitySnapshot, known []bool) (hits int, err error) {
	if c == nil {
		return 0, nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	defer func() { c.hits += uint64(hits); c.misses += uint64(len(known) - hits) }()
	if c.generation != generation {
		return 0, nil
	}
	var canonical []byte
	for i, ref := range entries.refs {
		if raw, ok := c.entries[entries.view.ID(int(ref))]; ok {
			canonical = entries.view.AppendCanonical(canonical[:0], int(ref))
			if !bytes.Equal(raw, canonical) {
				return 0, errors.New("engine: shared series canonical identity conflict")
			}
			known[i] = true
			hits++
		}
	}
	return hits, nil
}

func (c *verifiedIdentityCache) canAdmit(generation uint64) bool {
	if c == nil {
		return false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.generation != generation || c.limit-c.used > 2*memsize.MapEntry[signal.SeriesID, []byte]()
}

func (c *verifiedIdentityCache) put(generation uint64, id signal.SeriesID, raw []byte) bool {
	if c == nil {
		return false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if generation < c.generation {
		return false
	}
	if c.generation != generation {
		c.clearLocked()
		c.generation = generation
	}
	if _, ok := c.entries[id]; ok {
		return true
	}
	// Charge map growth slack as well as the owned byte payload before allocating.
	charge := 2*memsize.MapEntry[signal.SeriesID, []byte]() + int64(len(raw))
	if charge > c.limit-c.used {
		return false
	}
	c.entries[id] = append([]byte(nil), raw...)
	c.used += charge
	return true
}

package profile

import (
	"context"
	"math"

	"github.com/maypok86/otter/v2"
	"github.com/maypok86/otter/v2/stats"
)

// SymbolCache holds decoded [Tables] keyed by the immutable object they were read from, bounded by
// their resident bytes. A key must name content that never changes, such as a part prefix. Safe for
// concurrent use; a nil cache decodes on every call.
type SymbolCache struct {
	maxBytes int64
	values   *otter.Cache[string, *Tables]
	stats    *stats.Counter
}

// CacheStats is a snapshot of a [SymbolCache]'s effectiveness.
type CacheStats struct {
	Hits, Misses int64
	Bytes        int64 // resident decoded bytes, as [Tables.Size] prices them
	Items        int
}

// NewSymbolCache returns a cache bounded to maxBytes of decoded tables, or nil when maxBytes ≤ 0. A
// budget of 4 GiB or more is capped just below it.
func NewSymbolCache(maxBytes int64) *SymbolCache {
	if maxBytes <= 0 {
		return nil
	}

	// otter weighs an entry in a uint32. Capping the budget below its maximum means every entry that
	// is kept weighs exactly its size, and one whose weight saturates is over budget, so never kept.
	maxBytes = min(maxBytes, math.MaxUint32-1)

	c := &SymbolCache{maxBytes: maxBytes, stats: stats.NewCounter()}
	c.values = otter.Must(&otter.Options[string, *Tables]{
		MaximumWeight: uint64(maxBytes),
		Weigher:       weighTables,
		StatsRecorder: c.stats,
	})

	return c
}

func weighTables(_ string, t *Tables) uint32 {
	return uint32(min(t.Size(), math.MaxUint32))
}

// Get returns the tables under key, decoding what load returns on a miss. Concurrent misses on one
// key usually share a load; a failed load is not cached.
func (c *SymbolCache) Get(
	ctx context.Context, key string, load func(context.Context) (map[string][]byte, error),
) (*Tables, error) {
	return c.get(ctx, key, func(ctx context.Context) (*Tables, error) {
		raw, err := load(ctx)
		if err != nil {
			return nil, err
		}

		return DecodeTables(raw)
	})
}

// Stats returns the cache's hits, misses and resident size. Zero for a nil cache.
func (c *SymbolCache) Stats() CacheStats {
	if c == nil {
		return CacheStats{}
	}

	c.values.CleanUp()

	s := c.stats.Snapshot()

	return CacheStats{
		Hits:   int64(s.Hits),
		Misses: int64(s.Misses),
		Bytes:  int64(c.values.WeightedSize()),
		Items:  c.values.EstimatedSize(),
	}
}

func (c *SymbolCache) get(
	ctx context.Context, key string, decode func(context.Context) (*Tables, error),
) (*Tables, error) {
	if c == nil {
		return decode(ctx)
	}

	t, err := c.values.Get(ctx, key, otter.LoaderFunc[string, *Tables](func(ctx context.Context, _ string) (*Tables, error) {
		return decode(ctx)
	}))
	if err != nil {
		return nil, err
	}

	// otter drops an entry heavier than the whole budget only at its next maintenance; drop it now,
	// so one oversized part is served uncached rather than sitting over budget.
	if t.Size() > c.maxBytes {
		c.values.Invalidate(key)
	}

	return t, nil
}

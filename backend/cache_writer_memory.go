package backend

import (
	"math"

	"github.com/oteldb/storage/internal/memsize"
)

// WriterMemory includes the inner writer and cache invalidation metadata.
func (c *cachedBackend) WriterMemory(key string) (int64, bool) {
	n, known := WriterMemory(c.inner, key)
	if !known {
		return 0, false
	}
	metadata := memsize.Of[cacheInvalidatingWriter]()
	if n > math.MaxInt64-metadata || int64(len(key)) > math.MaxInt64-metadata-n {
		return math.MaxInt64, true
	}
	return n + metadata + int64(len(key)), true
}

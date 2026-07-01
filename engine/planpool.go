package engine

import (
	"sync"

	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/signal"
)

// A fetch builds per-series identity + head/flush/recent snapshot structures sized to the matched
// count, so a broad fetch allocates several thousand-entry structures every call — a dominant
// per-fetch allocation now that the block cache + streaming merge no longer re-decode columns.
// planMapPools recycles them across fetches: a fetch borrows the structures (cleared) and returns
// them on releaseParts, so steady-state fetching reuses their capacity instead of re-allocating.
//
// The identity store is a *slice*, not a map: a map with a large value type (signal.Series is > 128
// bytes) stores each value indirectly, so every insert heap-allocates and recycling the map structure
// does not help. A slice stores the identities inline in one backing array (zero per-entry allocs)
// and recycles cleanly. The snapshot maps hold *fetch.Batch (a pointer, stored inline), so recycling
// the map structure works. A returned batch copies out what it needs (its Series by value, its
// samples via collect), so both are dead once the fetch returns — safe to clear and reuse.
//
// Like the decode-buffer pool these are GC-stable freelists, not sync.Pools: a sync.Pool is cleared
// at every GC, so under the allocation-driven collections a query load drives it would drop the
// structures and re-allocate each fetch; a rooted freelist keeps their capacity across collections.
type planMapPools struct {
	series sliceFreeList[signal.Series]
	batch  mapFreeList[signal.SeriesID, *fetch.Batch]
}

// planMapCap bounds the recycled maps of each kind — a small multiple of the per-fetch parallelism,
// like the decode pool, so memory stays ≈ cap × peak-map-size without over-retaining.
const planMapCap = prefetchConcurrency * 4

// mapFreeList is a bounded, GC-stable recycler for maps of one key/value type. The zero value is
// usable. Safe for concurrent use.
type mapFreeList[K comparable, V any] struct {
	mu   sync.Mutex
	free []map[K]V
}

// get returns a cleared map from the freelist, or a fresh one sized for n entries when empty.
func (p *mapFreeList[K, V]) get(n int) map[K]V {
	p.mu.Lock()

	if k := len(p.free); k > 0 {
		m := p.free[k-1]
		p.free[k-1] = nil
		p.free = p.free[:k-1]
		p.mu.Unlock()

		return m
	}

	p.mu.Unlock()

	return make(map[K]V, n)
}

// put clears m and returns it to the freelist (dropped when full, bounding memory). A nil map is
// ignored.
func (p *mapFreeList[K, V]) put(m map[K]V) {
	if m == nil {
		return
	}

	clear(m)

	p.mu.Lock()
	if len(p.free) < planMapCap {
		p.free = append(p.free, m)
	}
	p.mu.Unlock()
}

// sliceFreeList is a bounded, GC-stable recycler for slices of one element type. get returns a
// zeroed slice of the requested length (so unset positions read as the zero value); put returns a
// slice for reuse. The zero value is usable; safe for concurrent use.
type sliceFreeList[T any] struct {
	mu   sync.Mutex
	free [][]T
}

func (p *sliceFreeList[T]) get(n int) []T {
	p.mu.Lock()

	if k := len(p.free); k > 0 {
		s := p.free[k-1]
		p.free[k-1] = nil
		p.free = p.free[:k-1]
		p.mu.Unlock()

		if cap(s) >= n {
			s = s[:n]
			clear(s) // zero reused slots so absent positions are the zero value

			return s
		}
	} else {
		p.mu.Unlock()
	}

	return make([]T, n)
}

func (p *sliceFreeList[T]) put(s []T) {
	if s == nil {
		return
	}

	p.mu.Lock()
	if len(p.free) < planMapCap {
		p.free = append(p.free, s)
	}
	p.mu.Unlock()
}

func (e *Engine) getSeriesSlice(n int) []signal.Series               { return e.planMaps.series.get(n) }
func (e *Engine) putSeriesSlice(s []signal.Series)                   { e.planMaps.series.put(s) }
func (e *Engine) getBatchMap(n int) map[signal.SeriesID]*fetch.Batch { return e.planMaps.batch.get(n) }
func (e *Engine) putBatchMap(m map[signal.SeriesID]*fetch.Batch)     { e.planMaps.batch.put(m) }

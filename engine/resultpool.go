package engine

import "sync"

// resultFreeList is a GC-stable, bounded recycler for result column buffers ([]T). It replaces the
// sync.Pool the batch/window buffers used to sit in: sync.Pool is emptied at the start of every GC,
// and sustained query load drives allocation-triggered GCs constantly, so the pooled buffers were
// dropped before reuse and every fetch re-minted its result columns (the ensureCap churn in the
// query alloc profile). Entries here are rooted references that survive GC.
//
// Two bounds keep the resident contribution fixed: an entry count (so a flood of tiny buffers can't
// grow the slice header list without limit) and a total retained capacity in elements (so a burst of
// huge range-query buffers can't pin tens of MiB). A put past either bound drops the buffer to the
// GC — the list never grows the resident set beyond resultPoolElems × sizeof(T).
type resultFreeList[T any] struct {
	mu    sync.Mutex
	free  [][]T
	elems int64 // Σ cap over free; bounds retained bytes at elems × sizeof(T)
}

const (
	// resultPoolEntries bounds each list's entry count.
	resultPoolEntries = 4096
	// resultPoolElems bounds each list's total retained capacity: 2 Mi elements = 16 MiB of
	// int64/float64 per list, a fixed budget-neutral ceiling far below the churn it removes.
	resultPoolElems = 2 << 20
)

// get returns a recycled buffer truncated to length 0, or nil when the list is empty — the caller
// allocates on nil, so an empty pool behaves exactly like the unpooled path.
func (p *resultFreeList[T]) get() []T {
	p.mu.Lock()

	n := len(p.free)
	if n == 0 {
		p.mu.Unlock()

		return nil
	}

	s := p.free[n-1]
	p.free[n-1] = nil
	p.free = p.free[:n-1]
	p.elems -= int64(cap(s))
	p.mu.Unlock()

	return s[:0]
}

// put returns s to the list for reuse; buffers past either bound (or with no capacity) are dropped.
func (p *resultFreeList[T]) put(s []T) {
	if cap(s) == 0 {
		return
	}

	p.mu.Lock()
	if len(p.free) < resultPoolEntries && p.elems+int64(cap(s)) <= resultPoolElems {
		p.free = append(p.free, s)
		p.elems += int64(cap(s))
	}
	p.mu.Unlock()
}

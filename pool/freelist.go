package pool

import (
	"runtime"
	"sync"
)

// FreeList is a GC-stable, bounded recycler for pointers of type T. It is the
// zero-allocation replacement for [sync.Pool] in paths where buffers must survive
// allocation-driven GC bursts.
//
// Problem it solves: [sync.Pool] is cleared by the runtime at the start of every GC
// (its per-P caches are dropped, victim caches rotated out over the next cycle). Under
// sustained allocation pressure — concurrent fetches plus other large allocators — a
// sync.Pool stays drained, so every Get returns a fresh object and the capacity of its
// backing slices is lost. The decode-buffer path pays for that with a full chunk.resize
// reallocation on every fetch (the disk_io profile showed ~38 GB/35 s of resize churn,
// >50% of CPU in GC).
//
// FreeList holds its entries as ordinary rooted references (a guarded slice), so they
// are NOT collectable and NOT cleared at GC: a buffer Put back here keeps its capacity
// across any number of collections until the next Get reclaims it. Capacity bounds
// resident memory; a Put past capacity drops the pointer (it becomes collectable
// normally), so the list cannot grow unbounded.
//
// Get returns a recycled pointer or nil when empty — callers allocate on a nil return.
// The mutex critical section is a slice head/tail op (no allocation), so contention is
// minimal; for very high fan-in a sharded list can be layered on top.
type FreeList[T any] struct {
	mu   sync.Mutex
	free []*T
	cap  int
}

// NewFreeList returns a FreeList holding up to capacity recycled pointers. A capacity
// below 1 disables recycling (Put is a no-op, Get always returns nil) — useful for
// tests and size-0 fast paths. Size the capacity to the peak number of concurrently
// live buffers: in-flight buffers that don't fit are dropped and GC'd, keeping memory
// bounded to roughly capacity × buffer-size.
func NewFreeList[T any](capacity int) *FreeList[T] {
	if capacity < 0 {
		capacity = 0
	}

	return &FreeList[T]{cap: capacity}
}

// Get returns a recycled *T, or nil when the list is empty. A nil return means the
// caller should allocate (there is no New constructor — keeping the type literal avoids
// hiding allocations behind a pool that promises "no allocation").
func (p *FreeList[T]) Get() *T {
	p.mu.Lock()

	var x *T
	if n := len(p.free); n > 0 {
		x = p.free[n-1]
		p.free[n-1] = nil // drop the reference so the slot doesn't pin the object
		p.free = p.free[:n-1]
	}

	p.mu.Unlock()

	return x
}

// Put returns x to the list for reuse. A nil x is ignored. When the list is full, x is
// dropped (left for GC) rather than queued — this is what bounds resident memory.
func (p *FreeList[T]) Put(x *T) {
	if x == nil || p.cap == 0 {
		return
	}

	p.mu.Lock()
	if len(p.free) < p.cap {
		p.free = append(p.free, x)
	}
	p.mu.Unlock()
}

// DefaultCapacity returns a sensible FreeList capacity for a path bounded by per-fetch
// parallelism: the greater of runtime.GOMAXPROCS(0) and floor. It covers the expected
// peak of in-flight buffers (one per concurrent decode) without over-retaining.
func DefaultCapacity(floor int) int {
	if n := runtime.GOMAXPROCS(0); n > floor {
		floor = n
	}

	return floor
}

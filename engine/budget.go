package engine

import "sync"

// DecodeBudget caps the total in-flight decoded bytes across concurrent queries, so query concurrency
// cannot drive RSS past a bound. Each query estimates its decode footprint (the column buffers it
// will materialize across the parts it touches) and acquires that many bytes before decoding,
// releasing them when the fetch ends; an acquire blocks until enough is free. Under load this trades
// latency for a memory ceiling — N heavy concurrent queries serialize through the budget instead of
// each allocating GBs at once (the concurrency cliff).
//
// A query whose own estimate exceeds the whole budget is admitted alone (it cannot be bounded below
// its own footprint), so an unsatisfiable request never deadlocks. The budget is acquired once per
// query (the whole estimate up front), not incrementally per part, so two queries cannot each hold a
// partial reservation while waiting on the other.
//
// One budget may be shared by multiple engines (via [Config.DecodeBudget]) so the cap bounds the
// process-wide decode footprint rather than a per-engine (per-tenant) one.
type DecodeBudget struct {
	maxBytes int64
	mu       sync.Mutex
	cond     *sync.Cond
	used     int64
}

// NewDecodeBudget returns a budget capping in-flight decoded bytes at maxBytes. maxBytes ≤ 0 disables
// it (every acquire/release is a no-op).
func NewDecodeBudget(maxBytes int64) *DecodeBudget {
	b := &DecodeBudget{maxBytes: maxBytes}
	b.cond = sync.NewCond(&b.mu)

	return b
}

// acquire blocks until n bytes of budget are free, then reserves them. It admits the request
// immediately when nothing else is in flight (even if n exceeds the whole budget), so an
// over-budget query runs alone rather than waiting forever. A nil/disabled budget or n ≤ 0 is a
// no-op.
func (b *DecodeBudget) acquire(n int64) {
	if b == nil || b.maxBytes <= 0 || n <= 0 {
		return
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	for b.used > 0 && b.used+n > b.maxBytes {
		b.cond.Wait()
	}

	b.used += n
}

// release returns n bytes to the budget and wakes any waiters.
func (b *DecodeBudget) release(n int64) {
	if b == nil || b.maxBytes <= 0 || n <= 0 {
		return
	}

	b.mu.Lock()

	b.used -= n
	if b.used < 0 {
		b.used = 0
	}

	b.cond.Broadcast()
	b.mu.Unlock()
}

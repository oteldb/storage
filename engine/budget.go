package engine

import (
	"context"
	"sync"
)

// queryScope is one logical query's hold on a [DecodeBudget]. Fetches opened under the same scope
// share a single admission decision: the first blocks for its estimate as usual, and any later one
// is admitted without blocking.
//
// That exception is what keeps the budget deadlock-free once a caller keeps more than one fetch
// open at a time. Blocking on the second acquire would be hold-and-wait against itself — the
// waiter is the only one who could release, and [DecodeBudget.acquire]'s admit-alone escape does
// not fire, because `used` is non-zero precisely because this query holds it. The accounting stays
// exact; only the queueing is skipped, so a query with several fetches open can overshoot the
// ceiling by its own later estimates, exactly as a single over-budget fetch already may.
type queryScope struct {
	// mu serializes the scope's own admission so two concurrent fetches cannot both observe
	// held == 0 and both take the blocking path. It is held across that blocking acquire, which
	// only ever makes one query's fetches wait for its own first admission.
	mu   sync.Mutex
	held int64
}

type queryScopeKey struct{}

// WithQueryScope marks ctx as one logical query for decode-budget admission. Every [Engine.Fetch]
// (and aggregate read) made under the returned context — including several kept open at once —
// shares one admission decision, so a query that streams from more than one fetch cannot deadlock
// against itself.
//
// An embedder whose reads are one fetch each does not need it; the unscoped path is unchanged.
func WithQueryScope(ctx context.Context) context.Context {
	if ctx.Value(queryScopeKey{}) != nil {
		return ctx // Already scoped; nesting must not split the reservation.
	}

	return context.WithValue(ctx, queryScopeKey{}, &queryScope{})
}

// queryScopeFrom returns ctx's scope, or nil when the caller did not open one.
func queryScopeFrom(ctx context.Context) *queryScope {
	s, _ := ctx.Value(queryScopeKey{}).(*queryScope)

	return s
}

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

// acquireFor is [DecodeBudget.acquire] within a [WithQueryScope] context: the scope's first fetch
// blocks as usual, and while it still holds the reservation any further fetch is charged without
// blocking. Without a scope it is exactly acquire. It returns the scope so the matching release
// can find it.
func (b *DecodeBudget) acquireFor(ctx context.Context, n int64) *queryScope {
	if b == nil || b.maxBytes <= 0 || n <= 0 {
		return nil
	}

	s := queryScopeFrom(ctx)
	if s == nil {
		b.acquire(n)

		return nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.held > 0 {
		// This query is already admitted; charge without queueing. See [queryScope].
		b.mu.Lock()
		b.used += n
		b.mu.Unlock()
	} else {
		b.acquire(n)
	}

	s.held += n

	return s
}

// releaseFor returns n bytes charged through [DecodeBudget.acquireFor]. A nil scope is the
// unscoped path.
func (b *DecodeBudget) releaseFor(s *queryScope, n int64) {
	if s != nil {
		s.mu.Lock()
		s.held -= n
		if s.held < 0 {
			s.held = 0
		}
		s.mu.Unlock()
	}

	b.release(n)
}

// inFlight reports the currently reserved bytes. It is for tests and introspection.
func (b *DecodeBudget) inFlight() int64 {
	if b == nil {
		return 0
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	return b.used
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

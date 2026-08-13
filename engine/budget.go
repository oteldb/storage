package engine

import (
	"context"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-faster/errors"

	"github.com/oteldb/storage/query/fetch"
)

// DefaultDecodeBudgetForceAfter is how long a query waits for decode budget before it is admitted
// anyway. See [DecodeBudget].
const DefaultDecodeBudgetForceAfter = 30 * time.Second

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
// The ceiling is soft in two more ways, both liveness guards for a caller that holds several reads
// open at once (which the library cannot detect and must not hang on):
//
//   - the wait is cancellable — a done context aborts it with an error and no reservation, so a
//     query deadline or a client disconnect always recovers the goroutine;
//   - the wait is bounded — a waiter that sits for [DefaultDecodeBudgetForceAfter] without the
//     budget draining at all is admitted regardless, counted and logged. Trading an RSS overshoot
//     for liveness is the same trade the admit-alone rule already makes.
//
// One budget may be shared by multiple engines (via [Config.DecodeBudget]) so the cap bounds the
// process-wide decode footprint rather than a per-engine (per-tenant) one.
type DecodeBudget struct {
	maxBytes int64
	// forceAfter is the no-progress interval after which a waiter is force-admitted.
	forceAfter time.Duration
	forced     atomic.Int64

	mu   sync.Mutex
	used int64
	// waiters is the FIFO admission queue. Admission is a hand-off performed by the releaser
	// (wakeLocked reserves for the waiter before waking it) rather than a re-check by the woken
	// goroutine: with a cancellable wait, a woken-but-canceled waiter would otherwise have to hand
	// its turn back, and a barging arrival could starve a large query indefinitely. FIFO also makes
	// the queue truthful — the head is always the query that has waited longest.
	waiters []*budgetWaiter
	// releases counts completed releases. A waiter samples it to tell "the budget is draining, I am
	// merely behind others" (reset the force timer) from "nothing has moved at all" (force admit).
	releases uint64
}

// budgetWaiter is one queued acquire. ready is closed by the goroutine that reserves n on its
// behalf; admitted (guarded by DecodeBudget.mu) is the authoritative flag, since a wait can lose the
// race between admission and cancellation.
type budgetWaiter struct {
	n        int64
	admitted bool
	ready    chan struct{}
}

// NewDecodeBudget returns a budget capping in-flight decoded bytes at maxBytes. maxBytes ≤ 0 disables
// it (every acquire/release is a no-op).
func NewDecodeBudget(maxBytes int64) *DecodeBudget {
	return &DecodeBudget{maxBytes: maxBytes, forceAfter: DefaultDecodeBudgetForceAfter}
}

// acquire blocks until n bytes of budget are free, then reserves them. It admits the request
// immediately when nothing else is in flight (even if n exceeds the whole budget), so an
// over-budget query runs alone rather than waiting forever. A nil/disabled budget or n ≤ 0 is a
// no-op.
//
// It returns forced when the wait was cut short for liveness (see [DecodeBudget]); on error nothing
// is reserved.
func (b *DecodeBudget) acquire(ctx context.Context, n int64) (forced bool, _ error) {
	if b == nil || b.maxBytes <= 0 || n <= 0 {
		return false, nil
	}

	b.mu.Lock()

	// Queue behind existing waiters even when n would fit: barging past them is what turns a large
	// query into a starved one.
	if len(b.waiters) == 0 && b.fitsLocked(n) {
		b.used += n
		b.mu.Unlock()

		return false, nil
	}

	// This queue is deliberately not golang.org/x/sync/semaphore.Weighted, whose Acquire is exactly
	// this shape (context-aware, weighted, FIFO hand-off). Weighted enforces a *hard* cap: nothing
	// can push `cur` past `size`, and Release panics past zero. This budget's cap is soft by design
	// and has to be exceeded on three paths — a query bigger than the whole budget admitted alone, a
	// scoped query's later reads charged without queueing, and a stalled waiter force-admitted — so a
	// semaphore could only be the waiting half, with a shadow counter holding the real in-flight
	// bytes beside it. That splits the accounting in two (they disagree exactly when the ceiling is
	// exceeded, which is when the number matters), makes releases need a per-reservation "how much of
	// this was semaphore-held" token the release site does not carry, and still leaves the head/drain
	// state the force rule reads unexposed. One counter with the escapes built in is less to own.
	w := &budgetWaiter{n: n, ready: make(chan struct{})}
	b.waiters = append(b.waiters, w)
	seen := b.releases
	b.mu.Unlock()

	t := time.NewTimer(b.forceAfter)
	defer t.Stop()

	for {
		select {
		case <-w.ready:
			return false, nil
		case <-ctx.Done():
			return false, b.abandon(ctx, w)
		case <-t.C:
			admitted, progressed := b.forceAdmit(w, &seen)
			if admitted {
				return false, nil
			}

			if !progressed {
				return true, nil
			}

			t.Reset(b.forceAfter)
		}
	}
}

// fitsLocked reports whether n can be reserved right now: it fits under the cap, or nothing is in
// flight (the admit-alone escape for a query bigger than the whole budget).
func (b *DecodeBudget) fitsLocked(n int64) bool {
	return b.used == 0 || b.used+n <= b.maxBytes
}

// abandon drops w from the queue and returns why ctx ended. If w was admitted in the instant before
// the context did, its reservation is returned to the budget instead of leaking.
func (b *DecodeBudget) abandon(ctx context.Context, w *budgetWaiter) error {
	b.mu.Lock()

	if w.admitted {
		b.releaseLocked(w.n)
	} else {
		b.removeLocked(w)
	}

	b.mu.Unlock()

	cause := ctx.Err()
	if cause == nil {
		cause = context.Canceled
	}

	return errors.Wrap(cause, "acquire decode budget")
}

// forceAdmit resolves a fired force timer. It reports whether w was admitted normally in the
// meantime, and whether waiting another interval is warranted instead of overshooting the ceiling —
// which it is only while w is behind other waiters *and* the budget is draining, i.e. the queue is
// demonstrably moving and w's turn is coming. The head waiter is never reset: if the longest wait in
// the queue has not been satisfied within the interval, no amount of churn elsewhere will satisfy
// it, and that is precisely the wedge to break.
func (b *DecodeBudget) forceAdmit(w *budgetWaiter, seen *uint64) (admitted, progressed bool) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if w.admitted {
		return true, false
	}

	if b.releases != *seen && len(b.waiters) > 0 && b.waiters[0] != w {
		*seen = b.releases

		return false, true
	}

	b.removeLocked(w)

	w.admitted = true
	b.used += w.n
	b.forced.Add(1)

	return false, false
}

func (b *DecodeBudget) removeLocked(w *budgetWaiter) {
	if i := slices.Index(b.waiters, w); i >= 0 {
		b.waiters = slices.Delete(b.waiters, i, i+1)
	}
}

// wakeLocked hands the freed budget to the longest-waiting queries, reserving on their behalf
// before waking them.
func (b *DecodeBudget) wakeLocked() {
	for len(b.waiters) > 0 {
		w := b.waiters[0]
		if !b.fitsLocked(w.n) {
			return
		}

		b.waiters = slices.Delete(b.waiters, 0, 1)
		w.admitted = true
		b.used += w.n
		close(w.ready)
	}
}

// acquireFor is [DecodeBudget.acquire] for a read carrying a [fetch.Scope]: the scope's first
// read blocks as usual, and while it still holds a reservation any later read is charged without
// queueing. A nil scope is exactly acquire.
//
// Skipping the queue is what keeps the budget deadlock-free for a caller that holds several reads
// open at once: blocking there would be hold-and-wait against the query's own reservation, and
// acquire's admit-alone escape cannot fire because `used` is non-zero precisely because this
// query holds it. The accounting stays exact — only the waiting is skipped — so such a query may
// overshoot the ceiling by its own later estimates, the latitude a single over-budget read has.
func (b *DecodeBudget) acquireFor(ctx context.Context, s *fetch.Scope, n int64) (forced bool, _ error) {
	if b == nil || b.maxBytes <= 0 || n <= 0 {
		return false, nil
	}

	if !s.Enter() {
		forced, err := b.acquire(ctx, n)
		if err != nil {
			s.Abort()

			return false, err
		}

		s.Charge(n)

		return forced, nil
	}

	b.mu.Lock()
	b.used += n
	b.mu.Unlock()

	s.Charge(n)

	return false, nil
}

// releaseFor returns n bytes charged through [DecodeBudget.acquireFor].
func (b *DecodeBudget) releaseFor(s *fetch.Scope, n int64) {
	s.Release(n)
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

// waiting reports how many acquires are queued. It is for tests and introspection.
func (b *DecodeBudget) waiting() int {
	if b == nil {
		return 0
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	return len(b.waiters)
}

// forcedAdmissions reports how many waits were cut short for liveness. It is for tests and
// introspection; the operator-facing counter is the obs instrument the engine records.
func (b *DecodeBudget) forcedAdmissions() int64 {
	if b == nil {
		return 0
	}

	return b.forced.Load()
}

// release returns n bytes to the budget and admits whoever the freed bytes now fit.
func (b *DecodeBudget) release(n int64) {
	if b == nil || b.maxBytes <= 0 || n <= 0 {
		return
	}

	b.mu.Lock()
	b.releaseLocked(n)
	b.mu.Unlock()
}

func (b *DecodeBudget) releaseLocked(n int64) {
	b.used -= n
	if b.used < 0 {
		b.used = 0
	}

	b.releases++
	b.wakeLocked()
}

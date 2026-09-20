package memlimit

import (
	"context"
	"slices"
	"sync"
)

// Pool admits work by the bytes it intends to hold, so a budget divided across concurrent merges is
// one they actually have.
//
// Without it the per-merge share is a promise nobody keeps: the facade fans merges out on its own
// schedule, every one of them sizes itself against "the budget divided by the concurrency", and
// nothing stops more than that many from running. The division then understates what a merge may
// hold *and* overstates how many may hold it — wrong in both directions at once.
//
// A zero or negative total, and the nil Pool, admit everything: an embedder that opts out of the
// memory bound gets no gate rather than a gate of size zero.
type Pool struct {
	total int64

	mu      sync.Mutex
	free    int64
	waiters []*waiter
}

// waiter is one blocked Acquire. granted is set under the pool's lock before ready is closed, so a
// caller that loses the race to its own context cancellation can tell that it owns bytes and must
// hand them back.
type waiter struct {
	n       int64
	ready   chan struct{}
	granted bool
}

// NewPool returns a pool admitting total bytes at once.
func NewPool(total int64) *Pool {
	if total <= 0 {
		return nil
	}

	return &Pool{total: total, free: total}
}

// Total reports the pool's size, 0 for a pool that admits everything.
func (p *Pool) Total() int64 {
	if p == nil {
		return 0
	}

	return p.total
}

// Waiting reports how many callers are queued for bytes. It exists for tests that need to race the
// grant itself rather than the waiter's arrival, and for an operator counter should one be wanted.
func (p *Pool) Waiting() int {
	if p == nil {
		return 0
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	return len(p.waiters)
}

// Acquire blocks until n bytes are free, then returns the function that hands them back. The
// release is idempotent, so `defer release()` is the correct use.
//
// A request larger than the whole pool is clamped to it: such a merge runs alone rather than never,
// because a bound that can deadlock its own work is worse than one that is occasionally exceeded.
//
// Waiters are served strictly in arrival order, so a queued large request delays smaller ones behind
// it rather than being starved by them. Requests are near-uniform — each caller asks for its share of
// one budget — but not identical: the share is divided by a concurrency that rises as engines appear,
// so a waiter queued when the process held one engine asks for more than one queued later.
func (p *Pool) Acquire(ctx context.Context, n int64) (release func(), err error) {
	if p == nil || p.total <= 0 {
		return func() {}, nil
	}

	n = min(max(n, 1), p.total)

	p.mu.Lock()

	// Jumping the queue when it is empty is the fast path, not a fairness exception: with no waiters
	// there is no one to jump.
	if len(p.waiters) == 0 && p.free >= n {
		p.free -= n
		p.mu.Unlock()

		return p.releaser(n), nil
	}

	w := &waiter{n: n, ready: make(chan struct{})}
	p.waiters = append(p.waiters, w)
	p.mu.Unlock()

	select {
	case <-w.ready:
		return p.releaser(n), nil
	case <-ctx.Done():
		p.abandon(w)

		return nil, ctx.Err()
	}
}

// TryAcquire takes n bytes if they are free right now, reporting whether it got them. It never
// queues, so a caller on a shared goroutine — the maintenance loop, which also services flush
// pressure — can decline the work and come back next cycle instead of parking everything behind it.
func (p *Pool) TryAcquire(n int64) (release func(), ok bool) {
	if p == nil || p.total <= 0 {
		return func() {}, true
	}

	n = min(max(n, 1), p.total)

	p.mu.Lock()
	defer p.mu.Unlock()

	// Yielding to a queue that is already waiting keeps a blocked operator merge from being starved
	// by background ones that keep arriving.
	if len(p.waiters) > 0 || p.free < n {
		return nil, false
	}

	p.free -= n

	return p.releaser(n), true
}

// releaser returns the idempotent hand-back for n bytes.
func (p *Pool) releaser(n int64) func() {
	var once sync.Once

	return func() { once.Do(func() { p.put(n) }) }
}

// put returns n bytes and hands them on to whoever is waiting.
func (p *Pool) put(n int64) {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.free += n
	p.grantLocked()
}

// grantLocked serves the queue head while it fits. It stops at the first waiter that does not,
// rather than skipping past it, so arrival order is what decides.
func (p *Pool) grantLocked() {
	for len(p.waiters) > 0 && p.free >= p.waiters[0].n {
		w := p.waiters[0]
		p.waiters = p.waiters[1:]
		p.free -= w.n
		w.granted = true
		close(w.ready)
	}
}

// abandon drops a waiter whose caller gave up. The grant races the cancellation, so a waiter that
// was served in the meantime owns its bytes and must return them.
func (p *Pool) abandon(w *waiter) {
	p.mu.Lock()

	if w.granted {
		p.free += w.n
		p.grantLocked()
		p.mu.Unlock()

		return
	}

	if i := slices.Index(p.waiters, w); i >= 0 {
		p.waiters = slices.Delete(p.waiters, i, i+1)

		// The waiter that left may have been the head that did not fit, holding back one behind it
		// that does. Nothing else will look: a grant only happens on release, and no bytes moved.
		p.grantLocked()
	}

	p.mu.Unlock()
}

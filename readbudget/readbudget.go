// Package readbudget bounds how much memory one query may hold at once, so an unselective read
// fails loudly instead of taking the process down.
//
// The bound is charged where a read can still be refused — before the memory is committed — which in
// practice means at the producer, not around it. The record engine reads every part, accumulates
// every survivor and materializes every output batch before it returns an iterator at all, so
// anything wrapping that iterator sees only bytes that are already resident. It therefore admits
// against an estimate of its decoded footprint taken from part metadata, mirroring what the metric
// engine's decode budget does with [engine.DecodeBudget].
//
// Unlike that budget, this one *rejects*. Decode admission queues concurrent queries behind a shared
// ceiling but admits an over-budget query alone rather than refusing it, so it bounds queries against
// each other and never any one of them against the process.
package readbudget

import (
	"context"
	"sync"

	"github.com/go-faster/errors"

	"github.com/oteldb/storage/internal/memlimit"
)

// ErrExceeded is returned by [Budget.Reserve] when a query has asked for more memory than it is
// allowed to hold. It is load shedding, not a storage fault: the data is intact and a narrower
// window or a more selective matcher will answer, so an embedder should map it to a 4xx rather than
// a 5xx.
var ErrExceeded = errors.New("readbudget: query exceeds its memory budget")

// Budget is one query's allowance. Create one per logical query, carry it on the query's context
// with [With], and let every layer that materializes bytes charge against it.
//
// Reservations are held until released, so the bound is on what the query holds *at once*, not on
// what it has read in total. A consumer that streams and releases as it goes is charged only for
// what is in flight; one that accumulates is charged for everything it accumulates. That falls out
// of the same accounting rather than needing to know which kind of consumer it is talking to.
//
// A Budget is safe for concurrent use: a fan-out charges from several goroutines at once.
type Budget struct {
	mu    sync.Mutex
	limit int64
	held  int64
	peak  int64
}

// New returns a budget of limit bytes. A limit ≤ 0 returns nil, which every method accepts as
// "unbounded" — so an embedder that bounds reads itself pays nothing, and no caller needs a nil
// check.
func New(limit int64) *Budget {
	if limit <= 0 {
		return nil
	}

	return &Budget{limit: limit}
}

// Reserve charges n bytes, or returns [ErrExceeded] having charged nothing.
func (b *Budget) Reserve(n int64) error {
	if b == nil || n <= 0 {
		return nil
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	if b.held+n > b.limit {
		return errors.Wrapf(ErrExceeded, "reserve %d bytes: holding %d of %d", n, b.held, b.limit)
	}

	b.held += n
	b.peak = max(b.peak, b.held)

	return nil
}

// Release returns n reserved bytes. Releasing more than is held is clamped rather than panicking: a
// double release is a bug, but one that must not take the process down on a read path.
func (b *Budget) Release(n int64) {
	if b == nil || n <= 0 {
		return
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	b.held = max(b.held-n, 0)
}

// Remaining reports how many bytes the query may still hold. It is what a fan-out sends to its peers
// so they can stop early rather than serializing a response the caller cannot accept.
func (b *Budget) Remaining() int64 {
	if b == nil {
		return 0
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	return max(b.limit-b.held, 0)
}

// Peak reports the most this query ever held at once, for diagnostics.
func (b *Budget) Peak() int64 {
	if b == nil {
		return 0
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	return b.peak
}

type budgetKey struct{}

// With returns ctx carrying b, so a layer that materializes bytes can find the budget without every
// intervening signature growing a parameter. Install it once, at the query boundary.
//
// The budget must not outlive its query: a leaked one keeps its reservations forever and every later
// read that inherits it eventually fails.
func With(ctx context.Context, b *Budget) context.Context {
	if b == nil {
		return ctx
	}

	return context.WithValue(ctx, budgetKey{}, b)
}

// From returns the budget installed by [With], or nil when the read is unbounded.
func From(ctx context.Context) *Budget {
	b, _ := ctx.Value(budgetKey{}).(*Budget)

	return b
}

// ProcessShare resolves a configured limit against the memory the process actually has, so a
// deployment that sets nothing still gets a bound that tracks the container it runs in rather than a
// constant that is right at exactly one deployment size.
//
// It is exported because a query-only node has no [Storage] to derive it from and must size its own
// budget the same way an embedded one does — one sizing rule, not two that drift.
//
// configured: 0 takes a share of the detected process budget, positive is taken as given, and
// negative opts out (0, meaning install no limiter).
func ProcessShare(configured int64) int64 { return memlimit.QueryShare(configured) }

type hintKey struct{}

// WithLimitHint returns ctx carrying an upper bound a *caller* has declared — the remaining
// allowance a cluster aggregator sent with its request.
//
// It is deliberately a hint and not a budget. A declared limit may only ever *lower* what this node
// grants a query (see [LimitHint] callers), never raise it: the value arrives over the network, so
// treating it as authoritative would let anyone able to reach the read endpoint hand themselves an
// unbounded query by declaring a huge allowance.
func WithLimitHint(ctx context.Context, n int64) context.Context {
	if n <= 0 {
		return ctx
	}

	return context.WithValue(ctx, hintKey{}, n)
}

// LimitHint returns the caller-declared upper bound, or 0 when none was declared. Callers must
// combine it with their own configured limit by taking the smaller of the two.
func LimitHint(ctx context.Context) int64 {
	n, _ := ctx.Value(hintKey{}).(int64)

	return n
}

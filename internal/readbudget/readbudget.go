// Package readbudget bounds how much memory one query may hold at once.
//
// It is denominated in **peak resident bytes**, and that is the whole point of having one package
// rather than a limit per layer. A query's bytes enter at several places — a part column read off
// disk, a fan-out response body off the network, a materialized batch — and each of those measures
// something different: compressed on-disk bytes, wire bytes, Go structs. Left as separate limits
// they would be three knobs where whichever is tightest binds and the other two are dead.
//
// So every charge point converts into the same unit before charging. Where part metadata gives a
// real estimate of the decoded footprint, that estimate is charged rather than the compressed size;
// only the network body, where the wire size is all that is known, has to approximate.
package readbudget

import (
	"context"
	"sync"

	"github.com/go-faster/errors"
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

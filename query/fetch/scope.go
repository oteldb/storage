package fetch

import (
	"context"
	"sync"
)

// Scope ties several reads to one logical query — a PromQL request and every selector, chunk and
// subquery under it — so admission control can tell "this query again" from "another query".
//
// It is the read-side analog of Prometheus' `Storage.Querier`: the caller creates one per
// request, passes it on every [Request] it issues, and drops it when the request ends. An
// embedder that cannot thread it through every [Request] can install one on the request context
// instead ([WithScope]), which a [Fetcher] uses when Request.Scope is nil.
//
// An embedder whose reads are one fetch each can leave it nil; the unscoped path is unchanged, and
// is no longer able to wedge: an unscoped read that hold-and-waits against its own reservation is
// released by its context, or force-admitted, rather than blocking forever.
//
// It exists because the decode budget is reserved per fetch and released when that fetch ends,
// which is deadlock-free only while a caller keeps one fetch open at a time. A streaming consumer
// holds several iterators across an evaluation, and its second acquire is then hold-and-wait
// against its own reservation — the waiter is the only one who could release it. With a Scope the
// first read blocks for admission as usual and later reads under the same scope are charged
// without queueing.
//
// A Scope is safe for concurrent use: an engine may evaluate two subtrees at once.
type Scope struct {
	mu sync.Mutex
	// held is the bytes this scope currently has reserved. Guarded by mu.
	held int64
}

// NewScope returns a scope for one logical query.
func NewScope() *Scope { return &Scope{} }

type scopeKey struct{}

// WithScope returns ctx carrying s, so reads made under it are admitted as one logical query even
// when the [Request] they travel on has no Scope. Install it once at the request boundary — the
// scope must not outlive the query, or every later read inherits its reservation and the decode
// budget stops bounding anything.
//
// A Request's own Scope always wins; this is the fallback for a call path that cannot thread one.
func WithScope(ctx context.Context, s *Scope) context.Context {
	return context.WithValue(ctx, scopeKey{}, s)
}

// ScopeFrom returns the scope installed by [WithScope], or nil.
func ScopeFrom(ctx context.Context) *Scope {
	s, _ := ctx.Value(scopeKey{}).(*Scope)

	return s
}

// Enter locks the scope and reports whether it already holds a reservation. The caller MUST end
// the section with [Scope.Charge] or [Scope.Abort].
//
// The lock is held across the caller's admission decision on purpose: it is what stops two
// concurrent reads in the same query from both observing "holds nothing" and both blocking. It
// only ever makes one query's reads wait for that query's own first admission.
func (s *Scope) Enter() (holding bool) {
	if s == nil {
		return false
	}

	s.mu.Lock()

	return s.held > 0
}

// Charge records n as reserved and unlocks the scope, ending the [Scope.Enter] section.
func (s *Scope) Charge(n int64) {
	if s == nil {
		return
	}

	s.held += n
	s.mu.Unlock()
}

// Abort ends the [Scope.Enter] section without charging.
func (s *Scope) Abort() {
	if s == nil {
		return
	}

	s.mu.Unlock()
}

// Release returns n reserved bytes to the scope.
func (s *Scope) Release(n int64) {
	if s == nil {
		return
	}

	s.mu.Lock()

	s.held -= n
	if s.held < 0 {
		s.held = 0
	}

	s.mu.Unlock()
}

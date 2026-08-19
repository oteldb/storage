// Package faultbackend wraps a [backend.Backend] so a test can make it misbehave: fail chosen
// operations, and suspend one operation until another reaches an agreed point.
//
// The second capability is the reason the package exists. A distributed-storage bug is usually an
// interleaving, not an error path, and reproducing one with sleeps yields a flaky test that proves
// nothing on a loaded CI machine. A [Gate] blocks the matching operation inside the backend until
// the test releases it, so the interleaving is stated in the test rather than raced for.
package faultbackend

import (
	"context"
	"sync"

	"github.com/oteldb/storage/backend"
)

// Kind is the backend operation a [Rule] matches.
type Kind int

// The backend operations a rule can match.
const (
	Read Kind = iota
	Write
	PutIfAbsent
	List
	Delete
)

// String implements [fmt.Stringer].
func (k Kind) String() string {
	switch k {
	case Read:
		return "read"
	case Write:
		return "write"
	case PutIfAbsent:
		return "put-if-absent"
	case List:
		return "list"
	case Delete:
		return "delete"
	default:
		return "unknown"
	}
}

// Op is a single backend operation offered to a [Rule].
type Op struct {
	Kind Kind
	Key  string
}

// Rule decides what happens to the operations it matches. A rule with no Match matches every
// operation of its Kind.
type Rule struct {
	Kind  Kind
	Match func(Op) bool
	// Err, when non-nil, is returned instead of performing the operation.
	Err error
	// Before, when non-nil, runs before the operation. It may block, which is what suspends the
	// calling goroutine inside the backend (see [Gate]).
	Before func(Op)
	// Times limits how many operations the rule applies to. Zero ⇒ unlimited.
	Times int

	fired int
}

// Backend is a [backend.Backend] that applies [Rule]s to the operations passing through it, and
// records them. The zero value is not usable; call [Wrap].
//
// It deliberately forwards none of the optional backend capabilities ([backend.Viewer],
// [backend.Sizer], ReaderAt, ObjectCreator): every one of them has a mandatory fallback, so a
// wrapped backend exercises the same code as an unwrapped one, only slower.
type Backend struct {
	backend.Backend

	mu    sync.Mutex
	rules []*Rule
	log   []Op
}

// Wrap returns b with fault injection attached.
func Wrap(b backend.Backend) *Backend { return &Backend{Backend: b} }

// Add installs a rule. Rules are consulted in the order they were added, and the first one to match
// an operation decides it.
func (b *Backend) Add(r Rule) *Backend {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.rules = append(b.rules, &r)

	return b
}

// Reset removes every rule, leaving the recorded operations in place.
func (b *Backend) Reset() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.rules = nil
}

// Ops returns the operations performed so far, in order.
func (b *Backend) Ops() []Op {
	b.mu.Lock()
	defer b.mu.Unlock()

	return append([]Op(nil), b.log...)
}

// Count returns how many recorded operations satisfy match.
func (b *Backend) Count(match func(Op) bool) int {
	var n int
	for _, op := range b.Ops() {
		if match(op) {
			n++
		}
	}

	return n
}

// Read implements [backend.Backend].
func (b *Backend) Read(ctx context.Context, key string) ([]byte, error) {
	if r := b.intercept(Op{Kind: Read, Key: key}); r != nil && r.Err != nil {
		return nil, r.Err
	}

	return b.Backend.Read(ctx, key)
}

// Write implements [backend.Backend].
func (b *Backend) Write(ctx context.Context, key string, data []byte) error {
	if r := b.intercept(Op{Kind: Write, Key: key}); r != nil && r.Err != nil {
		return r.Err
	}

	return b.Backend.Write(ctx, key, data)
}

// PutIfAbsent implements [backend.Backend].
func (b *Backend) PutIfAbsent(ctx context.Context, key string, data []byte) (bool, error) {
	if r := b.intercept(Op{Kind: PutIfAbsent, Key: key}); r != nil && r.Err != nil {
		return false, r.Err
	}

	return b.Backend.PutIfAbsent(ctx, key, data)
}

// List implements [backend.Backend].
func (b *Backend) List(ctx context.Context, prefix string) ([]string, error) {
	if r := b.intercept(Op{Kind: List, Key: prefix}); r != nil && r.Err != nil {
		return nil, r.Err
	}

	return b.Backend.List(ctx, prefix)
}

// Delete implements [backend.Backend].
func (b *Backend) Delete(ctx context.Context, key string) error {
	if r := b.intercept(Op{Kind: Delete, Key: key}); r != nil && r.Err != nil {
		return r.Err
	}

	return b.Backend.Delete(ctx, key)
}

// intercept records op and returns the rule governing it, if any. The rule's Before hook runs
// outside b.mu — it may block for an unbounded time, and other operations must keep flowing while
// it does, or a gate could only ever suspend a backend with no other traffic.
func (b *Backend) intercept(op Op) *Rule {
	b.mu.Lock()
	b.log = append(b.log, op)

	var hit *Rule
	for _, r := range b.rules {
		if r.Kind != op.Kind {
			continue
		}
		if r.Match != nil && !r.Match(op) {
			continue
		}
		if r.Times > 0 && r.fired >= r.Times {
			continue
		}
		r.fired++
		hit = r

		break
	}
	b.mu.Unlock()

	if hit != nil && hit.Before != nil {
		hit.Before(op)
	}

	return hit
}

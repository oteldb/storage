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
	CompareAndSwap
	ReadVersioned
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
	case CompareAndSwap:
		return "compare-and-swap"
	case ReadVersioned:
		return "read-versioned"
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
	// Bytes is the length of the data a Write, PutIfAbsent or CompareAndSwap stores; zero for the
	// other kinds.
	Bytes int
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
	// Replace, when non-nil, rewrites the bytes a read returns. It models the failure a returned
	// error cannot: a store that hands back data which is not what was written, and says nothing.
	// It applies to [Read] alone; the wrapper implements no [backend.Viewer], so every read of an
	// object's bytes — including one made through [backend.ReadView] — passes through it.
	Replace func(Op, []byte) []byte
	// Lose, when true, makes a [PutIfAbsent] or [CompareAndSwap] report that it lost the race
	// without error or storing anything: the endlessly contended key an error cannot model.
	Lose bool
	// After, when non-nil, runs once the operation succeeded — for a conditional write, only once it
	// landed — with the bytes it stored or, for a read, returned. It is where an invariant checker
	// sees every committed value.
	After func(Op, []byte)
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

// Bytes returns the summed [Op.Bytes] of the recorded operations that satisfy match.
func (b *Backend) Bytes(match func(Op) bool) int {
	var n int
	for _, op := range b.Ops() {
		if match(op) {
			n += op.Bytes
		}
	}

	return n
}

// Read implements [backend.Backend].
func (b *Backend) Read(ctx context.Context, key string) ([]byte, error) {
	op := Op{Kind: Read, Key: key}

	r := b.intercept(op)
	if r.fails() {
		return nil, r.Err
	}

	data, err := b.Backend.Read(ctx, key)
	if err != nil {
		return data, err
	}

	if r != nil && r.Replace != nil {
		data = r.Replace(op, data)
	}

	r.after(op, data)

	return data, nil
}

// Write implements [backend.Backend].
func (b *Backend) Write(ctx context.Context, key string, data []byte) error {
	op := Op{Kind: Write, Key: key, Bytes: len(data)}

	r := b.intercept(op)
	if r.fails() {
		return r.Err
	}

	if err := b.Backend.Write(ctx, key, data); err != nil {
		return err
	}

	r.after(op, data)

	return nil
}

// PutIfAbsent implements [backend.Backend].
func (b *Backend) PutIfAbsent(ctx context.Context, key string, data []byte) (bool, error) {
	op := Op{Kind: PutIfAbsent, Key: key, Bytes: len(data)}

	r := b.intercept(op)
	if r.fails() {
		return false, r.Err
	}

	if r.loses() {
		return false, nil
	}

	ok, err := b.Backend.PutIfAbsent(ctx, key, data)
	if err == nil && ok {
		r.after(op, data)
	}

	return ok, err
}

// CompareAndSwap implements [backend.Backend]. Gating it is how a test states the interleaving
// the commit protocol turns on: one writer suspended inside its conditional write while another
// commits over it.
func (b *Backend) CompareAndSwap(
	ctx context.Context, key string, expected backend.Version, data []byte,
) (backend.Version, bool, error) {
	op := Op{Kind: CompareAndSwap, Key: key, Bytes: len(data)}

	r := b.intercept(op)
	if r.fails() {
		return backend.VersionAbsent, false, r.Err
	}

	if r.loses() {
		return backend.VersionAbsent, false, nil
	}

	version, ok, err := b.Backend.CompareAndSwap(ctx, key, expected, data)
	if err == nil && ok {
		r.after(op, data)
	}

	return version, ok, err
}

// ReadVersioned implements [backend.Backend].
func (b *Backend) ReadVersioned(ctx context.Context, key string) ([]byte, backend.Version, error) {
	op := Op{Kind: ReadVersioned, Key: key}

	r := b.intercept(op)
	if r.fails() {
		return nil, backend.VersionAbsent, r.Err
	}

	data, version, err := b.Backend.ReadVersioned(ctx, key)
	if err != nil {
		return data, version, err
	}

	r.after(op, data)

	return data, version, nil
}

// List implements [backend.Backend].
func (b *Backend) List(ctx context.Context, prefix string) ([]string, error) {
	op := Op{Kind: List, Key: prefix}

	r := b.intercept(op)
	if r.fails() {
		return nil, r.Err
	}

	keys, err := b.Backend.List(ctx, prefix)
	if err != nil {
		return keys, err
	}

	r.after(op, nil)

	return keys, nil
}

// Delete implements [backend.Backend].
func (b *Backend) Delete(ctx context.Context, key string) error {
	op := Op{Kind: Delete, Key: key}

	r := b.intercept(op)
	if r.fails() {
		return r.Err
	}

	if err := b.Backend.Delete(ctx, key); err != nil {
		return err
	}

	r.after(op, nil)

	return nil
}

func (r *Rule) fails() bool { return r != nil && r.Err != nil }

func (r *Rule) loses() bool { return r != nil && r.Lose }

func (r *Rule) after(op Op, data []byte) {
	if r != nil && r.After != nil {
		r.After(op, data)
	}
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

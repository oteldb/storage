// Package readerpool retains immutable object handles within a bounded process-wide budget.
package readerpool

import (
	"container/list"
	"context"
	"io"
	"io/fs"
	"math"
	"runtime"
	"slices"
	"sync"
	"sync/atomic"
	"syscall"

	"github.com/go-faster/errors"
)

// Reader is an opened object with independent ranged reads.
type Reader interface {
	io.Reader
	io.ReaderAt
	io.Seeker
	io.Closer
	Size() int64
}

type scopeKey struct{}
type binding struct {
	scope      *Scope
	generation uint64
}

var errGenerationChanged = errors.New("immutable object changed during read")

// Scope owns immutable generations and protects idle handles from ordinary LRU eviction.
type Scope struct {
	id         uint64
	active     *atomic.Int64
	generation *atomic.Uint64
	mutating   *atomic.Int64
	memory     *atomic.Int64
	closed     atomic.Bool
	cleanup    runtime.Cleanup
}

var scopeIDs atomic.Uint64
var shared = newPool(256)

// NewScope creates an immutable object owner, initially unpinned.
func NewScope() *Scope {
	s := &Scope{id: scopeIDs.Add(1), active: new(atomic.Int64), generation: new(atomic.Uint64),
		mutating: new(atomic.Int64), memory: new(atomic.Int64)}
	s.generation.Store(s.id)
	s.cleanup = runtime.AddCleanup(s, func(id uint64) { _ = shared.closeScope(id) }, s.id)
	return s
}

// Context attaches this immutable object owner to backend reads.
func (s *Scope) Context(ctx context.Context) context.Context {
	if b, ok := ctx.Value(scopeKey{}).(binding); ok && b.scope == s {
		return ctx
	}
	return context.WithValue(ctx, scopeKey{}, binding{scope: s, generation: s.generation.Load()})
}

// Acquire protects idle handles from ordinary LRU eviction until Release.
func (s *Scope) Acquire() { s.active.Add(1) }

// Release ends one pin.
func (s *Scope) Release() { s.active.Add(-1) }

// Close releases idle handles; outstanding leases keep their handles until closed.
func (s *Scope) Close() error {
	if !s.closed.CompareAndSwap(false, true) {
		return nil
	}
	s.cleanup.Stop()
	return shared.closeScope(s.id)
}

// Scoped reports whether a read belongs to an immutable object owner.
func Scoped(ctx context.Context) bool { return ctx.Value(scopeKey{}) != nil }

// Open reuses one immutable raw reader; namespace must be comparable. A full pinned
// budget declines reuse so the caller can use its ordinary ranged-read fallback.
func Open(ctx context.Context, namespace any, name string, open func() (Reader, error)) (Reader, bool, error) {
	binding, scoped := ctx.Value(scopeKey{}).(binding)
	if !scoped {
		r, err := open()
		if RelievePressure(err) {
			r, err = open()
		}
		return r, true, err
	}
	return shared.acquireGeneration(binding.scope, binding.generation, namespace, name, open)
}

type key struct {
	scope      uint64
	generation uint64
	namespace  any
	name       string
}
type entry struct {
	key       key
	reader    Reader
	refs      int
	evicted   bool
	pinned    *atomic.Int64
	element   *list.Element
	memory    *atomic.Int64
	heldBytes int64
}

type pool struct {
	mu        sync.Mutex
	limit     int
	opened    int
	entries   map[key]*entry
	lru       list.List
	owners    map[objectKey]map[uint64]ownerState
	owned     map[uint64]map[objectKey]struct{}
	mutations map[objectKey]int
}

type objectKey struct {
	namespace any
	name      string
}
type ownerState struct {
	active     *atomic.Int64
	generation *atomic.Uint64
	mutating   *atomic.Int64
	memory     *atomic.Int64
}

func newPool(limit int) *pool {
	return &pool{limit: limit, entries: make(map[key]*entry), owners: make(map[objectKey]map[uint64]ownerState),
		owned: make(map[uint64]map[objectKey]struct{}), mutations: make(map[objectKey]int)}
}

func (p *pool) acquire(s *Scope, namespace any, name string, open func() (Reader, error)) (Reader, bool, error) {
	return p.acquireGeneration(s, s.generation.Load(), namespace, name, open)
}

func (p *pool) acquireGeneration(
	s *Scope, generation uint64, namespace any, name string, open func() (Reader, error),
) (Reader, bool, error) {
	e, supported, err := p.borrowGeneration(s, generation, namespace, name, open)
	if err != nil || !supported {
		return nil, supported, err
	}
	return &lease{owner: p, entry: e}, true, nil
}

func (p *pool) borrowGeneration(
	s *Scope, generation uint64, namespace any, name string, open func() (Reader, error),
) (*entry, bool, error) {
	for {
		p.mu.Lock()
		if s.closed.Load() {
			p.mu.Unlock()
			return nil, false, nil
		}
		object := objectKey{namespace, name}
		k := key{scope: s.id, generation: generation, namespace: namespace, name: name}
		if e := p.entries[k]; e != nil {
			if e.reader == nil {
				p.mu.Unlock()
				return nil, false, nil
			}
			e.refs++
			p.lru.MoveToFront(e.element)
			p.mu.Unlock()
			return e, true, nil
		}
		p.registerOwner(s, object)
		if generation != s.generation.Load() || s.mutating.Load() != 0 || p.mutations[object] != 0 {
			p.mu.Unlock()
			return nil, true, errGenerationChanged
		}
		if p.opened >= p.limit {
			var victim *entry
			for element := p.lru.Back(); element != nil; element = element.Prev() {
				e := element.Value.(*entry)
				if e.refs == 0 && e.pinned.Load() == 0 {
					victim = e
					break
				}
			}
			if victim == nil {
				p.mu.Unlock()
				return nil, false, nil
			}
			p.detach(victim)
			p.mu.Unlock()
			_ = p.closeReader(victim)
			continue
		}
		e := &entry{key: k, refs: 1, pinned: s.active, memory: s.memory, heldBytes: 512}
		s.memory.Add(e.heldBytes)
		e.element = p.lru.PushFront(e)
		p.entries[k] = e
		p.opened++
		p.mu.Unlock()
		reader, err := open()
		if descriptorPressure(err) && p.reclaimIdle(32) > 0 {
			reader, err = open()
		}
		return p.publishOpen(s, generation, e, reader, err)
	}
}

func (p *pool) registerOwner(s *Scope, object objectKey) {
	if p.owners[object] == nil {
		p.owners[object] = make(map[uint64]ownerState)
	}
	if _, known := p.owners[object][s.id]; !known {
		s.memory.Add(registrationBytes(object))
	}
	p.owners[object][s.id] = ownerState{s.active, s.generation, s.mutating, s.memory}
	if p.owned[s.id] == nil {
		p.owned[s.id] = make(map[objectKey]struct{})
	}
	p.owned[s.id][object] = struct{}{}
}

func (p *pool) publishOpen(s *Scope, generation uint64, e *entry, reader Reader, err error) (*entry, bool, error) {
	p.mu.Lock()
	if err != nil {
		if !e.evicted {
			p.detach(e)
		}
		p.opened--
		e.memory.Add(-e.heldBytes)
		p.mu.Unlock()
		return nil, true, err
	}
	e.reader = reader
	readerBytes := int64(1024)
	if priced, ok := reader.(interface{ MemoryBytes() int64 }); ok {
		readerBytes = priced.MemoryBytes()
	}
	e.heldBytes += readerBytes
	e.memory.Add(readerBytes)
	changed := generation != s.generation.Load() || s.mutating.Load() != 0
	if changed && !e.evicted {
		p.detach(e)
	}
	p.mu.Unlock()
	if changed {
		_ = p.release(e)
		return nil, true, errGenerationChanged
	}
	return e, true, nil
}

func descriptorPressure(err error) bool {
	return errors.Is(err, syscall.EMFILE) || errors.Is(err, syscall.ENFILE) || systemDescriptorPressure(err)
}

func (p *pool) reclaimIdle(limit int) int {
	p.mu.Lock()
	var closing []*entry
	for item := p.lru.Back(); item != nil && len(closing) < limit; {
		previous := item.Prev()
		e := item.Value.(*entry)
		state := p.owners[objectKey{e.key.namespace, e.key.name}][e.key.scope]
		current := state.generation != nil && e.key.generation == state.generation.Load()
		if e.refs == 0 && (e.pinned.Load() == 0 || current) {
			p.detach(e)
			closing = append(closing, e)
		}
		item = previous
	}
	p.mu.Unlock()
	for _, e := range closing {
		_ = p.closeReader(e)
	}
	return len(closing)
}

func (p *pool) detach(e *entry) {
	delete(p.entries, e.key)
	p.lru.Remove(e.element)
	e.evicted = true
}

func (p *pool) closeReader(e *entry) error {
	err := e.reader.Close()
	e.memory.Add(-e.heldBytes)
	p.mu.Lock()
	p.opened--
	p.mu.Unlock()
	return err
}

func (p *pool) closeScope(id uint64) error {
	p.mu.Lock()
	var closing []*entry
	for k, e := range p.entries {
		if k.scope == id {
			p.detach(e)
			if e.refs == 0 {
				closing = append(closing, e)
			}
		}
	}
	for object := range p.owned[id] {
		state := p.owners[object][id]
		state.memory.Add(-registrationBytes(object))
		delete(p.owners[object], id)
		if len(p.owners[object]) == 0 {
			delete(p.owners, object)
		}
	}
	delete(p.owned, id)
	p.mu.Unlock()
	var first error
	for _, e := range closing {
		if err := p.closeReader(e); err != nil && first == nil {
			first = err
		}
	}
	return first
}

type lease struct {
	owner  *pool
	entry  *entry
	pos    int64
	closed atomic.Bool
}

func (r *lease) Size() int64 { return r.entry.reader.Size() }
func (r *lease) ReadAt(dst []byte, off int64) (int, error) {
	if r.closed.Load() {
		return 0, fs.ErrClosed
	}
	return r.entry.reader.ReadAt(dst, off)
}
func (r *lease) Read(dst []byte) (int, error) {
	n, err := r.ReadAt(dst, r.pos)
	r.pos += int64(n)
	return n, err
}
func (r *lease) Seek(off int64, whence int) (int64, error) {
	if r.closed.Load() {
		return 0, fs.ErrClosed
	}
	var base int64
	switch whence {
	case io.SeekStart:
	case io.SeekCurrent:
		base = r.pos
	case io.SeekEnd:
		base = r.Size()
	default:
		return 0, fs.ErrInvalid
	}
	if off < -base || off > math.MaxInt64-base {
		return 0, fs.ErrInvalid
	}
	r.pos = base + off
	return r.pos, nil
}
func (r *lease) Close() error {
	if !r.closed.CompareAndSwap(false, true) {
		return nil
	}
	return r.owner.release(r.entry)
}

// Rebind preserves an immutable owner while replacing operation values and cancellation.
func Rebind(ctx, from context.Context) context.Context {
	if b, ok := from.Value(scopeKey{}).(binding); ok {
		return b.scope.Context(ctx)
	}
	return ctx
}

// ID identifies the operation's immutable generation without retaining its context.
func ID(ctx context.Context) uint64 {
	if b, ok := ctx.Value(scopeKey{}).(binding); ok {
		return b.generation
	}
	return 0
}

// Valid reports whether an operation can still open its generation.
func Valid(ctx context.Context) bool {
	b, ok := ctx.Value(scopeKey{}).(binding)
	return !ok || (b.generation == b.scope.generation.Load() && b.scope.mutating.Load() == 0)
}

func (p *pool) release(e *entry) error {
	p.mu.Lock()
	e.refs--
	closing := e.refs == 0 && e.evicted
	p.mu.Unlock()
	if closing {
		return p.closeReader(e)
	}
	return nil
}

// BeginMutation rotates scopes before publication; its callback rotates them again
// after publication, so new reads cannot retain an object opened mid-replacement.
func BeginMutation(namespace any, name string) func() { return shared.beginMutation(namespace, name) }

func (p *pool) beginMutation(namespace any, name string) func() {
	object := objectKey{namespace, name}
	p.mu.Lock()
	p.mutations[object]++
	states := make([]ownerState, 0, len(p.owners[object]))
	for _, state := range p.owners[object] {
		state.mutating.Add(1)
		state.generation.Store(scopeIDs.Add(1))
		states = append(states, state)
	}
	p.mu.Unlock()
	return func() {
		p.mu.Lock()
		for _, state := range states {
			state.generation.Store(scopeIDs.Add(1))
			state.mutating.Add(-1)
		}
		p.mutations[object]--
		if p.mutations[object] == 0 {
			delete(p.mutations, object)
		}
		p.mu.Unlock()
		if len(states) > 0 {
			p.pruneOld()
		}
	}
}

func (p *pool) pruneOld() {
	p.mu.Lock()
	var closing []*entry
	for _, e := range p.entries {
		if e.refs != 0 || e.pinned.Load() != 0 {
			continue
		}
		state := p.owners[objectKey{e.key.namespace, e.key.name}][e.key.scope]
		if state.generation != nil && e.key.generation != state.generation.Load() {
			p.detach(e)
			closing = append(closing, e)
		}
	}
	p.mu.Unlock()
	for _, e := range closing {
		_ = p.closeReader(e)
	}
}

// Append reads a clamped range into caller-owned storage without allocating a cursor lease.
func Append(ctx context.Context, namespace any, name string, dst []byte, off, n int64, open func() (Reader, error)) ([]byte, bool, error) {
	b, ok := ctx.Value(scopeKey{}).(binding)
	if !ok {
		return dst, false, nil
	}
	e, supported, err := shared.borrowGeneration(b.scope, b.generation, namespace, name, open)
	if err != nil || !supported {
		return dst, supported, err
	}
	defer func() { _ = shared.release(e) }()
	if off >= e.reader.Size() {
		return dst, true, nil
	}
	n = min(n, e.reader.Size()-off)
	if n > int64(math.MaxInt-len(dst)) {
		return dst, true, fs.ErrInvalid
	}
	start := len(dst)
	dst = slices.Grow(dst, int(n))[:start+int(n)]
	if _, err := e.reader.ReadAt(dst[start:], off); err != nil {
		return dst[:start], true, err
	}
	return dst, true, nil
}

// RelievePressure reclaims a bounded batch of idle current-generation handles.
func RelievePressure(err error) bool { return descriptorPressure(err) && shared.reclaimIdle(32) > 0 }

// ResidentBytes bounds scope and registered/retained reader bookkeeping.
func (s *Scope) ResidentBytes() int64 { return 512 + s.memory.Load() }

func registrationBytes(object objectKey) int64 { return 1024 + 4*int64(len(object.name)) }

// Supported reports whether a backend participates in scoped file invalidation.
func Supported(backend any) bool {
	p, ok := backend.(interface{ RetainsImmutableReaders() bool })
	return ok && p.RetainsImmutableReaders()
}

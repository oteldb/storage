package readerpool

import (
	"context"
	"io/fs"
	"sync"
)

const batchReaderLimit = 8

type batchKey struct{}

// Batch retains a bounded set of leases for one operation; reset it between parts.
// Reset and Close wait for reads using those leases.
type Batch struct {
	mu      sync.RWMutex
	drainMu sync.Mutex
	owner   *pool
	entries map[key]*entry
	closed  bool
}

// WithBatch lets scoped Append calls reuse leases until Reset or Close.
func WithBatch(ctx context.Context) (context.Context, *Batch) {
	b := newBatch(shared)
	return context.WithValue(ctx, batchKey{}, b), b
}

func newBatch(owner *pool) *Batch { return &Batch{owner: owner} }

// Reset releases the previous part's leases while retaining bounded bookkeeping.
func (b *Batch) Reset() error { return b.drain(false) }

// Close releases leases and refuses further reads through this batch.
func (b *Batch) Close() error { return b.drain(true) }

func (b *Batch) drain(closeBatch bool) error {
	b.drainMu.Lock()
	defer b.drainMu.Unlock()
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return nil
	}
	b.closed = closeBatch
	var held [batchReaderLimit]*entry
	count := 0
	for k, e := range b.entries {
		delete(b.entries, k)
		if e != nil {
			held[count] = e
			count++
		}
	}
	b.mu.Unlock()
	var first error
	for _, e := range held[:count] {
		if err := b.owner.release(e); err != nil && first == nil {
			first = err
		}
	}
	return first
}

func (b *Batch) append(scope binding, namespace any, name string, dst []byte, off, n int64,
	open func() (Reader, error),
) ([]byte, bool, error) {
	k := key{scope: scope.scope.id, generation: scope.generation, namespace: namespace, name: name}
	b.mu.RLock()
	if b.closed {
		b.mu.RUnlock()
		return dst, true, fs.ErrClosed
	}
	if e, known := b.entries[k]; known {
		data, supported, err := batchEntry(scope, e, dst, off, n)
		b.mu.RUnlock()
		return data, supported, err
	}
	b.mu.RUnlock()
	return b.appendCold(scope, k, dst, off, n, open)
}

func (b *Batch) appendCold(scope binding, k key, dst []byte, off, n int64, open func() (Reader, error)) ([]byte, bool, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return dst, true, fs.ErrClosed
	}
	if e, known := b.entries[k]; known {
		return batchEntry(scope, e, dst, off, n)
	}
	if len(b.entries) >= batchReaderLimit {
		return b.owner.appendGeneration(scope, k.namespace, k.name, dst, off, n, open)
	}
	e, supported, err := b.owner.borrowGeneration(scope.scope, scope.generation, k.namespace, k.name, open)
	if err != nil {
		return dst, supported, err
	}
	if b.entries == nil {
		b.entries = make(map[key]*entry, batchReaderLimit)
	}
	b.entries[k] = e
	if !supported {
		return dst, false, nil
	}
	return appendEntry(e, dst, off, n)
}

func batchEntry(scope binding, e *entry, dst []byte, off, n int64) ([]byte, bool, error) {
	if e != nil {
		return appendEntry(e, dst, off, n)
	}
	if scope.scope.closed.Load() {
		return dst, false, nil
	}
	if scope.generation != scope.scope.generation.Load() || scope.scope.mutating.Load() != 0 {
		return dst, true, errGenerationChanged
	}
	return dst, false, nil
}

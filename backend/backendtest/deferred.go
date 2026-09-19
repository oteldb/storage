package backendtest

import (
	"context"
	"slices"
	"strings"
	"sync"

	"github.com/oteldb/storage/backend"
)

// Deferred wraps a backend with a [backend.DeferredSyncer] that remembers which deferred writes
// and deletes no SyncPrefix has covered yet, so a test can assert that nothing durable names a
// prefix while it still has some. It also implements [backend.ObjectCreator], so a streaming writer
// takes its streaming path over a backend that buffers — while StreamsWrites still answers for
// that backend, which does not.
type Deferred struct {
	backend.Backend

	mu      sync.Mutex
	pending map[string]struct{}
	deletes []string
}

var (
	_ backend.DeferredSyncer = (*Deferred)(nil)
	_ backend.ObjectCreator  = (*Deferred)(nil)
)

// WithDeferred wraps b.
func WithDeferred(b backend.Backend) *Deferred {
	return &Deferred{Backend: b, pending: map[string]struct{}{}}
}

// Pending returns, sorted, the keys under prefix whose deferred operation is not yet synced.
func (d *Deferred) Pending(prefix string) []string {
	d.mu.Lock()
	defer d.mu.Unlock()

	var out []string

	for k := range d.pending {
		if strings.HasPrefix(k, prefix+"/") {
			out = append(out, k)
		}
	}

	slices.Sort(out)

	return out
}

// Deletes returns every delete in the order it happened, a deferred one marked with a "~" prefix.
func (d *Deferred) Deletes() []string {
	d.mu.Lock()
	defer d.mu.Unlock()

	return slices.Clone(d.deletes)
}

// Delete implements [backend.Backend].
func (d *Deferred) Delete(ctx context.Context, key string) error {
	d.record(key, "")

	return d.Backend.Delete(ctx, key)
}

// CreateObject implements [backend.ObjectCreator].
func (d *Deferred) CreateObject(ctx context.Context, key string) (backend.ObjectWriter, error) {
	return backend.CreateObject(ctx, d.Backend, key)
}

// StreamsWrites answers for the wrapped backend. Implements [backend.ObjectCreator].
func (d *Deferred) StreamsWrites() bool { return backend.StreamsWrites(d.Backend) }

// WriteDeferred implements [backend.DeferredSyncer].
func (d *Deferred) WriteDeferred(ctx context.Context, key string, data []byte) error {
	d.mark(key)

	return d.Write(ctx, key, data)
}

// CreateObjectDeferred implements [backend.DeferredSyncer].
func (d *Deferred) CreateObjectDeferred(ctx context.Context, key string) (backend.ObjectWriter, error) {
	d.mark(key)

	return backend.CreateObject(ctx, d.Backend, key)
}

// DeleteDeferred implements [backend.DeferredSyncer].
func (d *Deferred) DeleteDeferred(ctx context.Context, key string) error {
	d.mark(key)
	d.record(key, "~")

	return d.Backend.Delete(ctx, key)
}

// SyncPrefix implements [backend.DeferredSyncer].
func (d *Deferred) SyncPrefix(_ context.Context, prefix string) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	prefix = strings.TrimSuffix(prefix, "/")

	for k := range d.pending {
		if strings.HasPrefix(k, prefix+"/") {
			delete(d.pending, k)
		}
	}

	return nil
}

func (d *Deferred) mark(key string) {
	d.mu.Lock()
	defer d.mu.Unlock()

	d.pending[key] = struct{}{}
}

func (d *Deferred) record(key, tag string) {
	d.mu.Lock()
	defer d.mu.Unlock()

	d.deletes = append(d.deletes, tag+key)
}

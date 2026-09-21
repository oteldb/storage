package mergestreamtest

import (
	"context"
	"maps"
	"sync"

	"github.com/go-faster/errors"

	"github.com/oteldb/storage/backend"
)

// ErrInjected is the failure [Backend] injects into a chosen write.
var ErrInjected = errors.New("mergestreamtest: injected write failure")

// Backend is an in-memory [backend.Backend] that counts reads per key and can fail the n-th write.
// It embeds the interface rather than the concrete store, so it implements neither Viewer nor
// Sizer and every read a merge makes — including a size probe — funnels through Read and is seen.
type Backend struct {
	backend.Backend

	mu     sync.Mutex
	reads  map[string]int
	writes int
	failAt int
}

// NewBackend returns a Backend over [backend.Memory].
func NewBackend() *Backend {
	return &Backend{Backend: backend.Memory(), reads: map[string]int{}}
}

// FailWriteAt makes the n-th write from now on fail with [ErrInjected]; n ≤ 0 disables injection.
func (b *Backend) FailWriteAt(n int) {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.writes, b.failAt = 0, n
}

// Writes returns how many writes have happened since the last [Backend.FailWriteAt].
func (b *Backend) Writes() int {
	b.mu.Lock()
	defer b.mu.Unlock()

	return b.writes
}

// Reads returns a snapshot of the per-key read counts.
func (b *Backend) Reads() map[string]int {
	b.mu.Lock()
	defer b.mu.Unlock()

	return maps.Clone(b.reads)
}

// Read implements [backend.Backend].
func (b *Backend) Read(ctx context.Context, key string) ([]byte, error) {
	b.mu.Lock()
	b.reads[key]++
	b.mu.Unlock()

	return b.Backend.Read(ctx, key)
}

// Write implements [backend.Backend].
func (b *Backend) Write(ctx context.Context, key string, data []byte) error {
	if err := b.charge(); err != nil {
		return err
	}

	return b.Backend.Write(ctx, key, data)
}

func (b *Backend) charge() error {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.writes++
	if b.failAt > 0 && b.writes == b.failAt {
		return ErrInjected
	}

	return nil
}

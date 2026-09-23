package backendtest

import (
	"context"
	"maps"
	"sync"

	"github.com/go-faster/errors"

	"github.com/oteldb/storage/backend"
)

// ErrInjected is the failure [Counting] injects into a chosen write.
var ErrInjected = errors.New("backendtest: injected write failure")

// Counting counts reads per key and can fail the n-th write. It embeds the Backend interface, so it
// forwards no optional capability: every read, a view or a size probe included, funnels through Read
// and is counted.
type Counting struct {
	backend.Backend

	mu     sync.Mutex
	reads  map[string]int
	writes int
	failAt int
}

// NewCounting wraps b.
func NewCounting(b backend.Backend) *Counting {
	return &Counting{Backend: b, reads: map[string]int{}}
}

// Reads returns a snapshot of the per-key read counts.
func (c *Counting) Reads() map[string]int {
	c.mu.Lock()
	defer c.mu.Unlock()

	return maps.Clone(c.reads)
}

// FailWriteAt makes the n-th write from now on fail with [ErrInjected]; n ≤ 0 disables injection.
func (c *Counting) FailWriteAt(n int) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.writes, c.failAt = 0, n
}

// Writes returns how many writes have happened since the last [Counting.FailWriteAt].
func (c *Counting) Writes() int {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.writes
}

// Read implements [backend.Backend].
func (c *Counting) Read(ctx context.Context, key string) ([]byte, error) {
	c.mu.Lock()
	c.reads[key]++
	c.mu.Unlock()

	return c.Backend.Read(ctx, key)
}

// Write implements [backend.Backend].
func (c *Counting) Write(ctx context.Context, key string, data []byte) error {
	if err := c.charge(); err != nil {
		return err
	}

	return c.Backend.Write(ctx, key, data)
}

func (c *Counting) charge() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.writes++
	if c.failAt > 0 && c.writes == c.failAt {
		return ErrInjected
	}

	return nil
}

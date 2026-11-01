package engine

import (
	"context"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/query/fetch"
)

type memoryResetBackend struct {
	backend.Backend

	listed chan struct{}
	resume chan struct{}
	once   sync.Once
}

func (b *memoryResetBackend) List(ctx context.Context, prefix string) ([]string, error) {
	b.once.Do(func() { close(b.listed) })
	<-b.resume
	return b.Backend.List(ctx, prefix)
}

func TestMemoryResetExcludesReloadThroughSweep(t *testing.T) {
	t.Parallel()
	e, _ := newFaultWALEngine(t, 0)
	e.cfg.Backend, e.cfg.Prefix = backend.Memory(), "reset-catalog"
	enableMemoryPublicationTest(t, e)
	_, err := e.Append(apiSeries(), 10, 1)
	require.NoError(t, err)
	require.NoError(t, e.Flush(t.Context()))
	gated := &memoryResetBackend{Backend: e.cfg.Backend, listed: make(chan struct{}), resume: make(chan struct{})}
	e.cfg.Backend = gated
	release := sync.OnceFunc(func() { close(gated.resume) })
	t.Cleanup(release)
	reset := make(chan error, 1)
	go func() { reset <- e.Reset(t.Context()) }()
	<-gated.listed
	if e.mu.TryLock() {
		e.mu.Unlock()
		t.Fatal("reset released catalog ownership before deletion finished")
	}
	loaded := make(chan error, 1)
	go func() { loaded <- e.LoadPartsReadOnly(t.Context()) }()
	// Independent writes and readers remain admitted during the reset's backend I/O.
	_, err = e.Append(apiSeries(), 20, 2)
	require.NoError(t, err)
	count, err := e.Count(t.Context(), fetch.Request{Start: 0, End: 30})
	require.NoError(t, err)
	require.Equal(t, 1, count)
	release()
	require.NoError(t, <-reset)
	require.NoError(t, <-loaded)
}

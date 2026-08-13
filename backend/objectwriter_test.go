package backend_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/backend/backendtest"
)

// streamingMemory is [backend.Memory] plus the incremental-write capability, standing in for the
// file backend so the wrapper behavior can be tested without touching a disk.
type streamingMemory struct {
	backend.Backend

	creates int
}

func newStreamingMemory() *streamingMemory { return &streamingMemory{Backend: backend.Memory()} }

func (b *streamingMemory) CreateObject(_ context.Context, key string) (backend.ObjectWriter, error) {
	b.creates++

	return &memoryObjectWriter{b: b.Backend, key: key}, nil
}

type memoryObjectWriter struct {
	b   backend.Backend
	key string
	buf []byte
}

func (w *memoryObjectWriter) Write(p []byte) (int, error) {
	w.buf = append(w.buf, p...)

	return len(p), nil
}

func (w *memoryObjectWriter) Commit(ctx context.Context) error { return w.b.Write(ctx, w.key, w.buf) }
func (w *memoryObjectWriter) Abort()                           { w.buf = nil }

func TestStreamingMemoryConformance(t *testing.T) {
	t.Parallel()
	backendtest.Run(t, func(*testing.T) backend.Backend {
		return newStreamingMemory()
	})
}

func TestCachedConformance(t *testing.T) {
	t.Parallel()

	t.Run("over a whole-object backend", func(t *testing.T) {
		t.Parallel()
		backendtest.Run(t, func(*testing.T) backend.Backend {
			return backend.Cached(backend.Memory(), 1<<20)
		})
	})

	t.Run("over a streaming backend", func(t *testing.T) {
		t.Parallel()
		backendtest.Run(t, func(*testing.T) backend.Backend {
			return backend.Cached(newStreamingMemory(), 1<<20)
		})
	})
}

// TestStreamsWritesIsNotClaimedByWrappers is the trap the [backend.SpaceReporter] forwarding already
// fell into, in reverse: a wrapper that forwards [backend.ObjectCreator] unconditionally would
// report a streaming write over an inner backend that buffers, and the merge engine sizes parts on
// that answer.
func TestStreamsWritesIsNotClaimedByWrappers(t *testing.T) {
	t.Parallel()

	assert.False(t, backend.StreamsWrites(backend.Memory()))
	assert.True(t, backend.StreamsWrites(newStreamingMemory()))

	assert.False(t, backend.StreamsWrites(backend.Cached(backend.Memory(), 1<<20)),
		"caching a whole-object backend does not make it stream")
	assert.True(t, backend.StreamsWrites(backend.Cached(newStreamingMemory(), 1<<20)),
		"caching a streaming backend must not hide the capability")
}

// TestCachedStreamedWriteInvalidates covers the coherence rule: a streamed object replaces the key,
// so a value cached from the old one must not survive it.
func TestCachedStreamedWriteInvalidates(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	inner := newStreamingMemory()
	c := backend.Cached(inner, 1<<20)

	require.NoError(t, c.Write(ctx, "k", []byte("first")))

	got, err := c.Read(ctx, "k")
	require.NoError(t, err)
	require.Equal(t, []byte("first"), got)

	w, err := backend.CreateObject(ctx, c, "k")
	require.NoError(t, err)

	_, err = w.Write([]byte("second"))
	require.NoError(t, err)

	got, err = c.Read(ctx, "k")
	require.NoError(t, err)
	assert.Equal(t, []byte("first"), got, "an uncommitted stream must not disturb the cached value")

	require.NoError(t, w.Commit(ctx))
	assert.Equal(t, 1, inner.creates, "the wrapper must forward, not buffer")

	got, err = c.Read(ctx, "k")
	require.NoError(t, err)
	assert.Equal(t, []byte("second"), got)
}

// TestCachedStreamedAbortKeepsCache pins the other half: an aborted stream changed nothing, so it
// must not evict either.
func TestCachedStreamedAbortKeepsCache(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	c := backend.Cached(newStreamingMemory(), 1<<20)

	require.NoError(t, c.Write(ctx, "k", []byte("first")))

	w, err := backend.CreateObject(ctx, c, "k")
	require.NoError(t, err)

	_, err = w.Write([]byte("second"))
	require.NoError(t, err)
	w.Abort()

	got, err := c.Read(ctx, "k")
	require.NoError(t, err)
	assert.Equal(t, []byte("first"), got)
}

// TestUncachedHelpersSeeStreamingWrapper guards the second type [backend.Cached] can return: the
// uncached helpers recognize a cached backend by assertion, and a plain one would miss it and
// silently start caching the identity sets they exist to keep out.
func TestUncachedHelpersSeeStreamingWrapper(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	c := backend.Cached(newStreamingMemory(), 1<<20)

	require.NoError(t, backend.WriteUncached(ctx, c, "k", []byte("v")))

	got, err := backend.ReadUncached(ctx, c, "k")
	require.NoError(t, err)
	assert.Equal(t, []byte("v"), got)

	stats := c.(interface{ Stats() backend.CacheStats }).Stats()
	assert.Zero(t, stats.Items, "neither helper may populate the cache")
}

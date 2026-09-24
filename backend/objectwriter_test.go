package backend_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/backend/backendtest"
)

func TestStreamingMemoryConformance(t *testing.T) {
	t.Parallel()
	backendtest.Run(t, func(*testing.T) backend.Backend {
		return backendtest.NewStreamingMemory()
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
			return backend.Cached(backendtest.NewStreamingMemory(), 1<<20)
		})
	})
}

// TestMemoryDoesNotStream: memory has no disk to keep finished bytes on, so claiming a streaming
// write would size merge output against one. Wrappers are covered by the root package's
// TestWrappersForwardExactlyTheirInnerCapabilities.
func TestMemoryDoesNotStream(t *testing.T) {
	t.Parallel()

	assert.False(t, backend.StreamsWrites(backend.Memory()))
	assert.True(t, backend.StreamsWrites(backendtest.NewStreamingMemory()))
}

func TestRangesNatively(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	bare := backendtest.WithoutCapabilities(backend.Memory())

	assert.True(t, backend.RangesNatively(ctx, backend.Memory(), "k"))
	assert.False(t, backend.RangesNatively(ctx, bare, "k"))
	assert.True(t, backend.RangesNatively(ctx, backend.Cached(backend.Memory(), 1<<20), "k"))
	assert.False(t, backend.RangesNatively(ctx, backend.Cached(bare, 1<<20), "k"),
		"a cache over a whole-object store serves every ranged miss whole")
}

// TestCachedStreamedWriteInvalidates covers the coherence rule: a streamed object replaces the key,
// so a value cached from the old one must not survive it.
func TestCachedStreamedWriteInvalidates(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	inner := backendtest.NewStreamingMemory()
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
	assert.Equal(t, int64(1), inner.Creates(), "the wrapper must forward, not buffer")

	got, err = c.Read(ctx, "k")
	require.NoError(t, err)
	assert.Equal(t, []byte("second"), got)
}

// TestCachedStreamedAbortKeepsCache pins the other half: an aborted stream changed nothing, so it
// must not evict either.
func TestCachedStreamedAbortKeepsCache(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	c := backend.Cached(backendtest.NewStreamingMemory(), 1<<20)

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

// TestUncachedHelpersSeeStreamingWrapper guards the assertion the uncached helpers recognize a
// cached backend by: miss it and they would silently start caching the identity sets they exist to
// keep out.
func TestUncachedHelpersSeeStreamingWrapper(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	c := backend.Cached(backendtest.NewStreamingMemory(), 1<<20)

	require.NoError(t, backend.WriteUncached(ctx, c, "k", []byte("v")))

	got, err := backend.ReadUncached(ctx, c, "k")
	require.NoError(t, err)
	assert.Equal(t, []byte("v"), got)

	stats := c.(interface{ Stats() backend.CacheStats }).Stats()
	assert.Zero(t, stats.Items, "neither helper may populate the cache")
}

package backend_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/backend/backendtest"
)

func TestDeferredFallsBackToSynchronous(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	b := backendtest.WithoutCapabilities(backend.Memory())

	require.NoError(t, backend.WriteDeferred(ctx, b, "p/a", []byte("a")))

	w, err := backend.CreateObjectDeferred(ctx, b, "p/b")
	require.NoError(t, err)
	_, err = w.Write([]byte("b"))
	require.NoError(t, err)
	require.NoError(t, w.Commit(ctx))

	require.NoError(t, backend.SyncPrefix(ctx, b, "p"))

	keys, err := b.List(ctx, "p/")
	require.NoError(t, err)
	assert.Equal(t, []string{"p/a", "p/b"}, keys)

	require.NoError(t, backend.DeleteDeferred(ctx, b, "p/a"))
	_, err = b.Read(ctx, "p/a")
	assert.ErrorIs(t, err, backend.ErrNotExist)
}

// TestCachedForwardsDeferredSync checks the cache neither drops the capability — the saving would
// silently vanish — nor serves a value a deferred operation superseded.
func TestCachedForwardsDeferredSync(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	inner := backendtest.WithDeferred(backend.Memory())
	c := backend.Cached(inner, 1<<20)

	require.NoError(t, c.Write(ctx, "p/k", []byte("v1")))
	require.NoError(t, backend.WriteDeferred(ctx, c, "p/k", []byte("v2")))
	assert.Equal(t, []byte("v2"), mustRead(t, c, "p/k"))

	w, err := backend.CreateObjectDeferred(ctx, c, "p/k")
	require.NoError(t, err)
	_, err = w.Write([]byte("v3"))
	require.NoError(t, err)
	require.NoError(t, w.Commit(ctx))
	assert.Equal(t, []byte("v3"), mustRead(t, c, "p/k"))

	require.NoError(t, backend.WriteUncachedDeferred(ctx, c, "p/id", []byte("id")))
	assert.Equal(t, []string{"p/id", "p/k"}, inner.Pending("p"))

	require.NoError(t, backend.SyncPrefix(ctx, c, "p"))
	assert.Empty(t, inner.Pending("p"))

	require.NoError(t, backend.DeleteDeferred(ctx, c, "p/k"))
	assert.Equal(t, []string{"~p/k"}, inner.Deletes())

	_, err = c.Read(ctx, "p/k")
	assert.ErrorIs(t, err, backend.ErrNotExist)
}

func mustRead(t *testing.T, b backend.Backend, key string) []byte {
	t.Helper()

	v, err := b.Read(context.Background(), key)
	require.NoError(t, err)

	return v
}

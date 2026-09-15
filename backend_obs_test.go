package storage

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/backend/backendtest"
	"github.com/oteldb/storage/internal/obs"
)

// TestWrappersForwardDeferredSync keeps the saving from silently vanishing under the metering and
// EC wrappers, and EC deletes synchronous.
func TestWrappersForwardDeferredSync(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	inner := backendtest.WithDeferred(backend.Memory())

	for name, b := range map[string]backend.Backend{
		"instrumented": instrumentBackend(inner, obs.NewNop().Backend),
		"ec":           &ecBackend{inner: inner},
	} {
		require.NoError(t, backend.WriteDeferred(ctx, b, name+"/a", []byte("a")), name)

		w, err := backend.CreateObjectDeferred(ctx, b, name+"/b")
		require.NoError(t, err, name)
		require.NoError(t, w.Commit(ctx), name)

		assert.Equal(t, []string{name + "/a", name + "/b"}, inner.Pending(name), name)
		require.NoError(t, backend.SyncPrefix(ctx, b, name), name)
		assert.Empty(t, inner.Pending(name), name)

		require.NoError(t, backend.DeleteDeferred(ctx, b, name+"/a"), name)
	}

	assert.ElementsMatch(t, []string{"~instrumented/a", "ec/a"}, inner.Deletes())
}

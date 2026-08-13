package backend_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/backend/file"
)

// TestFreeSpaceReporting pins the optional capability: a file backend reports a positive figure,
// an ephemeral one reports ErrSpaceUnknown, and the cache wrapper forwards rather than hides it.
func TestFreeSpaceReporting(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	t.Run("memory is unknown", func(t *testing.T) {
		t.Parallel()

		_, err := backend.FreeSpace(ctx, backend.Memory())
		require.ErrorIs(t, err, backend.ErrSpaceUnknown)
	})

	t.Run("file reports", func(t *testing.T) {
		t.Parallel()

		f, err := file.New(t.TempDir())
		require.NoError(t, err)

		n, err := backend.FreeSpace(ctx, f)
		if err != nil {
			require.ErrorIs(t, err, backend.ErrSpaceUnknown,
				"a platform without statfs must report unknown, not a hard failure")
			t.Skip("no free-space reporting on this platform")
		}

		assert.Positive(t, n, "a writable temp dir must have some room")
	})

	t.Run("cache forwards", func(t *testing.T) {
		t.Parallel()

		f, err := file.New(t.TempDir())
		require.NoError(t, err)

		direct, err := backend.FreeSpace(ctx, f)
		if err != nil {
			t.Skip("no free-space reporting on this platform")
		}

		cached, err := backend.FreeSpace(ctx, backend.Cached(f, 1<<20))
		require.NoError(t, err, "the cache wrapper must not hide the capability")
		assert.InDelta(t, direct, cached, float64(direct)/10)
	})
}

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

// TestFreeInodeReporting pins the second capacity axis. It is separate from bytes because a
// filesystem with terabytes free can still fail every create, and only a unix statfs reports it —
// elsewhere, and on a filesystem that allocates inodes dynamically, "unknown" is the honest answer.
func TestFreeInodeReporting(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	t.Run("memory is unknown", func(t *testing.T) {
		t.Parallel()

		_, err := backend.FreeInodes(ctx, backend.Memory())
		require.ErrorIs(t, err, backend.ErrSpaceUnknown)
	})

	t.Run("file reports or declines", func(t *testing.T) {
		t.Parallel()

		f, err := file.New(t.TempDir())
		require.NoError(t, err)

		n, err := backend.FreeInodes(ctx, f)
		if err != nil {
			require.ErrorIs(t, err, backend.ErrSpaceUnknown,
				"a platform or filesystem without an inode table must report unknown, not a hard failure")
			t.Skip("no inode reporting here")
		}

		assert.Positive(t, n, "a writable temp dir must be able to hold another file")
	})

	t.Run("cache forwards", func(t *testing.T) {
		t.Parallel()

		f, err := file.New(t.TempDir())
		require.NoError(t, err)

		direct, err := backend.FreeInodes(ctx, f)
		if err != nil {
			t.Skip("no inode reporting here")
		}

		cached, err := backend.FreeInodes(ctx, backend.Cached(f, 1<<20))
		require.NoError(t, err, "the cache wrapper must not hide the capability")
		assert.InDelta(t, direct, cached, float64(direct)/10)
	})
}

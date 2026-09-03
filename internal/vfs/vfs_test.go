package vfs_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/internal/vfs"
	"github.com/oteldb/storage/internal/vfs/vfstest"
)

// TestOSConformance runs the shared suite against a real directory: the reference answer the
// in-memory fake is held to.
func TestOSConformance(t *testing.T) {
	t.Parallel()

	vfstest.Conformance(t, func(t *testing.T) vfs.FS {
		t.Helper()

		f, err := vfs.OpenRoot(t.TempDir(), 0o750)
		require.NoError(t, err)
		t.Cleanup(func() { _ = f.Close() })

		return f
	})
}

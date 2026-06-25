package file_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/backend/backendtest"
	"github.com/oteldb/storage/backend/file"
)

func TestFileConformance(t *testing.T) {
	t.Parallel()
	backendtest.Run(t, func(t *testing.T) backend.Backend {
		t.Helper()
		b, err := file.New(t.TempDir())
		require.NoError(t, err)

		return b
	})
}

func TestFileIsNotEphemeral(t *testing.T) {
	t.Parallel()
	b, err := file.New(t.TempDir())
	require.NoError(t, err)
	assert.False(t, b.IsEphemeral())
}

func TestFilePersistsAcrossReopen(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	dir := t.TempDir()

	b1, err := file.New(dir)
	require.NoError(t, err)
	require.NoError(t, b1.Write(ctx, "a/b", []byte("persisted")))

	b2, err := file.New(dir)
	require.NoError(t, err)
	got, err := b2.Read(ctx, "a/b")
	require.NoError(t, err)
	assert.Equal(t, []byte("persisted"), got)
}

func TestFileRejectsTraversal(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	b, err := file.New(t.TempDir())
	require.NoError(t, err)

	err = b.Write(ctx, "../escape", []byte("x"))
	require.Error(t, err, "key escaping root must be rejected")

	_, err = b.Read(ctx, "../../etc/passwd")
	require.Error(t, err)
}

// TestFileReadDirectoryErrors covers the non-not-exist Read error branch: a key that
// resolves to a directory cannot be read as a file.
func TestFileReadDirectoryErrors(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	b, err := file.New(t.TempDir())
	require.NoError(t, err)

	require.NoError(t, b.Write(ctx, "d/x", []byte("v"))) // creates directory "d"

	_, err = b.Read(ctx, "d")
	require.Error(t, err, "reading a directory key must error")
	assert.NotErrorIs(t, err, backend.ErrNotExist, "a directory is not 'not exist'")
}

// TestFileWriteParentIsFile covers the MkdirAll error branch: a key whose parent path
// is an existing file cannot have its directory created.
func TestFileWriteParentIsFile(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	b, err := file.New(t.TempDir())
	require.NoError(t, err)

	require.NoError(t, b.Write(ctx, "f", []byte("v")))

	err = b.Write(ctx, "f/child", []byte("v"))
	require.Error(t, err, "parent 'f' is a file; mkdir must fail")
}

// TestFileDeleteNonEmptyDir covers the non-not-exist Delete error branch.
func TestFileDeleteNonEmptyDir(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	b, err := file.New(t.TempDir())
	require.NoError(t, err)

	require.NoError(t, b.Write(ctx, "d/x", []byte("v")))

	err = b.Delete(ctx, "d") // "d" is a non-empty directory
	require.Error(t, err)
	assert.NotErrorIs(t, err, backend.ErrNotExist)
}

// TestFileNewUnderFileErrors covers the New error branch: rooting under an existing
// file makes MkdirAll fail.
func TestFileNewUnderFileErrors(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	f := filepath.Join(root, "afile")
	require.NoError(t, os.WriteFile(f, []byte("x"), 0o600))

	_, err := file.New(filepath.Join(f, "sub")) // parent is a file
	require.Error(t, err)
}

// TestFileWriteIntoReadOnlyDir covers the temp-create error branch in Write.
func TestFileWriteIntoReadOnlyDir(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	root := t.TempDir()
	b, err := file.New(root)
	require.NoError(t, err)
	require.NoError(t, b.Write(ctx, "sub/a", []byte("v")))

	sub := filepath.Join(root, "sub")
	require.NoError(t, os.Chmod(sub, 0o500))       // read+execute, no write
	t.Cleanup(func() { _ = os.Chmod(sub, 0o700) }) // restore so TempDir cleanup works

	err = b.Write(ctx, "sub/b", []byte("v"))
	require.Error(t, err, "creating a temp file in a read-only dir must fail")
}

// TestFileWriteRenameOverDirErrors covers the rename error branch (and the deferred
// temp cleanup): writing a key that resolves to an existing non-empty directory makes
// the final rename fail after the temp file is fully written, synced, and closed.
func TestFileWriteRenameOverDirErrors(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	root := t.TempDir()
	b, err := file.New(root)
	require.NoError(t, err)

	require.NoError(t, b.Write(ctx, "k/x", []byte("v"))) // "k" is now a non-empty dir

	err = b.Write(ctx, "k", []byte("v")) // rename(tmp, root/k) fails: k is a dir
	require.Error(t, err, "rename over a non-empty directory must fail")

	// The temp file must have been cleaned up by the deferred handler.
	entries, derr := os.ReadDir(root)
	require.NoError(t, derr)
	for _, e := range entries {
		assert.NotContains(t, e.Name(), ".tmp-", "temp leaked after failed rename: %s", e.Name())
	}
}

// TestFileDeleteRejectsTraversal covers the path-validation error branch in Delete.
func TestFileDeleteRejectsTraversal(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	b, err := file.New(t.TempDir())
	require.NoError(t, err)

	err = b.Delete(ctx, "../escape")
	require.Error(t, err, "delete of an escaping key must be rejected")
}

// TestFileListSkipsTempFiles covers the leftover-temp-file skip branch in List.
func TestFileListSkipsTempFiles(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	root := t.TempDir()
	b, err := file.New(root)
	require.NoError(t, err)

	require.NoError(t, b.Write(ctx, "real", []byte("v")))
	// Simulate a leftover temp file from an interrupted write.
	require.NoError(t, os.WriteFile(filepath.Join(root, ".tmp-leftover"), []byte("x"), 0o600))

	keys, err := b.List(ctx, "")
	require.NoError(t, err)
	assert.Equal(t, []string{"real"}, keys, "leftover temp files must not appear as keys")
}

// TestFileListWalkError covers the WalkDir error-propagation branch: an unreadable
// subdirectory surfaces an error from the walk.
func TestFileListWalkError(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	root := t.TempDir()
	b, err := file.New(root)
	require.NoError(t, err)
	require.NoError(t, b.Write(ctx, "sub/a", []byte("v")))

	sub := filepath.Join(root, "sub")
	require.NoError(t, os.Chmod(sub, 0o000)) // unreadable/untraversable
	t.Cleanup(func() { _ = os.Chmod(sub, 0o700) })

	_, err = b.List(ctx, "")
	require.Error(t, err, "walk over an unreadable subdir must error")
}

func TestFileAtomicWriteLeavesNoTemp(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	dir := t.TempDir()

	b, err := file.New(dir)
	require.NoError(t, err)
	require.NoError(t, b.Write(ctx, "x", []byte("v")))

	// No leftover temp files in the tree, and List ignores any that might appear.
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	for _, e := range entries {
		assert.NotContains(t, e.Name(), ".tmp-", "temp file leaked: %s", e.Name())
	}

	keys, err := b.List(ctx, "")
	require.NoError(t, err)
	assert.Equal(t, []string{"x"}, keys)
}

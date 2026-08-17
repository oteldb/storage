package file_test

import (
	"context"
	"os"
	"path/filepath"
	"runtime"
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

func TestFileIsNodeLocal(t *testing.T) {
	t.Parallel()
	b, err := file.New(t.TempDir())
	require.NoError(t, err)
	assert.True(t, backend.IsNodeLocal(b))
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
	if runtime.GOOS == "windows" {
		t.Skip("POSIX directory permission bits do not block file creation on Windows")
	}
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
	if runtime.GOOS == "windows" {
		t.Skip("POSIX directory permission bits do not block directory walks on Windows")
	}
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

// TestFileListPrefixBoundsTraversal asserts the prefix bounds the work, not just the result:
// an unreadable sibling subtree — which fails the walk when visited — must not be visited.
func TestFileListPrefixBoundsTraversal(t *testing.T) {
	t.Parallel()
	if runtime.GOOS == "windows" {
		t.Skip("POSIX directory permission bits do not block directory walks on Windows")
	}
	ctx := context.Background()
	root := t.TempDir()
	b, err := file.New(root)
	require.NoError(t, err)

	require.NoError(t, b.Write(ctx, "default/metrics/0/manifest", []byte("v")))
	require.NoError(t, b.Write(ctx, "default/logs/0/manifest", []byte("v")))

	other := filepath.Join(root, "default", "logs")
	require.NoError(t, os.Chmod(other, 0o000))
	t.Cleanup(func() { _ = os.Chmod(other, 0o700) })

	keys, err := b.List(ctx, "default/metrics/")
	require.NoError(t, err, "listing one signal must not traverse the others")
	assert.Equal(t, []string{"default/metrics/0/manifest"}, keys)
}

func TestFileListPartialSegmentPrefix(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	b, err := file.New(t.TempDir())
	require.NoError(t, err)

	for _, k := range []string{
		"default/metrics/a", "default/metadata/b", "default/logs/c", "other/metrics/d",
	} {
		require.NoError(t, b.Write(ctx, k, []byte("v")))
	}

	// "met" is a partial final segment: both sibling directories still match.
	keys, err := b.List(ctx, "default/met")
	require.NoError(t, err)
	assert.Equal(t, []string{"default/metadata/b", "default/metrics/a"}, keys)

	keys, err = b.List(ctx, "def")
	require.NoError(t, err)
	assert.Equal(t, []string{"default/logs/c", "default/metadata/b", "default/metrics/a"}, keys)
}

func TestFileListMissingPrefix(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	b, err := file.New(t.TempDir())
	require.NoError(t, err)
	require.NoError(t, b.Write(ctx, "a/b", []byte("v")))

	keys, err := b.List(ctx, "nope/deeper/")
	require.NoError(t, err, "a prefix with no objects lists empty, as on an object store")
	assert.Empty(t, keys)
}

// TestFileDeletePrunesEmptyDirs pins the second half of the fix: a deleted part must not leave
// its directories behind, or every later List keeps paying for them.
func TestFileDeletePrunesEmptyDirs(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	root := t.TempDir()
	b, err := file.New(root)
	require.NoError(t, err)

	require.NoError(t, b.Write(ctx, "t/metrics/0000000001/c/col", []byte("v")))
	require.NoError(t, b.Write(ctx, "t/metrics/0000000001/manifest", []byte("v")))
	require.NoError(t, b.Write(ctx, "t/metrics/0000000002/manifest", []byte("v")))

	require.NoError(t, b.Delete(ctx, "t/metrics/0000000001/c/col"))
	_, err = os.Stat(filepath.Join(root, "t", "metrics", "0000000001", "c"))
	require.ErrorIs(t, err, os.ErrNotExist, "emptied column dir must be removed")

	require.NoError(t, b.Delete(ctx, "t/metrics/0000000001/manifest"))
	_, err = os.Stat(filepath.Join(root, "t", "metrics", "0000000001"))
	require.ErrorIs(t, err, os.ErrNotExist, "emptied part dir must be removed")

	// Directories still holding objects survive, up to the root itself.
	_, err = os.Stat(filepath.Join(root, "t", "metrics"))
	require.NoError(t, err)

	require.NoError(t, b.Delete(ctx, "t/metrics/0000000002/manifest"))
	_, err = os.Stat(root)
	require.NoError(t, err, "root is never pruned")

	keys, err := b.List(ctx, "")
	require.NoError(t, err)
	assert.Empty(t, keys)
}

// TestFileNewSweepsEmptyDirs covers the one-time sweep for deployments that already leaked
// directories under a pre-pruning version.
func TestFileNewSweepsEmptyDirs(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	root := t.TempDir()

	require.NoError(t, os.MkdirAll(filepath.Join(root, "t", "metrics", "0000000001", "c"), 0o750))
	require.NoError(t, os.MkdirAll(filepath.Join(root, "t", "logs", "0000000002"), 0o750))
	require.NoError(t, os.MkdirAll(filepath.Join(root, "t", "traces", "0000000003"), 0o750))
	live := filepath.Join(root, "t", "traces", "0000000003", "manifest")
	require.NoError(t, os.WriteFile(live, []byte("v"), 0o600))

	b, err := file.New(root)
	require.NoError(t, err)

	for _, dead := range []string{
		filepath.Join(root, "t", "metrics"),
		filepath.Join(root, "t", "logs"),
	} {
		_, serr := os.Stat(dead)
		require.ErrorIs(t, serr, os.ErrNotExist, "empty subtree must be swept: %s", dead)
	}

	keys, err := b.List(ctx, "")
	require.NoError(t, err)
	assert.Equal(t, []string{"t/traces/0000000003/manifest"}, keys)
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

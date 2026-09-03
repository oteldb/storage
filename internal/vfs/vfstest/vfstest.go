// Package vfstest is the conformance suite every [vfs.FS] must pass.
//
// It exists to keep the in-memory fake honest: a fake that a test trusts but that answers
// differently from a real directory turns a passing suite into a false one, so both implementations
// answer the same questions here. It covers ordinary behavior only — durability is what the fake
// adds beyond a real filesystem's reach, and that is tested where it lives.
package vfstest

import (
	"io/fs"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/internal/vfs"
)

// New builds a fresh, empty filesystem for one subtest.
type New func(t *testing.T) vfs.FS

// Conformance runs the suite against the filesystems newFS builds.
func Conformance(t *testing.T, newFS New) {
	t.Helper()

	t.Run("WriteReadBack", func(t *testing.T) {
		f := newFS(t)

		w, err := f.OpenFile("obj", os.O_CREATE|os.O_WRONLY, 0o600)
		require.NoError(t, err)
		n, err := w.Write([]byte("hello"))
		require.NoError(t, err)
		assert.Equal(t, 5, n)
		require.NoError(t, w.Sync())
		require.NoError(t, w.Close())

		got, err := f.ReadFile("obj")
		require.NoError(t, err)
		assert.Equal(t, []byte("hello"), got)
	})

	t.Run("ReadAt", func(t *testing.T) {
		f := newFS(t)
		write(t, f, "obj", "0123456789")

		r, err := f.OpenFile("obj", os.O_RDONLY, 0)
		require.NoError(t, err)
		defer func() { _ = r.Close() }()

		buf := make([]byte, 4)
		_, err = r.ReadAt(buf, 3)
		require.NoError(t, err)
		assert.Equal(t, []byte("3456"), buf)
	})

	t.Run("MissingIsNotExist", func(t *testing.T) {
		f := newFS(t)

		_, err := f.OpenFile("nope", os.O_RDONLY, 0)
		require.ErrorIs(t, err, fs.ErrNotExist)

		_, err = f.ReadFile("nope")
		require.ErrorIs(t, err, fs.ErrNotExist)

		_, err = f.Stat("nope")
		require.ErrorIs(t, err, fs.ErrNotExist)

		require.ErrorIs(t, f.Remove("nope"), fs.ErrNotExist)
	})

	t.Run("EscapingNameRefused", func(t *testing.T) {
		f := newFS(t)

		_, err := f.OpenFile("../outside", os.O_RDONLY, 0)
		assert.Error(t, err, "a name may not leave the root")
	})

	t.Run("MkdirAllAndNested", func(t *testing.T) {
		f := newFS(t)
		require.NoError(t, f.MkdirAll("a/b/c", 0o750))
		write(t, f, "a/b/c/obj", "deep")

		got, err := f.ReadFile("a/b/c/obj")
		require.NoError(t, err)
		assert.Equal(t, []byte("deep"), got)

		info, err := f.Stat("a/b")
		require.NoError(t, err)
		assert.True(t, info.IsDir())
	})

	t.Run("ReadDirSorted", func(t *testing.T) {
		f := newFS(t)
		require.NoError(t, f.MkdirAll("d", 0o750))
		write(t, f, "d/b", "2")
		write(t, f, "d/a", "1")
		require.NoError(t, f.MkdirAll("d/sub", 0o750))

		entries, err := f.ReadDir("d")
		require.NoError(t, err)

		names := make([]string, len(entries))
		for i, e := range entries {
			names[i] = e.Name()
		}

		assert.Equal(t, []string{"a", "b", "sub"}, names)
		assert.True(t, entries[2].IsDir())
	})

	t.Run("RenameReplaces", func(t *testing.T) {
		f := newFS(t)
		write(t, f, "src", "new")
		write(t, f, "dst", "old")

		require.NoError(t, f.Rename("src", "dst"))

		got, err := f.ReadFile("dst")
		require.NoError(t, err)
		assert.Equal(t, []byte("new"), got)

		_, err = f.OpenFile("src", os.O_RDONLY, 0)
		require.ErrorIs(t, err, fs.ErrNotExist, "the source name is gone")
	})

	t.Run("LinkRefusesExisting", func(t *testing.T) {
		f := newFS(t)
		write(t, f, "src", "body")
		write(t, f, "taken", "other")

		require.NoError(t, f.Link("src", "fresh"))

		got, err := f.ReadFile("fresh")
		require.NoError(t, err)
		assert.Equal(t, []byte("body"), got)

		require.ErrorIs(t, f.Link("src", "taken"), fs.ErrExist)
	})

	t.Run("ExclusiveCreate", func(t *testing.T) {
		f := newFS(t)
		write(t, f, "obj", "x")

		_, err := f.OpenFile("obj", os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
		require.ErrorIs(t, err, fs.ErrExist)
	})

	t.Run("Truncate", func(t *testing.T) {
		f := newFS(t)
		write(t, f, "obj", "long-original")

		w, err := f.OpenFile("obj", os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
		require.NoError(t, err)
		_, err = w.Write([]byte("hi"))
		require.NoError(t, err)
		require.NoError(t, w.Close())

		got, err := f.ReadFile("obj")
		require.NoError(t, err)
		assert.Equal(t, []byte("hi"), got)
	})

	t.Run("RemoveThenGone", func(t *testing.T) {
		f := newFS(t)
		write(t, f, "obj", "x")
		require.NoError(t, f.Remove("obj"))

		_, err := f.OpenFile("obj", os.O_RDONLY, 0)
		require.ErrorIs(t, err, fs.ErrNotExist)
	})

	t.Run("SyncDirOnRoot", func(t *testing.T) {
		f := newFS(t)
		write(t, f, "obj", "x")
		assert.NoError(t, f.SyncDir("."), "syncing a directory that exists is not an error")
	})
}

// write creates name with body and syncs it — the shape every subtest needs before asserting.
func write(t *testing.T, f vfs.FS, name, body string) {
	t.Helper()

	w, err := f.OpenFile(name, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
	require.NoError(t, err)

	_, err = w.Write([]byte(body))
	require.NoError(t, err)
	require.NoError(t, w.Sync())
	require.NoError(t, w.Close())
	require.NoError(t, f.SyncDir(dirOf(name)))
}

// dirOf is path.Dir for the suite's slash-separated names, with the root spelled the way the
// implementations key it.
func dirOf(name string) string {
	for i := len(name) - 1; i >= 0; i-- {
		if name[i] == '/' {
			return name[:i]
		}
	}

	return "."
}

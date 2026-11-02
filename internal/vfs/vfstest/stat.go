package vfstest

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func runFileStat(t *testing.T, newFS New) {
	t.Helper()
	t.Run("FileStatSnapshot", func(t *testing.T) {
		f := newFS(t)
		file, err := f.OpenFile("object", os.O_CREATE|os.O_RDWR, 0o600)
		require.NoError(t, err)
		defer func() { _ = file.Close() }()
		_, err = file.Write([]byte("old"))
		require.NoError(t, err)
		before, err := file.Stat()
		require.NoError(t, err)
		require.Equal(t, "object", before.Name())
		require.False(t, before.IsDir())
		_, err = file.Write([]byte("more"))
		require.NoError(t, err)
		after, err := file.Stat()
		require.NoError(t, err)
		require.EqualValues(t, 3, before.Size(), "FileInfo is a metadata snapshot")
		require.EqualValues(t, 7, after.Size(), "Stat observes current handle contents")
		require.NoError(t, file.Close())
		// Windows reports ERROR_INVALID_HANDLE rather than fs.ErrClosed.
		closed, err := file.Stat()
		require.Error(t, err)
		require.Nil(t, closed)
	})
	t.Run("FileStatAfterReplacementAndRemoval", func(t *testing.T) {
		f := newFS(t)
		write(t, f, "object", "old")
		write(t, f, "replacement", "replacement")
		file, err := f.OpenFile("object", os.O_RDONLY, 0)
		require.NoError(t, err)
		defer func() { _ = file.Close() }()
		require.NoError(t, f.Rename("replacement", "object"))
		opened, err := file.Stat()
		require.NoError(t, err)
		named, err := f.Stat("object")
		require.NoError(t, err)
		require.EqualValues(t, 3, opened.Size())
		require.EqualValues(t, len("replacement"), named.Size())
		require.NoError(t, f.Remove("object"))
		opened, err = file.Stat()
		require.NoError(t, err)
		require.EqualValues(t, 3, opened.Size())
		data := make([]byte, opened.Size())
		_, err = file.ReadAt(data, 0)
		require.NoError(t, err)
		require.Equal(t, "old", string(data))
	})
}

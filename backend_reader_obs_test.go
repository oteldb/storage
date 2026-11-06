package storage

import (
	"io"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/backend/file"
	"github.com/oteldb/storage/internal/obs/obstest"
)

func TestMeteredReusableReader(t *testing.T) {
	t.Parallel()
	f, err := file.New(t.TempDir())
	require.NoError(t, err)
	o, metrics := obstest.New(t)
	b := backend.Cached(instrumentBackend(f, o.Backend), 1<<20)
	require.NoError(t, f.Write(t.Context(), "object", []byte("abcdef")))
	bytes, known := backend.ReaderMemory(b, "object")
	require.True(t, known)
	require.Positive(t, bytes)
	dst, err := backend.ReadAtAppend(t.Context(), b, []byte("prefix:"), "object", 2, 2)
	require.NoError(t, err)
	require.Equal(t, "prefix:cd", string(dst))
	r, supported, err := backend.OpenReader(t.Context(), b, "object")
	require.NoError(t, err)
	require.True(t, supported)
	t.Cleanup(func() { require.NoError(t, r.Close()) })
	scratch := make([]byte, 2)
	_, err = r.ReadAt(scratch, 0)
	require.NoError(t, err)
	_, err = r.Read(scratch)
	require.NoError(t, err)
	_, err = r.ReadAt(scratch, 10)
	require.ErrorIs(t, err, io.EOF)
	require.Equal(t, int64(4), metrics.Counter("storage.backend.ops", "op", "read", "result", "ok"))
	require.Zero(t, metrics.Counter("storage.backend.ops", "op", "read", "result", "error"))
	require.Equal(t, int64(6), metrics.Counter("storage.backend.bytes", "op", "read"))
}

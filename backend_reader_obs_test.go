package storage

import (
	"io"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/backend/file"
	"github.com/oteldb/storage/internal/obs"
	"github.com/oteldb/storage/internal/obs/obstest"
)

func TestMeteredObjectWrites(t *testing.T) {
	t.Parallel()
	f, err := file.New(t.TempDir())
	require.NoError(t, err)
	o, metrics := obstest.New(t)
	b := instrumentBackend(f, o.Backend)
	ctx := obs.WithWritePhase(t.Context(), "sample_merge")

	w, err := backend.CreateObjectDeferred(ctx, b, "metric/p-1/c/0")
	require.NoError(t, err)
	_, err = w.Write([]byte("abc"))
	require.NoError(t, err)
	require.NoError(t, w.Commit(ctx))
	w.Abort()

	w, err = backend.CreateObjectDeferred(ctx, b, "metric/p-2/c/0")
	require.NoError(t, err)
	_, err = w.Write([]byte("defgh"))
	require.NoError(t, err)
	w.Abort()

	ctx = obs.WithWritePhase(t.Context(), "descriptor_delta")
	require.NoError(t, b.Write(ctx, "metric/s-1/.postings/0", []byte("ij")))
	require.NoError(t, backend.WriteDeferred(ctx, b, "metric/s-1/c/0", []byte("klm")))
	require.EqualValues(t, 3, metrics.Counter("storage.backend.object_write_bytes", "kind", "column", "phase", "sample_merge", "result", "ok"))
	require.EqualValues(t, 5, metrics.Counter("storage.backend.object_write_bytes", "kind", "column", "phase", "sample_merge", "result", "aborted"))
	require.EqualValues(t, 2, metrics.Counter("storage.backend.object_write_bytes", "kind", "postings_spill", "phase", "descriptor_delta", "result", "ok"))
	require.EqualValues(t, 3, metrics.Counter("storage.backend.object_write_bytes", "kind", "descriptor_ids", "phase", "descriptor_delta", "result", "ok"))
}

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

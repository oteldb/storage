package file_test

import (
	"context"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/backend/backendtest"
	"github.com/oteldb/storage/backend/file"
)

func TestNativeFileSize(t *testing.T) {
	t.Parallel()
	files, err := file.New(t.TempDir())
	require.NoError(t, err)
	require.NoError(t, files.Write(t.Context(), "p/obj", []byte("payload")))
	counted := backendtest.NewByteCounter(files)
	n, known, err := backend.NativeSizeOf(t.Context(), backend.Cached(counted, 1<<20), "p/obj")
	require.NoError(t, err)
	assert.True(t, known)
	assert.EqualValues(t, 7, n)
	assert.Zero(t, counted.Reads(), "file sizes use stat without a content read")
	_, _, err = backend.NativeSizeOf(t.Context(), files, "absent")
	require.ErrorIs(t, err, backend.ErrNotExist)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	_, _, err = files.NativeSize(ctx, "p/obj")
	require.ErrorIs(t, err, context.Canceled)
	require.NoError(t, os.RemoveAll(files.Dir()))
	_, _, err = backend.NativeSizeOf(t.Context(), files, "p/obj")
	require.Error(t, err, "metadata errors must remain visible")
}

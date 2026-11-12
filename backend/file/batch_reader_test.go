package file

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/internal/readerpool"
)

func TestBatchKeepsOpenedFileGeneration(t *testing.T) {
	t.Parallel()
	be, err := New(t.TempDir())
	require.NoError(t, err)
	require.NoError(t, be.Write(t.Context(), "part/smax", []byte("old")))
	scope := readerpool.NewScope()
	base, batch := readerpool.WithBatch(t.Context())
	old := scope.Context(base)
	defer func() { require.NoError(t, batch.Close()); require.NoError(t, scope.Close()) }()
	data, err := be.ReadAtAppend(old, nil, "part/smax", 0, 3)
	require.NoError(t, err)
	require.Equal(t, "old", string(data))
	require.NoError(t, be.Write(t.Context(), "part/smax", []byte("new")))
	data, err = be.ReadAtAppend(old, data[:0], "part/smax", 0, 3)
	require.NoError(t, err)
	require.Equal(t, "old", string(data))
	// A fresh binding in the same batch must not reuse the old generation's handle.
	data, err = be.ReadAtAppend(scope.Context(base), data[:0], "part/smax", 0, 3)
	require.NoError(t, err)
	require.Equal(t, "new", string(data))
	require.NoError(t, scope.Close())
	data, err = be.ReadAtAppend(old, data[:0], "part/smax", 0, 3)
	require.NoError(t, err)
	require.Equal(t, "old", string(data), "the actual lease survives owner retirement")
	require.NoError(t, batch.Close())
	_, err = be.ReadAtAppend(old, nil, "part/smax", 0, 3)
	require.Error(t, err)
	data, err = be.ReadAtAppend(context.Background(), nil, "part/smax", 0, 3)
	require.NoError(t, err)
	require.Equal(t, "new", string(data))
}

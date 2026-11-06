package storage

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/backend/file"
	"github.com/oteldb/storage/internal/obs"
)

func TestMeteredWriterMemory(t *testing.T) {
	t.Parallel()
	f, err := file.New(t.TempDir())
	require.NoError(t, err)
	const key = "part/identity"
	base, known := backend.WriterMemory(f, key)
	require.True(t, known)
	n, known := backend.WriterMemory(backend.Cached(instrumentBackend(f, obs.NewNop().Backend), 4096), key)
	require.True(t, known)
	require.Greater(t, n, base)
	_, known = backend.WriterMemory(instrumentBackend(backend.Memory(), obs.NewNop().Backend), key)
	require.False(t, known)
}

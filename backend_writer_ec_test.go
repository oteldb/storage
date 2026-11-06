package storage

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/backend/file"
)

func TestECWriterMemory(t *testing.T) {
	t.Parallel()
	files, err := file.New(t.TempDir())
	require.NoError(t, err)
	for _, inner := range []backend.Backend{backend.Memory(), files, backend.Cached(files, 4096)} {
		const key = "part/identity"
		want, wantKnown := backend.WriterMemory(inner, key)
		got, gotKnown := backend.WriterMemory(&ecBackend{inner: inner}, key)
		require.Equal(t, wantKnown, gotKnown)
		require.Equal(t, want, got)
	}
}

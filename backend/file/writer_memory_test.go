package file_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/backend/file"
)

func TestWriterMemory(t *testing.T) {
	t.Parallel()
	files, err := file.New(t.TempDir())
	require.NoError(t, err)
	var previous int64
	for _, key := range []string{"identity", strings.Repeat("part/", 20) + "identity"} {
		n, known := backend.WriterMemory(files, key)
		require.True(t, known)
		require.Greater(t, n, int64(64<<10))
		require.Greater(t, n, previous)
		previous = n
	}
}

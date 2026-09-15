package block

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/backend/backendtest"
)

// TestDeletePartRetiresManifestFirst pins the order a resurrected object is harmless in: the
// manifest is gone durably before anything it names is deleted deferred.
func TestDeletePartRetiresManifestFirst(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	b := backendtest.WithDeferred(backend.Memory())
	w, _ := samplePartWriter(t)
	require.NoError(t, WritePart(ctx, b, "p", w))
	require.NoError(t, WritePart(ctx, b, "pp", w))

	require.NoError(t, DeletePart(ctx, b, "p"))

	assert.Equal(t, []string{"p/manifest", "~p/c/0", "~p/c/1", "~p/c/3", "~p/marks"}, b.Deletes())

	keys, err := b.List(ctx, "")
	require.NoError(t, err)
	assert.Equal(t, []string{"pp/c/0", "pp/c/1", "pp/c/3", "pp/manifest", "pp/marks"}, keys)
}

package recordengine_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/backend/bucketindex"
)

const enginePrefix = "t/recs"

func indexKey() string { return enginePrefix + "/" + bucketindex.Object }

// loadIndex returns the committed bucket index under the engine prefix.
func loadIndex(t *testing.T, be backend.Backend) *bucketindex.Index {
	t.Helper()

	ix, err := bucketindex.Load(context.Background(), be, indexKey())
	require.NoError(t, err)

	return ix
}

func prefixes(entries []bucketindex.Entry) []string {
	out := make([]string, len(entries))
	for i := range entries {
		out[i] = entries[i].Prefix
	}

	return out
}

func wantPrefixes(wants []bucketindex.Want) []string {
	out := make([]string, len(wants))
	for i := range wants {
		out[i] = wants[i].Prefix
	}

	return out
}

// diskPartKeys returns the backend objects of the part with the given id.
func diskPartKeys(ctx context.Context, t *testing.T, be backend.Backend, id string) []string {
	t.Helper()

	keys, err := be.List(ctx, enginePrefix+"/"+id+"/")
	require.NoError(t, err)

	return keys
}

// erasePart deletes every backend object of the part with the given id, the disk failure a repair
// exists for.
func erasePart(ctx context.Context, t *testing.T, be backend.Backend, id string) {
	t.Helper()

	for _, k := range diskPartKeys(ctx, t, be, id) {
		require.NoError(t, be.Delete(ctx, k))
	}
}

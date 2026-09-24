package recordengine_test

import (
	"context"
	"slices"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/recordengine"
)

// partIDs returns the engine's live part prefixes, sorted — the identity of a part survives only if
// it was not rewritten, which is how these tests tell a drop from a merge.
func partIDs(e *recordengine.Engine) []string {
	stats := e.Parts()

	out := make([]string, 0, len(stats))
	for _, p := range stats {
		out = append(out, p.ID)
	}

	slices.Sort(out)

	return out
}

// TestRetentionDropReclaimsSidecars checks the drop is a real reclaim: a record part carries
// side-store and bloom sidecars under its own prefix, and every object must go with it.
func TestRetentionDropReclaimsSidecars(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	b := backend.Memory()
	e := newEngine(t, b)

	ingest(t, e, mkBatch("api", rrec{ts: 100, body: "old", id: "a", attr: [2]string{"k", "v"}}))
	require.NoError(t, e.Flush(ctx))
	ingest(t, e, mkBatch("api", rrec{ts: 900, body: "new", id: "b"}))
	require.NoError(t, e.Flush(ctx))

	expired := partIDs(e)[0]

	keys, err := b.List(ctx, expired)
	require.NoError(t, err)
	require.NotEmpty(t, keys, "the part must have objects before the drop")

	require.NoError(t, e.Merge(ctx, 500))

	keys, err = b.List(ctx, expired)
	require.NoError(t, err)
	assert.Empty(t, keys, "the dropped part's objects and sidecars must be reclaimed")
}

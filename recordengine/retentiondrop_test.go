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

// TestRetentionDropsWholePartWithoutRewrite pins the point of the change: a part whose newest record
// is already past the cutoff holds nothing retention would keep, so it is retired on the manifest
// alone. The surviving part must keep its identity — a rewrite would mint a new prefix — which is
// what proves nothing was decoded and re-encoded to achieve the drop.
func TestRetentionDropsWholePartWithoutRewrite(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	e := newEngine(t, backend.Memory())

	ingest(t, e, mkBatch("api", rrec{ts: 100, body: "old"}))
	require.NoError(t, e.Flush(ctx))

	ingest(t, e, mkBatch("api", rrec{ts: 900, body: "new"}))
	require.NoError(t, e.Flush(ctx))

	before := partIDs(e)
	require.Len(t, before, 2)

	// Cutoff past every record of the first part, before every record of the second.
	require.NoError(t, e.Merge(ctx, 500))

	after := partIDs(e)
	require.Len(t, after, 1)
	assert.Equal(t, before[1], after[0], "the live part must not be rewritten to drop an unrelated one")

	got := fetchAll(t, e, req("api"))
	require.Len(t, got, 1)
	assert.Equal(t, []string{"new"}, bodies(got[0]))
}

// TestRetentionDropsExpiredAndRewritesStraddler checks the two paths coexist in one cycle: the fully
// expired part is dropped whole, while the part straddling the cutoff is rewritten to shed its old
// records (so its prefix changes).
func TestRetentionDropsExpiredAndRewritesStraddler(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	e := newEngine(t, backend.Memory())

	ingest(t, e, mkBatch("api", rrec{ts: 100, body: "old"}))
	require.NoError(t, e.Flush(ctx))

	ingest(t, e, mkBatch("api", rrec{ts: 400, body: "mid"}, rrec{ts: 900, body: "new"}))
	require.NoError(t, e.Flush(ctx))

	before := partIDs(e)
	require.Len(t, before, 2)

	require.NoError(t, e.Merge(ctx, 500))

	after := partIDs(e)
	require.Len(t, after, 1)
	assert.NotContains(t, before, after[0], "the straddling part must be rewritten, not kept")

	got := fetchAll(t, e, req("api"))
	require.Len(t, got, 1)
	assert.Equal(t, []string{"new"}, bodies(got[0]), "ts=100 and ts=400 are both past the cutoff")
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

// TestRetentionDropSurvivesRestart checks the drop is durable: the committed index must no longer
// name the expired part, so a reload does not resurrect it.
func TestRetentionDropSurvivesRestart(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	b := backend.Memory()
	e := newEngine(t, b)

	ingest(t, e, mkBatch("api", rrec{ts: 100, body: "old"}))
	require.NoError(t, e.Flush(ctx))
	ingest(t, e, mkBatch("api", rrec{ts: 900, body: "new"}))
	require.NoError(t, e.Flush(ctx))
	require.NoError(t, e.Merge(ctx, 500))

	want := partIDs(e)

	reopened := newEngine(t, b)
	require.NoError(t, reopened.LoadParts(ctx))

	assert.Equal(t, want, partIDs(reopened))

	got := fetchAll(t, reopened, req("api"))
	require.Len(t, got, 1)
	assert.Equal(t, []string{"new"}, bodies(got[0]))
}

// TestRetentionDropsEveryPart checks the degenerate case: a cutoff past every record leaves no part
// and no rows, with nothing rewritten on the way.
func TestRetentionDropsEveryPart(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	e := newEngine(t, backend.Memory())

	ingest(t, e, mkBatch("api", rrec{ts: 100, body: "old"}))
	require.NoError(t, e.Flush(ctx))
	ingest(t, e, mkBatch("api", rrec{ts: 200, body: "older"}))
	require.NoError(t, e.Flush(ctx))

	require.NoError(t, e.Merge(ctx, 1000))

	assert.Empty(t, partIDs(e))
	assert.Empty(t, fetchAll(t, e, req("api")))
}

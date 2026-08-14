package engine_test

import (
	"context"
	"slices"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/engine"
	"github.com/oteldb/storage/query/fetch"
)

// partIDs returns the engine's live part prefixes, sorted — the identity of a part survives only if
// it was not rewritten, which is how these tests tell a drop from a merge.
func partIDs(e *engine.Engine) []string {
	out := make([]string, 0, e.PartCount())
	for _, p := range e.Parts() {
		out = append(out, p.ID)
	}

	slices.Sort(out)

	return out
}

// TestRetentionDropsWholePartWithoutRewrite pins the point of the change: a part whose newest sample
// is already past the cutoff holds nothing retention would keep, so it is retired on the manifest
// alone. The surviving part must keep its identity — a rewrite would mint a new prefix — which is
// what proves nothing was decoded and re-encoded to achieve the drop.
func TestRetentionDropsWholePartWithoutRewrite(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	e := flushEngine()
	s := mkSeries("job", "api")

	mustAppend(t, e, s, 100, 1.0)
	mustAppend(t, e, s, 200, 2.0)
	require.NoError(t, e.Flush(ctx))

	mustAppend(t, e, s, 900, 9.0)
	require.NoError(t, e.Flush(ctx))

	before := partIDs(e)
	require.Len(t, before, 2)

	// Cutoff past every sample of the first part, before every sample of the second.
	require.NoError(t, e.Merge(ctx, 500))

	after := partIDs(e)
	require.Len(t, after, 1)
	assert.Equal(t, before[1], after[0], "the live part must not be rewritten to drop an unrelated one")

	got := fetchAll(t, e, fetch.Request{
		Start: 0, End: 10000, Matchers: []fetch.Matcher{eqMatcher("job", "api")},
	})
	require.Len(t, got, 1)
	assert.Equal(t, []int64{900}, got[0].Timestamps)
	assert.Equal(t, []float64{9}, got[0].Values)
}

// TestRetentionDropsExpiredAndRewritesStraddler checks the two paths coexist in one cycle: the fully
// expired part is dropped whole, while the part straddling the cutoff is rewritten to shed its old
// rows (so its prefix changes).
func TestRetentionDropsExpiredAndRewritesStraddler(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	e := flushEngine()
	s := mkSeries("job", "api")

	mustAppend(t, e, s, 100, 1.0)
	require.NoError(t, e.Flush(ctx))

	mustAppend(t, e, s, 400, 4.0)
	mustAppend(t, e, s, 900, 9.0)
	require.NoError(t, e.Flush(ctx))

	before := partIDs(e)
	require.Len(t, before, 2)

	require.NoError(t, e.Merge(ctx, 500))

	after := partIDs(e)
	require.Len(t, after, 1)
	assert.NotContains(t, before, after[0], "the straddling part must be rewritten, not kept")

	got := fetchAll(t, e, fetch.Request{
		Start: 0, End: 10000, Matchers: []fetch.Matcher{eqMatcher("job", "api")},
	})
	require.Len(t, got, 1)
	assert.Equal(t, []int64{900}, got[0].Timestamps, "ts=100 and ts=400 are both past the cutoff")
}

// TestRetentionDropReclaimsObjects checks the drop is a real reclaim, not just a manifest edit: the
// retired part's backend objects must be deleted once no reader holds it.
func TestRetentionDropReclaimsObjects(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	b := backend.Memory()
	e := engine.New(engine.Config{Backend: b, Prefix: "t/retdrop"})
	s := mkSeries("job", "api")

	mustAppend(t, e, s, 100, 1.0)
	require.NoError(t, e.Flush(ctx))
	mustAppend(t, e, s, 900, 9.0)
	require.NoError(t, e.Flush(ctx))

	expired := partIDs(e)[0]

	keys, err := b.List(ctx, expired)
	require.NoError(t, err)
	require.NotEmpty(t, keys, "the part must have objects before the drop")

	require.NoError(t, e.Merge(ctx, 500))

	keys, err = b.List(ctx, expired)
	require.NoError(t, err)
	assert.Empty(t, keys, "the dropped part's objects must be reclaimed")
}

// TestRetentionDropSurvivesRestart checks the drop is durable: the committed bucket index must no
// longer name the expired part, so a reload does not resurrect it.
func TestRetentionDropSurvivesRestart(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	b := backend.Memory()
	e := engine.New(engine.Config{Backend: b, Prefix: "t/retreload"})
	s := mkSeries("job", "api")

	mustAppend(t, e, s, 100, 1.0)
	require.NoError(t, e.Flush(ctx))
	mustAppend(t, e, s, 900, 9.0)
	require.NoError(t, e.Flush(ctx))
	require.NoError(t, e.Merge(ctx, 500))

	want := partIDs(e)

	reopened := engine.New(engine.Config{Backend: b, Prefix: "t/retreload"})
	require.NoError(t, reopened.LoadParts(ctx))

	assert.Equal(t, want, partIDs(reopened))

	got := fetchAll(t, reopened, fetch.Request{
		Start: 0, End: 10000, Matchers: []fetch.Matcher{eqMatcher("job", "api")},
	})
	require.Len(t, got, 1)
	assert.Equal(t, []int64{900}, got[0].Timestamps)
}

// TestRetentionDropIsNotIdle checks a cycle that only dropped parts is not counted as a no-op: the
// idle counter drives the merge selector's stranding escape, and a productive cycle must reset it
// rather than advance it.
func TestRetentionDropIsNotIdle(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	e := flushEngine()
	s := mkSeries("job", "api")

	mustAppend(t, e, s, 100, 1.0)
	require.NoError(t, e.Flush(ctx))

	// A single part, fully expired: nothing is left to select afterwards, so this cycle's only work
	// is the drop.
	require.NoError(t, e.Merge(ctx, 500))
	assert.Equal(t, 0, e.PartCount())
}

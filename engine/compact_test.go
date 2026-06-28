package engine_test

import (
	"context"
	"sort"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/engine"
	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/signal"
)

// flushDistinct appends n fresh series (keyed off base so successive flushes never collide) at ts and
// flushes them to one part, returning the series ids written.
func flushDistinct(t *testing.T, e *engine.Engine, base, n int, ts int64) []signal.SeriesID {
	t.Helper()

	ids := make([]signal.SeriesID, n)
	series := make([]signal.Series, n)
	tss := make([]int64, n)
	vals := make([]float64, n)

	for i := range series {
		series[i] = mkSeries("id", strconv.Itoa(base+i))
		ids[i] = series[i].Hash()
		tss[i], vals[i] = ts, float64(base+i)
	}

	_, err := e.AppendBatch(ids, tss, vals, nil, func(i int) signal.Series { return series[i] }, engine.AppendLimits{})
	require.NoError(t, err)
	require.NoError(t, e.Flush(context.Background()))

	return ids
}

// sortedPartKeys lists the engine's backend part objects in a stable order for before/after
// comparison (the shared partKeys helper does not sort).
func sortedPartKeys(t *testing.T, b backend.Backend) []string {
	t.Helper()

	keys := partKeys(t, b)
	sort.Strings(keys)

	return keys
}

// TestMergeDoesNotChurnSealedParts is the core of issue 22: once parts have reached the per-part size
// cap, a merge must not re-read and rewrite them every cycle (the unbounded write-amplification that
// pinned multi-GB of RSS). With every part sealed, a merge is a no-op — the part objects are
// untouched and the merge is idempotent.
func TestMergeDoesNotChurnSealedParts(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	// MaxPartBytes ≈ 5 rows per part, so each 5-series flush produces one full (sealed) part.
	b := backend.Memory()
	e := engine.New(engine.Config{Backend: b, Prefix: "default/metrics", MaxPartBytes: 160})

	for k := range 3 {
		flushDistinct(t, e, k*5, 5, int64(100+k))
	}

	require.Equal(t, 3, e.PartCount())
	before := sortedPartKeys(t, b)

	// Every part is sealed ⇒ the merge compacts nothing and rewrites no object.
	require.NoError(t, e.Merge(ctx, 0))
	assert.Equal(t, 3, e.PartCount(), "sealed parts are not re-compacted")
	assert.Equal(t, before, sortedPartKeys(t, b), "no part object was rewritten (no churn)")

	// And it is idempotent across cycles — the engine does not re-compact the whole set each tick.
	require.NoError(t, e.Merge(ctx, 0))
	assert.Equal(t, before, sortedPartKeys(t, b))
}

// TestMergeCompactsUnsealedTier confirms the other half: parts below the size cap and in the same
// size tier are compacted together, and the streaming output stays bounded by the cap.
func TestMergeCompactsUnsealedTier(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	// maxRows ≈ 5. Three flushes of 2 distinct series each ⇒ three 2-row (unsealed, tier 0) parts.
	b := backend.Memory()
	e := engine.New(engine.Config{Backend: b, Prefix: "default/metrics", MaxPartBytes: 160})

	want := make([]signal.SeriesID, 0, 6)
	for k := range 3 {
		want = append(want, flushDistinct(t, e, k*2, 2, int64(100+k))...)
	}

	require.Equal(t, 3, e.PartCount())

	require.NoError(t, e.Merge(ctx, 0))
	assert.Less(t, e.PartCount(), 3, "same-tier unsealed parts compact together")

	// Every output part stays within the cap (streaming never builds the whole merged set).
	for _, p := range e.Parts() {
		assert.LessOrEqual(t, p.Rows, int64(5), "merged output part respects MaxPartBytes")
	}

	// All 6 distinct series survive the compaction.
	got := fetchAll(t, e, fetch.Request{Start: 0, End: 1 << 62})
	assert.Len(t, got, len(want), "every series readable after tiered compaction")
}

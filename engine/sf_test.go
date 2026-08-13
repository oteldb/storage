package engine_test

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/block"
	"github.com/oteldb/storage/engine"
	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/signal"
)

func fetchOne(t *testing.T, e *engine.Engine, job string) *fetch.Batch {
	t.Helper()

	got := fetchAll(t, e, fetch.Request{Start: 0, End: 1 << 62, Matchers: []fetch.Matcher{eqMatcher("job", job)}})
	require.Len(t, got, 1)

	return got[0]
}

// TestScaleFactorRoundTrip verifies a sample's scale factor survives the head, a flush to the
// 4th part column, and a merge — and that the merged read still carries the weights.
func TestScaleFactorRoundTrip(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	e := engine.New(engine.Config{Backend: backend.Memory(), Prefix: "default/metrics"})
	s := mkSeries("job", "api")
	ids := []signal.SeriesID{s.Hash(), s.Hash(), s.Hash()}
	mat := func(int) signal.Series { return s }

	// Three samples weighted [1, 10, 1] (the middle one represents 10 originals).
	_, err := e.AppendBatch(ids, []int64{1, 2, 3}, []float64{10, 20, 30}, []float64{1, 10, 1}, mat, engine.AppendLimits{})
	require.NoError(t, err)

	// Head read carries the weights.
	b := fetchOne(t, e, "api")
	assert.Equal(t, []float64{10, 20, 30}, b.Values)
	assert.Equal(t, []float64{1, 10, 1}, b.ScaleFactors)

	// Survives a flush (written and read back through the 4th column).
	require.NoError(t, e.Flush(ctx))
	b = fetchOne(t, e, "api")
	assert.Equal(t, []float64{1, 10, 1}, b.ScaleFactors, "scale factors persist through flush")

	// A second flushed part, then a merge: the weights survive compaction.
	_, err = e.AppendBatch([]signal.SeriesID{s.Hash()}, []int64{4}, []float64{40}, []float64{5}, mat, engine.AppendLimits{})
	require.NoError(t, err)
	require.NoError(t, e.Flush(ctx))
	require.NoError(t, e.Merge(ctx, 0))
	assert.Equal(t, 1, e.PartCount())

	b = fetchOne(t, e, "api")
	assert.Equal(t, []int64{1, 2, 3, 4}, b.Timestamps)
	assert.Equal(t, []float64{1, 10, 1, 5}, b.ScaleFactors, "scale factors persist through merge")
}

// TestUnsampledNoScaleFactors confirms the common (unsampled) path is unchanged: weight-1 samples
// produce no scale-factor column and a nil ScaleFactors on read, so the part keeps its original
// three-column layout.
func TestUnsampledNoScaleFactors(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	e := engine.New(engine.Config{Backend: backend.Memory(), Prefix: "default/metrics"})
	s := mkSeries("job", "api")
	ids := []signal.SeriesID{s.Hash(), s.Hash()}

	// nil sf ⇒ every weight is 1.
	_, err := e.AppendBatch(ids, []int64{1, 2}, []float64{10, 20}, nil, func(int) signal.Series { return s }, engine.AppendLimits{})
	require.NoError(t, err)

	assert.Nil(t, fetchOne(t, e, "api").ScaleFactors, "no weights in the head")

	require.NoError(t, e.Flush(ctx))
	assert.Nil(t, fetchOne(t, e, "api").ScaleFactors, "no scale-factor column written")
}

// TestMergeDropsCollapsedScaleFactors covers the seam the streaming writer adds: a weight column
// has to be declared before the first row is encoded, but the part format leaves it *absent* rather
// than constant when nothing is sampled. Downsampling a sampled series with Sum emits weight 1 for
// every output, so the sources carry weights the output does not — the writer must drop the column
// again rather than leave a constant one behind, which would not be block-framed and would cost
// every reader of the part the block-sliced decode path.
func TestMergeDropsCollapsedScaleFactors(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	b := backend.Memory()
	e := engine.New(engine.Config{Backend: b, Prefix: "default/metrics"})
	s := mkSeries("job", "api")
	mat := func(int) signal.Series { return s }

	// Three sampled parts, all samples old enough for the tier below. Three so the merge takes the
	// multi-part streaming path rather than the single-part rewrite.
	for _, base := range []int64{10, 20, 30} {
		_, err := e.AppendBatch(
			[]signal.SeriesID{s.Hash(), s.Hash()},
			[]int64{base, base + 1}, []float64{float64(base), float64(base * 2)}, []float64{4, 4},
			mat, engine.AppendLimits{})
		require.NoError(t, err)
		require.NoError(t, e.Flush(ctx))
	}

	require.NoError(t, e.MergeWith(ctx, engine.MergeOptions{
		Downsample: []engine.DownsampleTier{{Before: 1 << 40, Interval: 1000, Agg: signal.AggSum}},
	}))
	require.Equal(t, 1, e.PartCount())

	// Sum folds each weight into its value, so every surviving weight is 1 and the column must not
	// be in the part at all — the layout an unsampled part has.
	assert.NotContains(t, mergedPartColumns(t, ctx, b), "sf",
		"an all-unit weight column must be dropped, not written as a constant")

	assert.Nil(t, fetchOne(t, e, "api").ScaleFactors, "and the read reports no weights")
}

// mergedPartColumns opens the single part under the engine's prefix and returns its column names.
func mergedPartColumns(t *testing.T, ctx context.Context, b backend.Backend) []string {
	t.Helper()

	keys, err := b.List(ctx, "default/metrics/")
	require.NoError(t, err)

	var prefixes []string

	for _, k := range keys {
		if p, ok := strings.CutSuffix(k, "/manifest"); ok {
			prefixes = append(prefixes, p)
		}
	}

	require.Len(t, prefixes, 1, "expected exactly one live part")

	r, err := block.OpenPart(ctx, b, prefixes[0])
	require.NoError(t, err)

	return r.ColumnNames()
}

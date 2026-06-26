package engine_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
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

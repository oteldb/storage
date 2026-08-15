package storage

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/signal"
)

func cardLabel(t *testing.T, cs CardinalityStats, name string) LabelCardinality {
	t.Helper()

	for _, l := range cs.TopLabelNames {
		if l.Name == name {
			return l
		}
	}

	t.Fatalf("label %q not found in %+v", name, cs.TopLabelNames)

	return LabelCardinality{}
}

func TestStoragePartsAndDetailed(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s, err := InMemory()
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close(ctx) })

	_, err = s.WriteMetrics(ctx, gaugeBatch("api", "m1", []int64{100, 200}, []float64{1, 2}))
	require.NoError(t, err)

	assert.Empty(t, s.Parts("default", signal.Metric), "no parts before flush")
	require.NoError(t, s.Admin().Flush(ctx, "default", signal.Metric))

	// "" normalizes to the default tenant.
	parts := s.Parts("", signal.Metric)
	require.Len(t, parts, 1)
	assert.Equal(t, int64(2), parts[0].Rows)
	assert.Equal(t, 1, parts[0].Series)
	assert.Equal(t, int64(100), parts[0].MinTime)
	assert.Equal(t, int64(200), parts[0].MaxTime)

	// A signal with no engine for the tenant returns nil.
	assert.Nil(t, s.Parts("default", signal.Trace))

	ds, err := s.PartsDetailed(ctx, "default", signal.Metric)
	require.NoError(t, err)
	require.Len(t, ds, 1)
	assert.Equal(t, int64(2), ds[0].Rows)
	assert.Positive(t, ds[0].Bytes)
	assert.GreaterOrEqual(t, ds[0].Chunks, 1)
	assert.NotEmpty(t, ds[0].Columns)

	// PartsDetailed for an absent engine is an empty, non-error result.
	none, err := s.PartsDetailed(ctx, "default", signal.Trace)
	require.NoError(t, err)
	assert.Nil(t, none)

	// After close, PartsDetailed reports the store is closed.
	require.NoError(t, s.Close(ctx))
	_, err = s.PartsDetailed(ctx, "default", signal.Metric)
	assert.ErrorIs(t, err, ErrClosed)
}

// TestStorageIntrospectLogs exercises the record-signal path (logs) through the storage facade —
// the mapping from recordengine stats to the public types.
func TestStorageIntrospectLogs(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s, err := InMemory()
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close(ctx) })

	_, err = s.WriteLogs(ctx, logBatch("api", [3]any{150, 9, "hello"}))
	require.NoError(t, err)
	_, err = s.WriteLogs(ctx, logBatch("web", [3]any{160, 9, "world"}))
	require.NoError(t, err)

	cs := s.Cardinality("default", signal.Log, 0)
	assert.Equal(t, int64(2), cs.TotalSeries, "two streams")
	assert.Equal(t, int64(2), cardLabel(t, cs, "service.name").Series)

	require.NoError(t, s.Admin().Flush(ctx, "default", signal.Log))

	parts := s.Parts("default", signal.Log)
	require.Len(t, parts, 1)
	assert.Equal(t, 2, parts[0].Series)
	assert.Equal(t, int64(2), parts[0].Rows)

	ds, err := s.PartsDetailed(ctx, "default", signal.Log)
	require.NoError(t, err)
	require.Len(t, ds, 1)
	assert.Positive(t, ds[0].Bytes)
	assert.GreaterOrEqual(t, ds[0].Chunks, 1)
	assert.NotEmpty(t, ds[0].Columns)
}

func TestStorageStreamCosts(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s, err := InMemory()
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close(ctx) })

	tmpl := make([][3]any, 0, 200)
	for i := range 200 {
		tmpl = append(tmpl, [3]any{100 + i, 9, fmt.Sprintf("reconciling pvc-%d at 10:00:%02d", i, i%60)})
	}

	_, err = s.WriteLogs(ctx, logBatch("klog", tmpl...))
	require.NoError(t, err)
	_, err = s.WriteLogs(ctx, logBatch("web", [3]any{100, 9, "GET / 200"}))
	require.NoError(t, err)

	// Metrics carry no per-record columns to attribute.
	_, err = s.StreamCosts(ctx, "default", signal.Metric, StreamCostOptions{})
	require.Error(t, err)

	// A signal with no engine for the tenant is an empty, non-error result.
	none, err := s.StreamCosts(ctx, "default", signal.Trace, StreamCostOptions{})
	require.NoError(t, err)
	assert.Nil(t, none)

	require.NoError(t, s.Admin().Flush(ctx, "default", signal.Log))

	cs, err := s.StreamCosts(ctx, "", signal.Log, StreamCostOptions{GroupBy: "service.name"})
	require.NoError(t, err)
	require.Len(t, cs, 2)

	top := cs[0]
	assert.Equal(t, "klog", top.Key)
	assert.Equal(t, int64(200), top.Rows)
	assert.Equal(t, 1, top.Streams)
	assert.Positive(t, top.RawBytes)
	assert.Positive(t, top.DiskBytes)
	require.True(t, top.DistinctEstimated)

	var body ColumnCost

	for _, c := range top.Columns {
		if c.Name == "body" {
			body = c
		}
	}

	require.Equal(t, "body", body.Name)
	assert.InEpsilon(t, 200.0, float64(body.Distinct), 0.1, "every line differs")
	assert.LessOrEqual(t, body.DistinctNormalized, int64(4), "one template behind the digits")

	require.NoError(t, s.Close(ctx))
	_, err = s.StreamCosts(ctx, "default", signal.Log, StreamCostOptions{})
	assert.ErrorIs(t, err, ErrClosed)
}

func TestStorageCardinality(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s, err := InMemory()
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close(ctx) })

	_, err = s.WriteMetrics(ctx, gaugeBatch("api", "m1", []int64{100}, []float64{1}))
	require.NoError(t, err)
	_, err = s.WriteMetrics(ctx, gaugeBatch("api", "m2", []int64{100}, []float64{2}))
	require.NoError(t, err)

	cs := s.Cardinality("default", signal.Metric, 0)
	assert.Equal(t, int64(2), cs.TotalSeries, "two metric series")
	assert.Positive(t, cs.SymbolCount)
	assert.Positive(t, cs.DistinctLabelNames)

	svc := cardLabel(t, cs, "service.name")
	assert.Equal(t, int64(2), svc.Series, "both series share the service")
	assert.Equal(t, 1, svc.DistinctValues)

	// An absent engine yields a zero value.
	assert.Equal(t, CardinalityStats{}, s.Cardinality("default", signal.Profile, 0))
}

func TestInspectWALAndMergeFields(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s, err := InMemory()
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close(ctx) })

	_, err = s.WriteMetrics(ctx, gaugeBatch("api", "m1", []int64{100, 200}, []float64{1, 2}))
	require.NoError(t, err)

	before, _ := findSignal(mustTenant(t, s.Inspect(), "default"), signal.Metric)
	assert.False(t, before.WAL, "the in-memory engine is ephemeral (no WAL)")
	assert.False(t, before.MergeRunning)
	assert.Equal(t, 0, before.MergeBacklog, "no parts yet")

	require.NoError(t, s.Admin().Flush(ctx, "default", signal.Metric))

	after, _ := findSignal(mustTenant(t, s.Inspect(), "default"), signal.Metric)
	assert.Equal(t, 1, after.MergeBacklog, "one flushed part is the backlog")
	assert.Equal(t, after.Parts, after.MergeBacklog, "backlog tracks the part count")
	assert.False(t, after.MergeRunning)
}

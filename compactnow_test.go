package storage

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/signal"
)

// bulkLogBatch builds n log records under one service, each with a body wide enough that a part of
// them lands well above the record engine's tier floor.
func bulkLogRecords(first, n int) [][3]any {
	body := strings.Repeat("x", 512)

	recs := make([][3]any, 0, n)
	for i := range n {
		recs = append(recs, [3]any{first + i, 9, fmt.Sprintf("%s-%d", body, first+i)})
	}

	return recs
}

// TestCompactNowBreaksMergeFixedPoint reproduces the state that made #287 an evening's work: one
// large part and one small one, in different size tiers, so no tier holds the two parts the
// selector waits for. A maintenance cycle selects nothing however often it runs — the part count is
// a fixed point — and only a forced compaction moves it.
func TestCompactNowBreaksMergeFixedPoint(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s, err := InMemory()
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close(ctx) })

	_, err = s.WriteLogs(ctx, logBatch("api", bulkLogRecords(1, 20_000)...))
	require.NoError(t, err)
	require.NoError(t, s.Admin().Flush(ctx, "default", signal.Log))

	_, err = s.WriteLogs(ctx, logBatch("api", [3]any{20_001, 9, "small"}))
	require.NoError(t, err)
	require.NoError(t, s.Admin().Flush(ctx, "default", signal.Log))

	st, ok := findSignal(mustTenant(t, s.Inspect(), "default"), signal.Log)
	require.True(t, ok)
	require.Equal(t, 2, st.Parts)
	assert.Equal(t, 0, st.SealedParts, "neither part is near the cap")
	assert.Equal(t, 2, st.MergeBacklog, "both parts remain mergeable")
	assert.Zero(t, st.MergeCandidates, "yet the selector would take none of them — the fixed point")
	assert.Positive(t, st.MergeCapBytes, "the seal threshold in effect is reported")

	// The background cycle is a no-op here however many times it runs.
	for range 3 {
		require.NoError(t, s.Admin().MaintainNow(ctx))
	}

	st, _ = findSignal(mustTenant(t, s.Inspect(), "default"), signal.Log)
	require.Equal(t, 2, st.Parts, "maintenance cannot break the fixed point")

	require.NoError(t, s.Admin().CompactNow(ctx, "default", signal.Log))

	st, _ = findSignal(mustTenant(t, s.Inspect(), "default"), signal.Log)
	assert.Equal(t, 1, st.Parts, "the forced compaction merged what the heuristic declined")
	assert.Equal(t, 1, st.MergeBacklog)

	// The records survived the forced merge.
	got := logBodies(t, s.LogFetcher("default"), fetch.Request{
		Start: 0, End: 100_000,
		Matchers: []fetch.Matcher{logSvcMatcher("api")},
	})
	assert.Len(t, got, 20_001, "no record lost")
}

// TestCompactNowMetrics covers the metric engine's force path: the same merge, with the
// write-amplification guard waived instead of waited out.
func TestCompactNowMetrics(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s, err := InMemory()
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close(ctx) })

	for i := range 2 {
		_, err = s.WriteMetrics(ctx, gaugeBatch("api", "m1", []int64{int64(i) + 1}, []float64{1}))
		require.NoError(t, err)
		require.NoError(t, s.Admin().Flush(ctx, "default", signal.Metric))
	}

	st, _ := findSignal(mustTenant(t, s.Inspect(), "default"), signal.Metric)
	require.Equal(t, 2, st.Parts)

	require.NoError(t, s.Admin().CompactNow(ctx, "default", signal.Metric))

	st, _ = findSignal(mustTenant(t, s.Inspect(), "default"), signal.Metric)
	assert.Equal(t, 1, st.Parts, "forced compaction merged the parts")
}

// TestMaintainPublishesPartGauges confirms the durable form of the diagnostic: a maintenance cycle
// leaves the part shape on the meter, so a stuck engine is visible on a dashboard rather than only
// in a Debug log.
func TestMaintainPublishesPartGauges(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	reader := sdkmetric.NewManualReader()
	s, err := InMemory(WithMeterProvider(sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))))
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close(ctx) })

	_, err = s.WriteMetrics(ctx, gaugeBatch("api", "m1", []int64{1}, []float64{1}))
	require.NoError(t, err)
	require.NoError(t, s.Admin().MaintainNow(ctx))

	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(ctx, &rm))

	want := map[string]string{"signal": "metric"}
	assert.Equal(t, int64(1), partGauge(t, rm, "storage.parts.total", want))
	assert.Equal(t, int64(1), partGauge(t, rm, "storage.parts.merge_backlog", want))
	assert.Zero(t, partGauge(t, rm, "storage.parts.sealed", want))
	assert.Positive(t, partGauge(t, rm, "storage.merge.cap_bytes", want),
		"the cap is derived by the cycle's merge and reported")
}

func partGauge(t *testing.T, rm metricdata.ResourceMetrics, name string, want map[string]string) int64 {
	t.Helper()

	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != name {
				continue
			}

			g, ok := m.Data.(metricdata.Gauge[int64])
			require.Truef(t, ok, "%s is not an int64 gauge", name)

			for _, dp := range g.DataPoints {
				matched := true

				for k, v := range want {
					value, ok := dp.Attributes.Value(attribute.Key(k))
					if !ok || value.AsString() != v {
						matched = false

						break
					}
				}

				if matched {
					return dp.Value
				}
			}
		}
	}

	t.Fatalf("gauge %q not found", name)

	return 0
}

func TestCompactNowUnknownIsNoOp(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s, err := InMemory()
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close(ctx) })

	require.NoError(t, s.Admin().CompactNow(ctx, "nobody", signal.Metric))
	require.NoError(t, s.Admin().CompactNow(ctx, "nobody", signal.Log))
}

func TestCompactNowClosed(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s, err := InMemory()
	require.NoError(t, err)
	require.NoError(t, s.Close(ctx))

	require.ErrorIs(t, s.Admin().CompactNow(ctx, "default", signal.Metric), ErrClosed)
}

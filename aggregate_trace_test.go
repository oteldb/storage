package storage

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/engine"
	"github.com/oteldb/storage/query/fetch"
)

// spanAttrs returns the named span's attributes, keyed by attribute name. It fails if the span was
// not recorded, which is the assertion most of these tests actually care about.
func spanAttrs(t *testing.T, spans tracetest.SpanStubs, name string) map[string]attribute.Value {
	t.Helper()

	for i := range spans {
		if spans[i].Name != name {
			continue
		}

		attrs := spans[i].Attributes

		out := make(map[string]attribute.Value, len(attrs))
		for _, kv := range attrs {
			out[string(kv.Key)] = kv.Value
		}

		return out
	}

	t.Fatalf("span %q not recorded; got %v", name, recordedSpanNames(spans))

	return nil
}

func recordedSpanNames(spans tracetest.SpanStubs) []string {
	out := make([]string, 0, len(spans))
	for i := range spans {
		out = append(out, spans[i].Name)
	}

	return out
}

// tracedStore opens an in-memory store whose spans are recorded, with the aggregate stats sidecar
// enabled so the pushdown path is reachable.
func tracedStore(ctx context.Context, t *testing.T) (*Storage, *tracetest.SpanRecorder) {
	t.Helper()

	rec := tracetest.NewSpanRecorder()

	s, err := Open(ctx, Options{},
		WithBackend(backend.Memory()),
		WithAggregateStats(),
		WithTracerProvider(sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(rec))),
	)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, s.Close(ctx)) })

	_, err = s.WriteMetrics(ctx, buildCorpus(corpusProfile{
		name: "c", series: 8, points: 50, interval: 15_000_000_000, pattern: patCounter,
	}, 1))
	require.NoError(t, err)
	require.NoError(t, mustEngine(s.engineFor("default")).Flush(ctx))

	return s, rec
}

// TestAggregateWindowSpan pins the span the overlapping aggregate emits.
//
// The attributes are the point, not the span: an aggregate read that is slow is slow for one of a
// few reasons — too many parts, too many series, or having fallen off the stats-sidecar pushdown
// onto per-sample decoding — and the span has to say which without a profiler. This path was
// entirely untraced, so a caller saw one opaque duration and nothing else.
func TestAggregateWindowSpan(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	s, rec := tracedStore(ctx, t)

	req := fetch.Request{Start: 0, End: 1 << 62, Matchers: []fetch.Matcher{nameMatcher("bench.metric")}}

	// A window that is a whole multiple of the step takes the fine-grid path.
	const step, window = 30_000_000_000, 300_000_000_000

	got, err := s.AggregateMetricsWindowNamed(ctx, "default", req, engine.WindowSpec{
		Step: step, Window: window,
	})
	require.NoError(t, err)
	require.NotEmpty(t, got)

	attrs := spanAttrs(t, tracetest.SpanStubsFromReadOnlySpans(rec.Ended()), "engine.aggregateWindow")

	assert.EqualValues(t, step, attrs["storage.step"].AsInt64())
	assert.EqualValues(t, window, attrs["storage.window"].AsInt64())
	assert.EqualValues(t, 8, attrs["storage.series_matched"].AsInt64())
	assert.True(t, attrs["storage.window_grid"].AsBool(), "window is a multiple of step")
	assert.Positive(t, attrs["storage.parts_scanned"].AsInt64())
	assert.Positive(t, attrs["storage.series_emitted"].AsInt64())
	assert.Positive(t, attrs["storage.windows_emitted"].AsInt64())
}

// TestAggregateWindowSpanMisalignedFallback checks the span distinguishes the two window paths. A
// window that is not a whole multiple of the step cannot use the fine grid — a window edge falls
// inside a bucket — so it decodes and merges every sample instead. That is the expensive path, and
// telling the two apart from a trace alone is the reason the attribute exists.
func TestAggregateWindowSpanMisalignedFallback(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	s, rec := tracedStore(ctx, t)

	req := fetch.Request{Start: 0, End: 1 << 62, Matchers: []fetch.Matcher{nameMatcher("bench.metric")}}

	// 70s is not a whole multiple of 30s.
	got, err := s.AggregateMetricsWindowNamed(ctx, "default", req, engine.WindowSpec{
		Step: 30_000_000_000, Window: 70_000_000_000,
	})
	require.NoError(t, err)
	require.NotEmpty(t, got)

	attrs := spanAttrs(t, tracetest.SpanStubsFromReadOnlySpans(rec.Ended()), "engine.aggregateWindow")

	assert.False(t, attrs["storage.window_grid"].AsBool(), "misaligned window cannot use the grid")
	assert.False(t, attrs["storage.stats_pushdown"].AsBool(), "the fallback decodes samples")
}

// TestAggregateStepSpan covers the stepped aggregate entry point, which shared the same untraced
// shape as the windowed one.
//
// Both public facades land here: [Storage.AggregateMetricsNamed] is
// [Storage.AggregateMetricsStepNamed] with step 0 (one whole-range bucket per series), so there is
// no separate span for it — worth knowing when reading a trace and looking for one.
func TestAggregateStepSpan(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	s, rec := tracedStore(ctx, t)

	req := fetch.Request{Start: 0, End: 1 << 62, Matchers: []fetch.Matcher{nameMatcher("bench.metric")}}

	_, err := s.AggregateMetricsStepNamed(ctx, "default", req, aggStep)
	require.NoError(t, err)

	attrs := spanAttrs(t, tracetest.SpanStubsFromReadOnlySpans(rec.Ended()), "engine.aggregateStepNamed")

	assert.EqualValues(t, 8, attrs["storage.series_matched"].AsInt64())
	assert.Equal(t, aggStep, attrs["storage.step"].AsInt64())
	assert.Positive(t, attrs["storage.parts_scanned"].AsInt64())
}

// TestAggregateRangeSpan exercises [Engine.AggregateRange] directly: no [Storage] facade reaches it
// (they all route through the stepped form), but it is exported engine API and was untraced too.
func TestAggregateRangeSpan(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	s, rec := tracedStore(ctx, t)

	req := fetch.Request{Start: 0, End: 1 << 62, Matchers: []fetch.Matcher{nameMatcher("bench.metric")}}

	_, err := mustEngine(s.engineFor("default")).AggregateRange(ctx, req)
	require.NoError(t, err)

	attrs := spanAttrs(t, tracetest.SpanStubsFromReadOnlySpans(rec.Ended()), "engine.aggregateRange")

	assert.EqualValues(t, 8, attrs["storage.series_matched"].AsInt64())
	assert.Positive(t, attrs["storage.parts_scanned"].AsInt64())
}

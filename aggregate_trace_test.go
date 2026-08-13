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

// corpusStart is the epoch buildCorpus stamps its first point with; a request that starts after it
// and ends before the last point leaves the flushed part only partially covered.
const corpusStart = int64(1_600_000_000_000_000_000)

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

	// One flushed part, wholly inside an unbounded range, with nothing left in the head.
	assert.Equal(t, "ok", attrs["storage.stats_pushdown_reason"].AsString())
	assert.Zero(t, attrs["storage.stats_pushdown_parts"].AsInt64())

	// A safe plan still decodes here, and the count is why: the whole-part stat only answers a fine
	// bucket the part fits inside, and this part spans 735s of 30s buckets. 8 series x 50 points.
	assert.EqualValues(t, 400, attrs["storage.samples_decoded"].AsInt64())
}

// TestAggregateWindowSpanPushdownPartialCoverage covers the reason an operator meets most often: a
// part that reaches outside the query range cannot be folded from its whole-part stats, so every
// matched series decodes. It is also the reason compaction makes *worse* — the fewer and larger the
// parts, the less likely any one of them is contained in a dashboard's window.
func TestAggregateWindowSpanPushdownPartialCoverage(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	s, rec := tracedStore(ctx, t)

	// The corpus spans corpusStart … corpusStart+735s; ask for a slice of it, so the single part
	// straddles both range ends.
	req := fetch.Request{
		Start:    corpusStart + 100_000_000_000,
		End:      corpusStart + 400_000_000_000,
		Matchers: []fetch.Matcher{nameMatcher("bench.metric")},
	}

	got, err := s.AggregateMetricsWindowNamed(ctx, "default", req, engine.WindowSpec{
		Step: 30_000_000_000, Window: 300_000_000_000,
	})
	require.NoError(t, err)
	require.NotEmpty(t, got)

	attrs := spanAttrs(t, tracetest.SpanStubsFromReadOnlySpans(rec.Ended()), "engine.aggregateWindow")

	assert.True(t, attrs["storage.window_grid"].AsBool(), "the grid itself is usable")
	assert.Equal(t, "partial_coverage", attrs["storage.stats_pushdown_reason"].AsString())
	assert.EqualValues(t, 1, attrs["storage.stats_pushdown_parts"].AsInt64(), "one part straddles the range")
	assert.Positive(t, attrs["storage.samples_decoded"].AsInt64(), "the fallback decodes every sample")
}

// TestAggregateWindowSpanPushdownOverlappingParts covers the other rejection: two parts covering the
// same time range could hold the same timestamp twice, so folding both sidecars could double-count.
// Nothing about the query window fixes this one — it is a property of the store's layout.
func TestAggregateWindowSpanPushdownOverlappingParts(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	s, rec := tracedStore(ctx, t)

	// A second flush of the same timestamps produces a part covering the same span as the first.
	_, err := s.WriteMetrics(ctx, buildCorpus(corpusProfile{
		name: "c", series: 8, points: 50, interval: 15_000_000_000, pattern: patCounter,
	}, 2))
	require.NoError(t, err)
	require.NoError(t, mustEngine(s.engineFor("default")).Flush(ctx))

	req := fetch.Request{Start: 0, End: 1 << 62, Matchers: []fetch.Matcher{nameMatcher("bench.metric")}}

	got, err := s.AggregateMetricsWindowNamed(ctx, "default", req, engine.WindowSpec{
		Step: 30_000_000_000, Window: 300_000_000_000,
	})
	require.NoError(t, err)
	require.NotEmpty(t, got)

	attrs := spanAttrs(t, tracetest.SpanStubsFromReadOnlySpans(rec.Ended()), "engine.aggregateWindow")

	require.EqualValues(t, 2, attrs["storage.parts_scanned"].AsInt64(), "two parts over the same span")
	assert.Equal(t, "overlapping_parts", attrs["storage.stats_pushdown_reason"].AsString())
	assert.EqualValues(t, 1, attrs["storage.stats_pushdown_parts"].AsInt64())
	assert.Positive(t, attrs["storage.samples_decoded"].AsInt64())
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

	spans := tracetest.SpanStubsFromReadOnlySpans(rec.Ended())
	attrs := spanAttrs(t, spans, "engine.aggregateWindow")

	assert.False(t, attrs["storage.window_grid"].AsBool(), "misaligned window cannot use the grid")
	assert.Equal(t, "grid_unusable", attrs["storage.stats_pushdown_reason"].AsString())
	assert.Zero(t, attrs["storage.stats_pushdown_parts"].AsInt64(), "the part layout was never consulted")

	// 8 series x 50 points, all decoded and folded per-sample: the input quantity that explains the
	// duration, which the emitted-window counters do not.
	assert.EqualValues(t, 400, attrs["storage.samples_decoded"].AsInt64())

	// The coarse phase split: planning is its own span, decode and fold are accumulated durations
	// (they interleave per series, so they cannot be contiguous child spans).
	assert.Contains(t, recordedSpanNames(spans), "engine.aggregateWindow.plan")
	assert.Contains(t, attrs, "storage.decode_duration_ms")
	assert.Contains(t, attrs, "storage.fold_duration_ms")
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

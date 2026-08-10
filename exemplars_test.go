package storage

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/signal"
	"github.com/oteldb/storage/signal/exemplar"
	"github.com/oteldb/storage/signal/metric"
)

// exemplarSpec is one exemplar to attach to a point.
type exemplarSpec struct {
	ts      int64
	value   float64
	traceID string
}

// exemplarBatch builds a one-service metrics batch with a single gauge named metricName carrying
// one point per route, each with the given exemplars.
func exemplarBatch(svc, metricName string, ts int64, byRoute map[string][]exemplarSpec) metric.Metrics {
	var md metric.Metrics

	rm := md.AddResource()
	rm.Resource = signal.Resource{Attributes: signal.NewAttributes(
		signal.KeyValue{Key: []byte("service.name"), Value: signal.StringValue([]byte(svc))},
	)}

	sm := rm.AddScope()
	mt := sm.AddMetric()
	mt.Name = []byte(metricName)
	mt.Kind = metric.KindGauge

	for _, route := range slicesSorted(byRoute) {
		p := mt.AddPoint()
		p.Attributes = signal.NewAttributes(
			signal.KeyValue{Key: []byte("http.route"), Value: signal.StringValue([]byte(route))},
		)
		p.Ts = ts
		p.Value = 1

		for _, es := range byRoute[route] {
			e := p.AddExemplar()
			e.Ts = es.ts
			e.Value = es.value
			e.TraceID = []byte(es.traceID)
			e.SpanID = []byte("span0001")
		}
	}

	return md
}

// slicesSorted returns m's keys in sorted order, so batch construction is deterministic.
func slicesSorted(m map[string][]exemplarSpec) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}

	for i := range out {
		for j := i + 1; j < len(out); j++ {
			if out[j] < out[i] {
				out[i], out[j] = out[j], out[i]
			}
		}
	}

	return out
}

// exemplarValues decodes a fetched batch's value column back to float64.
func exemplarValues(b *fetch.Batch) []float64 {
	col, _ := b.Column(exemplar.ColValue)

	out := make([]float64, 0, len(col.Int64))
	for _, v := range col.Int64 {
		out = append(out, exemplar.DecodeValue(v))
	}

	return out
}

func exemplarTraceIDs(b *fetch.Batch) []string {
	col, _ := b.Column(exemplar.ColTraceID)

	out := make([]string, 0, len(col.Bytes))
	for _, v := range col.Bytes {
		out = append(out, string(v))
	}

	return out
}

// The whole point of the design: a metric write stores exemplars, and the *metric's own* label
// matchers select them back out — no exemplar-specific query surface.
func TestFacadeWriteAndQueryExemplars(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	s, err := InMemory()
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close(ctx) })

	acc, err := s.WriteMetrics(ctx, exemplarBatch("api", "http.duration", 1000, map[string][]exemplarSpec{
		"/a": {{ts: 1001, value: 12.5, traceID: "trace-aaaaaaaaaa1"}},
		"/b": {{ts: 1002, value: 7.25, traceID: "trace-bbbbbbbbbb1"}, {ts: 1003, value: 9, traceID: "trace-bbbbbbbbbb2"}},
	}))
	require.NoError(t, err)

	// Exemplar counts stay out of Accepted: it reports data points, and there were two.
	assert.Equal(t, Accepted{Accepted: 2}, acc)

	all := fetch.Request{
		Signal: signal.Exemplar, Start: 0, End: 1 << 60,
		Matchers: []fetch.Matcher{nameMatcherSvc("api")},
	}

	got, err := fetch.Drain(ctx, must(s.ExemplarFetcher("default").Fetch(ctx, all)))
	require.NoError(t, err)
	require.Len(t, got, 2, "one exemplar stream per metric point")

	total := 0
	for _, b := range got {
		total += len(exemplarValues(b))
	}

	assert.Equal(t, 3, total)

	// A matcher on the point's own label narrows to that point's exemplars — the metric series
	// identity is the exemplar stream identity, so this needs no exemplar-specific machinery.
	all.Matchers = append(all.Matchers, labelMatcher([]byte("http.route"), "/a"))
	got, err = fetch.Drain(ctx, must(s.ExemplarFetcher("default").Fetch(ctx, all)))
	require.NoError(t, err)
	require.Len(t, got, 1)
	assert.InDeltaSlice(t, []float64{12.5}, exemplarValues(got[0]), 0)
	assert.Equal(t, []string{"trace-aaaaaaaaaa1"}, exemplarTraceIDs(got[0]))
}

// A matcher on the reserved __name__ label selects one metric's exemplars, exactly as it selects
// that metric's samples.
func TestFacadeExemplarsByMetricName(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	s, err := InMemory()
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close(ctx) })

	for _, name := range []string{"http.duration", "db.duration"} {
		_, err := s.WriteMetrics(ctx, exemplarBatch("api", name, 1000, map[string][]exemplarSpec{
			"/a": {{ts: 1001, value: 1, traceID: "trace-" + name}},
		}))
		require.NoError(t, err)
	}

	got, err := fetch.Drain(ctx, must(s.ExemplarFetcher("default").Fetch(ctx, fetch.Request{
		Signal: signal.Exemplar, Start: 0, End: 1 << 60,
		Matchers: []fetch.Matcher{nameMatcherSvc("api"), labelMatcher(metric.LabelName, "db.duration")},
	})))
	require.NoError(t, err)
	require.Len(t, got, 1)
	assert.Equal(t, []string{"trace-db.duration"}, exemplarTraceIDs(got[0]))
}

// ExemplarsForTrace completes the correlation triangle: given a trace id, find the metric
// exemplars recorded against it.
func TestFacadeExemplarsForTrace(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	s, err := InMemory()
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close(ctx) })

	_, err = s.WriteMetrics(ctx, exemplarBatch("api", "http.duration", 1000, map[string][]exemplarSpec{
		"/a": {{ts: 1001, value: 12.5, traceID: "trace-aaaaaaaaaa1"}},
		"/b": {{ts: 1002, value: 7.25, traceID: "trace-bbbbbbbbbb1"}},
	}))
	require.NoError(t, err)

	got, err := s.ExemplarsForTrace(ctx, "default", []byte("trace-bbbbbbbbbb1"))
	require.NoError(t, err)

	var values []float64

	for _, b := range got {
		for i, id := range exemplarTraceIDs(b) {
			if id == "trace-bbbbbbbbbb1" {
				values = append(values, exemplarValues(b)[i])
			}
		}
	}

	assert.InDeltaSlice(t, []float64{7.25}, values, 0)

	// An unknown trace id yields nothing (and prunes on the bloom rather than erroring).
	got, err = s.ExemplarsForTrace(ctx, "default", []byte("trace-zzzzzzzzzz9"))
	require.NoError(t, err)

	for _, b := range got {
		for _, id := range exemplarTraceIDs(b) {
			assert.NotEqual(t, "trace-zzzzzzzzzz9", id)
		}
	}
}

// A metrics batch with no exemplars must not create an exemplar engine at all — the common case
// takes the cheap structural scan and never touches the record path.
func TestFacadeNoExemplarsNoEngine(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	s, err := InMemory()
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close(ctx) })

	_, err = s.WriteMetrics(ctx, exemplarBatch("api", "http.duration", 1000, map[string][]exemplarSpec{
		"/a": nil,
	}))
	require.NoError(t, err)

	_, ok := s.lookupExemplarEngine("default")
	assert.False(t, ok, "no exemplar engine is created for a batch with no exemplars")
}

// Exemplars survive a flush to parts, not just the head.
func TestFacadeExemplarsSurviveFlush(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	s, err := InMemory()
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close(ctx) })

	_, err = s.WriteMetrics(ctx, exemplarBatch("api", "http.duration", 1000, map[string][]exemplarSpec{
		"/a": {{ts: 1001, value: 12.5, traceID: "trace-aaaaaaaaaa1"}},
	}))
	require.NoError(t, err)

	eng, ok := s.lookupExemplarEngine("default")
	require.True(t, ok)
	require.NoError(t, eng.Flush(ctx))

	got, err := fetch.Drain(ctx, must(s.ExemplarFetcher("default").Fetch(ctx, fetch.Request{
		Signal: signal.Exemplar, Start: 0, End: 1 << 60,
		Matchers: []fetch.Matcher{nameMatcherSvc("api")},
	})))
	require.NoError(t, err)
	require.Len(t, got, 1)
	assert.InDeltaSlice(t, []float64{12.5}, exemplarValues(got[0]), 0)
	assert.Equal(t, []string{"trace-aaaaaaaaaa1"}, exemplarTraceIDs(got[0]))
}

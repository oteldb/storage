package storage

import (
	"bytes"
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/index/bloom"
	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/signal"
	"github.com/oteldb/storage/signal/trace"
)

type spanSpec struct {
	traceID, spanID, parent, name string
	start, end                    int64
	status                        int32
}

// traceBatch builds a one-service Traces batch.
func traceBatch(svc string, spans ...spanSpec) trace.Traces {
	var td trace.Traces
	rs := td.AddResource()
	rs.Resource = signal.Resource{Attributes: signal.NewAttributes(
		signal.KeyValue{Key: []byte("service.name"), Value: signal.StringValue([]byte(svc))},
	)}
	ss := rs.AddScope()

	for _, sp := range spans {
		s := ss.AddSpan()
		s.TraceID, s.SpanID, s.ParentSpanID = []byte(sp.traceID), []byte(sp.spanID), []byte(sp.parent)
		s.Name = []byte(sp.name)
		s.Start, s.End = sp.start, sp.end
		s.StatusCode = sp.status
	}

	return td
}

func nameMatcherSvc(svc string) fetch.Matcher {
	want := []byte(svc)

	return fetch.Matcher{Name: []byte("service.name"), Match: func(v signal.Value) bool { return bytes.Equal(v.Str(), want) }}
}

func durationAtLeast(d int64) fetch.Condition {
	return fetch.Condition{Column: trace.ColDuration, Match: func(v signal.Value) bool { return v.Int() >= d }}
}

func spanNameContains(sub string) fetch.Condition {
	want := []byte(sub)

	return fetch.Condition{
		Column: trace.ColName,
		Match:  func(v signal.Value) bool { return bytes.Contains(v.Str(), want) },
		Tokens: bloom.Tokenize(nil, want),
	}
}

// spanNames extracts the name column of a batch.
func spanNames(b *fetch.Batch) []string {
	col, _ := b.Column(trace.ColName)
	out := make([]string, len(col.Bytes))
	for i, v := range col.Bytes {
		out[i] = string(v)
	}

	return out
}

func TestFacadeWriteAndQueryTraces(t *testing.T) {
	t.Parallel()

	s, err := InMemory()
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close(context.Background()) })

	acc, err := s.WriteTraces(context.Background(), traceBatch("api",
		spanSpec{traceID: "t1", spanID: "s1", name: "GET /a", start: 100, end: 250, status: 1},
		spanSpec{traceID: "t1", spanID: "s2", parent: "s1", name: "db.query", start: 120, end: 130},
	))
	require.NoError(t, err)
	assert.Equal(t, Accepted{Accepted: 2}, acc)

	// Filter spans by duration ≥ 100 (only s1: 150ns).
	r := fetch.Request{
		Signal: signal.Trace, Start: 0, End: 1 << 60,
		Matchers: []fetch.Matcher{nameMatcherSvc("api")}, Conditions: []fetch.Condition{durationAtLeast(100)}, AllConditions: true,
	}
	it, err := s.TraceFetcher("default").Fetch(context.Background(), r)
	require.NoError(t, err)
	got, err := fetch.Drain(context.Background(), it)
	require.NoError(t, err)
	require.Len(t, got, 1)
	assert.Equal(t, []string{"GET /a"}, spanNames(got[0]))

	// Full-text on span name.
	r.Conditions = []fetch.Condition{spanNameContains("db")}
	it, err = s.TraceFetcher("default").Fetch(context.Background(), r)
	require.NoError(t, err)
	got, err = fetch.Drain(context.Background(), it)
	require.NoError(t, err)
	require.Len(t, got, 1)
	assert.Equal(t, []string{"db.query"}, spanNames(got[0]))
}

func TestFacadeTraceByID(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s, err := InMemory()
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close(ctx) })

	// One trace spanning two services, plus an unrelated trace.
	_, err = s.WriteTraces(ctx, traceBatch("frontend",
		spanSpec{traceID: "trace-A", spanID: "root", name: "GET /", start: 100, end: 900}))
	require.NoError(t, err)
	_, err = s.WriteTraces(ctx, traceBatch("backend",
		spanSpec{traceID: "trace-A", spanID: "child", parent: "root", name: "rpc", start: 200, end: 400}))
	require.NoError(t, err)
	_, err = s.WriteTraces(ctx, traceBatch("frontend",
		spanSpec{traceID: "trace-B", spanID: "x", name: "GET /other", start: 100, end: 200}))
	require.NoError(t, err)

	// Flush so the trace_id equality bloom prunes parts on the by-id lookup.
	s.maintain(ctx)

	got, err := s.Trace(ctx, "default", []byte("trace-A"))
	require.NoError(t, err)

	names := make([]string, 0, 2)
	for _, b := range got {
		names = append(names, spanNames(b)...)
	}

	assert.ElementsMatch(t, []string{"GET /", "rpc"}, names, "trace-by-id returns the trace's spans across services, not trace-B")
}

func TestFacadeTracesLogsMetricsCoexist(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s, err := InMemory()
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close(ctx) })

	_, err = s.WriteTraces(ctx, traceBatch("api", spanSpec{traceID: "t", spanID: "s", name: "op", start: 1, end: 2}))
	require.NoError(t, err)
	_, err = s.WriteLogs(ctx, logBatch("api", [3]any{100, 9, "log line"}))
	require.NoError(t, err)
	_, err = s.WriteMetrics(ctx, gaugeBatch("api", "http.requests", []int64{100}, []float64{1}))
	require.NoError(t, err)

	drain := func(f fetch.Fetcher, r fetch.Request) []*fetch.Batch {
		it, ferr := f.Fetch(ctx, r)
		require.NoError(t, ferr)
		out, derr := fetch.Drain(ctx, it)
		require.NoError(t, derr)

		return out
	}

	all := fetch.Request{Start: 0, End: 1 << 60}
	traces := drain(s.TraceFetcher("default"), all)
	require.Len(t, traces, 1)
	assert.Equal(t, []string{"op"}, spanNames(traces[0]))

	require.Len(t, drain(s.LogFetcher("default"), all), 1)

	metrics := drain(s.Fetcher("default"), fetch.Request{
		Start: 0, End: 1 << 60, Matchers: []fetch.Matcher{nameMatcher("http.requests")},
	})
	require.Len(t, metrics, 1)
	assert.Equal(t, []float64{1}, metrics[0].Values)
}

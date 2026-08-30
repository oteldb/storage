package storage

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/signal"
	"github.com/oteldb/storage/signal/log"
	"github.com/oteldb/storage/signal/trace"
)

func columnValues(t *testing.T, s *Storage, req ValuesRequest) []string {
	t.Helper()

	got, err := s.ColumnValues(context.Background(), "default", req)
	require.NoError(t, err)

	out := make([]string, len(got))
	for i, v := range got {
		out[i] = string(v)
	}

	return out
}

// The TraceQL-intrinsics case: span names answered from the column dictionary, not a window scan.
func TestFacadeColumnValuesSpanNames(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s, err := InMemory()
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close(ctx) })

	_, err = s.WriteTraces(ctx, traceBatch("api",
		spanSpec{traceID: "t1", spanID: "s1", name: "GET /users", start: 100, end: 150},
		spanSpec{traceID: "t1", spanID: "s2", parent: "s1", name: "db.query", start: 110, end: 120},
		spanSpec{traceID: "t2", spanID: "s3", name: "GET /users", start: 200, end: 260},
	))
	require.NoError(t, err)

	assert.Equal(t, []string{"GET /users", "db.query"},
		columnValues(t, s, ValuesRequest{Signal: signal.Trace, Column: trace.ColName}))
	assert.Equal(t, []string{"GET /users"},
		columnValues(t, s, ValuesRequest{Signal: signal.Trace, Column: trace.ColName, Start: 190, End: 300}))
	assert.Equal(t, []string{"GET /users"},
		columnValues(t, s, ValuesRequest{Signal: signal.Trace, Column: trace.ColName, Limit: 1}))
}

// The Loki label-values case: per-record log attribute values, which no fixed column exposes.
func TestFacadeColumnValuesLogAttrs(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s, err := InMemory()
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close(ctx) })

	var ld log.Logs
	rl := ld.AddResource()
	rl.Resource = signal.Resource{Attributes: signal.NewAttributes(
		signal.KeyValue{Key: []byte("service.name"), Value: signal.StringValue([]byte("api"))},
	)}
	sl := rl.AddScope()

	for i, method := range []string{"GET", "POST", "GET"} {
		rec := sl.AddRecord()
		rec.Timestamp = int64(100 * (i + 1))
		rec.Body = []byte("hello")
		rec.Attributes = signal.NewAttributes(
			signal.KeyValue{Key: []byte("http.method"), Value: signal.StringValue([]byte(method))},
			signal.KeyValue{Key: []byte("http.status_code"), Value: signal.IntValue(int64(200 + i))},
		)
	}

	_, err = s.WriteLogs(ctx, ld)
	require.NoError(t, err)

	assert.Equal(t, []string{"GET", "POST"},
		columnValues(t, s, ValuesRequest{Signal: signal.Log, AttrKey: []byte("http.method")}))
	// Non-string values project through the same canonical text form the matching layer compares.
	assert.Equal(t, []string{"200", "201", "202"},
		columnValues(t, s, ValuesRequest{Signal: signal.Log, AttrKey: []byte("http.status_code")}))
	assert.Empty(t, columnValues(t, s, ValuesRequest{Signal: signal.Log, AttrKey: []byte("absent")}))
}

func TestFacadeColumnValuesUnknownTenant(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s, err := InMemory()
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close(ctx) })

	got, err := s.ColumnValues(ctx, "nobody", ValuesRequest{Signal: signal.Log, Column: log.ColBody})
	require.NoError(t, err)
	assert.Empty(t, got)
}

func TestFacadeColumnValuesRejectsNonRecordSignal(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s, err := InMemory()
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close(ctx) })

	_, err = s.ColumnValues(ctx, "default", ValuesRequest{Signal: signal.Metric, Column: "value"})
	require.Error(t, err)
}

func TestFacadeColumnValuesClosed(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s, err := InMemory()
	require.NoError(t, err)
	require.NoError(t, s.Close(ctx))

	_, err = s.ColumnValues(ctx, "default", ValuesRequest{Signal: signal.Log, Column: log.ColBody})
	require.ErrorIs(t, err, ErrClosed)
}

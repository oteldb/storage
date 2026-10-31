package otlpdecode_test

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/ptrace"

	"github.com/oteldb/storage"
	"github.com/oteldb/storage/backend/file"
	"github.com/oteldb/storage/otlp/otlpdecode"
	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/signal"
	"github.com/oteldb/storage/signal/trace"
)

func TestDecodedTracesSurviveInputReuseAndReopen(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	root := t.TempDir()
	be, err := file.New(filepath.Join(root, "parts"))
	require.NoError(t, err)
	s, err := storage.Open(ctx, storage.Options{Backend: be, WALDir: filepath.Join(root, "wal"), FlushInterval: int64(time.Hour)})
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close(context.WithoutCancel(ctx)) })

	td := ptrace.NewTraces()
	rs := td.ResourceSpans().AppendEmpty()
	rs.Resource().Attributes().PutStr("service.name", "decoder")
	span := rs.ScopeSpans().AppendEmpty().Spans().AppendEmpty()
	span.SetTraceID([16]byte{1})
	span.SetSpanID([8]byte{2})
	span.SetName("GET /orders")
	span.SetStartTimestamp(100)
	span.SetEndTimestamp(200)
	raw := marshalTraces(t, td)
	var converter otlpdecode.TracesConverter
	batch, err := converter.Convert(raw)
	require.NoError(t, err)
	accepted, err := s.WriteTraces(ctx, *batch)
	require.NoError(t, err)
	require.Equal(t, int64(1), accepted.Accepted)
	clear(raw)
	require.NoError(t, s.Close(ctx))

	reopened, err := storage.Open(ctx, storage.Options{Backend: be, ReadOnly: true})
	require.NoError(t, err)
	t.Cleanup(func() { _ = reopened.Close(context.WithoutCancel(ctx)) })
	it, err := reopened.TraceFetcher("default").Fetch(ctx, fetch.Request{Start: 0, End: 1000,
		Projection: []string{trace.ColName, trace.ColTraceID}, Matchers: []fetch.Matcher{{Name: []byte("service.name"),
			Match: func(value signal.Value) bool { return string(value.Str()) == "decoder" }}}})
	require.NoError(t, err)
	batches, err := fetch.Drain(ctx, it)
	require.NoError(t, err)
	require.Len(t, batches, 1)
	names, ok := batches[0].Column(trace.ColName)
	require.True(t, ok)
	require.Equal(t, [][]byte{[]byte("GET /orders")}, names.Bytes)
	ids, ok := batches[0].Column(trace.ColTraceID)
	require.True(t, ok)
	require.Equal(t, []byte{1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}, ids.Bytes[0])
}

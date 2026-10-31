package recordengine_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/internal/obs"
	"github.com/oteldb/storage/recordengine"
)

func TestKeysContext(t *testing.T) {
	t.Parallel()
	for _, flushed := range []bool{false, true} {
		name := "head"
		if flushed {
			name = "flushed"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			rec := tracetest.NewSpanRecorder()
			tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(rec))
			t.Cleanup(func() { require.NoError(t, tp.Shutdown(context.Background())) })
			o, err := obs.New(obs.Config{TracerProvider: tp})
			require.NoError(t, err)
			e := recordengine.New(recordengine.Config{Schema: testSchema, Backend: backend.Memory(), Prefix: "t/recs", Obs: o})
			ingest(t, e, mkScopedBatch("api", rrec{ts: 100, attr: [2]string{"method", "GET"}}))
			ingest(t, e, mkScopedBatch("worker", rrec{ts: 200, attr: [2]string{"task", "sync"}}))
			if flushed {
				require.NoError(t, e.Flush(t.Context()))
			}
			for _, window := range []struct {
				name       string
				start, end int64
			}{
				{name: "all"}, {name: "partial", start: 50, end: 150}, {name: "empty", start: 500, end: 600},
			} {
				t.Run(window.name, func(t *testing.T) {
					t.Parallel()
					want := e.Keys(window.start, window.end)
					ctx, parent := tp.Tracer("test").Start(t.Context(), "query")
					got := e.KeysContext(ctx, window.start, window.end)
					parent.End()
					require.Equal(t, want, got)
					spans := map[string]sdktrace.ReadOnlySpan{}
					for _, s := range rec.Ended() {
						if s.SpanContext().TraceID() == parent.SpanContext().TraceID() {
							spans[s.Name()] = s
						}
					}
					require.Len(t, spans, 7)
					root := spans["recordengine.keys"]
					require.NotNil(t, root)
					require.Equal(t, parent.SpanContext().SpanID(), root.Parent().SpanID())
					for _, stage := range []string{"lock", "streams", "records", "parts", "sort"} {
						s := spans["recordengine.keys."+stage]
						require.NotNil(t, s)
						require.Equal(t, root.SpanContext().SpanID(), s.Parent().SpanID())
					}
					attrs := map[attribute.Key]attribute.Value{}
					for _, kv := range root.Attributes() {
						attrs[kv.Key] = kv.Value
					}
					require.EqualValues(t, 2, attrs["storage.streams"].AsInt64())
					require.EqualValues(t, len(got), attrs["storage.keys"].AsInt64())
					parts := int64(0)
					if flushed {
						parts = 1
					}
					require.Equal(t, parts, attrs["storage.parts_scanned"].AsInt64())
				})
			}
		})
	}
}

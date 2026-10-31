package storage

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	"github.com/oteldb/storage/backend/bucketindex"
	"github.com/oteldb/storage/cluster"
	"github.com/oteldb/storage/signal"
)

func TestKeysTraceBoundaries(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		name                              string
		peer, missing, incomplete, closed bool
	}{
		{name: "facade"}, {name: "peer", peer: true},
		{name: "missing", missing: true}, {name: "peer absent", peer: true, missing: true},
		{name: "incomplete", incomplete: true}, {name: "peer incomplete", peer: true, incomplete: true},
		{name: "closed", closed: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			rec := tracetest.NewSpanRecorder()
			tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(rec))
			t.Cleanup(func() { require.NoError(t, tp.Shutdown(context.Background())) })
			s, err := InMemory(WithTracerProvider(tp))
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, s.Close(context.Background())) })
			if !tt.missing {
				_, err := s.WriteLogs(t.Context(), logBatchWithAttrs("api", [4]any{100, 9, "line", "method"}))
				require.NoError(t, err)
			}
			if tt.incomplete {
				e, ok := s.lookupLogEngine("default")
				require.True(t, ok)
				e.AdoptWants([]bucketindex.Want{{Prefix: "lost", MinTime: 0, MaxTime: 200}})
			}
			if tt.closed {
				require.NoError(t, s.Close(t.Context()))
			}
			ctx, parent := tp.Tracer("test").Start(t.Context(), "query")
			rootName := "storage.keys"
			if tt.peer {
				rootName = "storage.keys.local"
				_, err = s.localKeys(ctx, signal.Log, "default", 0, 0)
			} else {
				_, err = s.LogKeys(ctx, "default", 0, 0)
			}
			parent.End()
			switch {
			case tt.closed:
				require.ErrorIs(t, err, ErrClosed)
			case tt.incomplete:
				require.ErrorIs(t, err, cluster.ErrShardIncomplete)
			case tt.peer && tt.missing:
				require.ErrorIs(t, err, cluster.ErrShardAbsent)
			default:
				require.NoError(t, err)
			}
			spans := map[string]sdktrace.ReadOnlySpan{}
			for _, span := range rec.Ended() {
				if span.SpanContext().TraceID() == parent.SpanContext().TraceID() {
					spans[span.Name()] = span
				}
			}
			root := spans[rootName]
			require.NotNil(t, root)
			require.Equal(t, parent.SpanContext().SpanID(), root.Parent().SpanID())
			if tt.incomplete || tt.closed {
				require.Equal(t, codes.Error, root.Status().Code)
			} else {
				require.Equal(t, codes.Unset, root.Status().Code)
			}
			if !tt.closed {
				lookup := spans["storage.keys.lookup"]
				require.NotNil(t, lookup)
				require.Equal(t, root.SpanContext().SpanID(), lookup.Parent().SpanID())
			}
			if !tt.closed && (!tt.missing || tt.peer) {
				gap := spans["storage.keys.gapCheck"]
				require.NotNil(t, gap)
				require.Equal(t, root.SpanContext().SpanID(), gap.Parent().SpanID())
				if tt.incomplete {
					require.Equal(t, codes.Error, gap.Status().Code)
				}
			}
			if !tt.closed && !tt.incomplete && !tt.missing {
				engine := spans["recordengine.keys"]
				require.NotNil(t, engine)
				require.Equal(t, root.SpanContext().SpanID(), engine.Parent().SpanID())
			} else {
				require.NotContains(t, spans, "recordengine.keys")
			}
		})
	}
}

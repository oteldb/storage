package storage

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/signal"
	"github.com/oteldb/storage/signal/log"
)

// logBatchWithAttrs builds a one-stream Logs batch where each record carries one attribute, used to
// exercise record-scope key enumeration (the gap LogKeys closes over LogSeries).
func logBatchWithAttrs(svc string, recs ...[4]any) log.Logs {
	var ld log.Logs
	rl := ld.AddResource()
	rl.Resource = signal.Resource{Attributes: signal.NewAttributes(
		signal.KeyValue{Key: []byte("service.name"), Value: signal.StringValue([]byte(svc))},
	)}
	sl := rl.AddScope()
	sl.Scope = signal.Scope{Name: []byte("lib")}

	for _, r := range recs {
		rec := sl.AddRecord()
		rec.Timestamp = int64(r[0].(int))
		rec.SeverityNumber = int32(r[1].(int))
		rec.Body = []byte(r[2].(string))

		if key := r[3].(string); key != "" {
			rec.Attributes = signal.NewAttributes(
				signal.KeyValue{Key: []byte(key), Value: signal.StringValue([]byte("v"))},
			)
		}
	}

	return ld
}

func logKeyScopes(keys []KeyInfo) map[string]KeyScope {
	out := make(map[string]KeyScope, len(keys))
	for _, k := range keys {
		out[string(k.Key)] = k.Scope
	}

	return out
}

func TestFacadeLogKeys(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s, err := InMemory()
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close(ctx) })

	_, err = s.WriteLogs(ctx, logBatchWithAttrs("api",
		[4]any{100, 9, "first", "http.method"},
		[4]any{200, 17, "second", "http.status_code"},
	))
	require.NoError(t, err)

	keys, err := s.LogKeys(ctx, "default", 0, 0)
	require.NoError(t, err)

	got := logKeyScopes(keys)
	assert.Equal(t, KeyScopeResource, got["service.name"], "resource attribute (a stream label)")
	assert.Equal(t, KeyScopeScope, got["otel.scope.name"], "scope name is a stream label")
	assert.Equal(t, KeyScopeRecord, got["http.method"], "record attribute — invisible to LogSeries")
	assert.Equal(t, KeyScopeRecord, got["http.status_code"])
}

func TestFacadeLogKeysWindowAndFlush(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s, err := InMemory()
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close(ctx) })

	_, err = s.WriteLogs(ctx, logBatchWithAttrs("api", [4]any{100, 9, "old", "early.key"}))
	require.NoError(t, err)
	_, err = s.WriteLogs(ctx, logBatchWithAttrs("api", [4]any{500, 9, "new", "late.key"}))
	require.NoError(t, err)

	keys, err := s.LogKeys(ctx, "default", 400, 600)
	require.NoError(t, err)

	got := logKeyScopes(keys)
	assert.Contains(t, got, "late.key")
	assert.NotContains(t, got, "early.key", "ts=100 record is outside the window")
}

func TestFacadeLogKeysUnknownTenant(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s, err := InMemory()
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close(ctx) })

	keys, err := s.LogKeys(ctx, "nobody", 0, 0)
	require.NoError(t, err)
	assert.Empty(t, keys)
}

func TestFacadeLogKeysClosed(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s, err := InMemory()
	require.NoError(t, err)
	require.NoError(t, s.Close(ctx))

	_, err = s.LogKeys(ctx, "default", 0, 0)
	require.ErrorIs(t, err, ErrClosed)
}

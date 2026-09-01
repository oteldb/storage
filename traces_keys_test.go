package storage

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/signal"
	"github.com/oteldb/storage/signal/trace"
)

// traceBatchWithAttrs builds a one-stream Traces batch where each span carries one attribute, used
// to exercise span-scope key enumeration (the gap TraceKeys closes over TraceSeries).
func traceBatchWithAttrs(svc string, spans ...spanAttrSpec) trace.Traces {
	var td trace.Traces
	rs := td.AddResource()
	rs.Resource = signal.Resource{Attributes: signal.NewAttributes(
		signal.KeyValue{Key: []byte("service.name"), Value: signal.StringValue([]byte(svc))},
	)}
	ss := rs.AddScope()
	ss.Scope = signal.Scope{Name: []byte("lib")}

	for _, sp := range spans {
		s := ss.AddSpan()
		s.TraceID, s.SpanID = []byte(sp.traceID), []byte(sp.spanID)
		s.Name = []byte(sp.name)
		s.Start, s.End = sp.start, sp.start+1

		if sp.attrKey != "" {
			s.Attributes = signal.NewAttributes(
				signal.KeyValue{Key: []byte(sp.attrKey), Value: signal.StringValue([]byte("v"))},
			)
		}
	}

	return td
}

type spanAttrSpec struct {
	traceID, spanID, name, attrKey string
	start                          int64
}

func traceKeyScopes(keys []KeyInfo) map[string]KeyScope {
	out := make(map[string]KeyScope, len(keys))
	for _, k := range keys {
		out[string(k.Key)] = k.Scope
	}

	return out
}

func TestFacadeTraceKeys(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s, err := InMemory()
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close(ctx) })

	_, err = s.WriteTraces(ctx, traceBatchWithAttrs("api",
		spanAttrSpec{traceID: "t1", spanID: "s1", name: "GET /", start: 100, attrKey: "http.method"},
		spanAttrSpec{traceID: "t1", spanID: "s2", name: "db", start: 200, attrKey: "db.system"},
	))
	require.NoError(t, err)

	keys, err := s.TraceKeys(ctx, "default", 0, 0)
	require.NoError(t, err)

	got := traceKeyScopes(keys)
	assert.Equal(t, KeyScopeResource, got["service.name"], "resource attribute (a stream label)")
	assert.Equal(t, KeyScopeScope, got["otel.scope.name"], "scope name is a stream label")
	assert.Equal(t, KeyScopeRecord, got["http.method"], "span attribute — invisible to TraceSeries")
	assert.Equal(t, KeyScopeRecord, got["db.system"])
}

func TestFacadeTraceKeysWindow(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s, err := InMemory()
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close(ctx) })

	_, err = s.WriteTraces(ctx, traceBatchWithAttrs("api",
		spanAttrSpec{traceID: "t1", spanID: "s1", name: "old", start: 100, attrKey: "early.key"},
	))
	require.NoError(t, err)
	_, err = s.WriteTraces(ctx, traceBatchWithAttrs("api",
		spanAttrSpec{traceID: "t2", spanID: "s2", name: "new", start: 500, attrKey: "late.key"},
	))
	require.NoError(t, err)

	keys, err := s.TraceKeys(ctx, "default", 400, 600)
	require.NoError(t, err)

	got := traceKeyScopes(keys)
	assert.Contains(t, got, "late.key")
	assert.NotContains(t, got, "early.key", "start=100 span is outside the window")
}

func TestFacadeTraceKeysIsolatedFromLogs(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s, err := InMemory()
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close(ctx) })

	_, err = s.WriteLogs(ctx, logBatchWithAttrs("api", [4]any{100, 9, "log", "log.only.key"}))
	require.NoError(t, err)
	_, err = s.WriteTraces(ctx, traceBatchWithAttrs("api",
		spanAttrSpec{traceID: "t1", spanID: "s1", name: "span", start: 100, attrKey: "span.only.key"},
	))
	require.NoError(t, err)

	traceKeys, err := s.TraceKeys(ctx, "default", 0, 0)
	require.NoError(t, err)
	assert.Contains(t, traceKeyScopes(traceKeys), "span.only.key")
	assert.NotContains(t, traceKeyScopes(traceKeys), "log.only.key", "TraceKeys reads the traces engine")

	logKeys, err := s.LogKeys(ctx, "default", 0, 0)
	require.NoError(t, err)
	assert.Contains(t, logKeyScopes(logKeys), "log.only.key")
	assert.NotContains(t, logKeyScopes(logKeys), "span.only.key")
}

func TestFacadeTraceKeysUnknownTenant(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s, err := InMemory()
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close(ctx) })

	keys, err := s.TraceKeys(ctx, "nobody", 0, 0)
	require.NoError(t, err)
	assert.Empty(t, keys)
}

func TestFacadeTraceKeysClosed(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s, err := InMemory()
	require.NoError(t, err)
	require.NoError(t, s.Close(ctx))

	_, err = s.TraceKeys(ctx, "default", 0, 0)
	require.ErrorIs(t, err, ErrClosed)
}

// TestClusteredTraceKeysFansOut covers the TraceKeys cluster fan-out: a non-owner, holding none of
// a tenant's span data, serves the key enumeration from an owner over HTTP.
//
//nolint:paralleltest // owns an embedded etcd; runs serially
func TestClusteredTraceKeysFansOut(t *testing.T) {
	endpoint := startEtcd(t)
	ctx := context.Background()

	nodes := map[string]*Storage{
		"node-a": openClusterNode(t, endpoint, "node-a"),
		"node-b": openClusterNode(t, endpoint, "node-b"),
		"node-c": openClusterNode(t, endpoint, "node-c"),
	}
	a := nodes["node-a"]

	awaitMembership(t, nodes)

	_, err := a.WriteTraces(ctx, traceBatchWithAttrs("api",
		spanAttrSpec{traceID: "t1", spanID: "s1", name: "GET /", start: 100, attrKey: "http.method"},
		spanAttrSpec{traceID: "t1", spanID: "s2", name: "db", start: 200, attrKey: "db.system"},
	))
	require.NoError(t, err)

	owners := a.cluster.membership.Ring().Lookup([]byte("default"), 2)
	require.Len(t, owners, 2)
	ownerID := map[string]bool{owners[0].ID: true, owners[1].ID: true}

	var nonOwner *Storage
	var nonOwnerName string
	for name, s := range nodes {
		if !ownerID[name] {
			nonOwner, nonOwnerName = s, name
		}
	}
	require.NotNil(t, nonOwner, "one node is not an owner")

	_, hasLocal := nonOwner.lookupTraceEngine("default")
	require.Falsef(t, hasLocal, "%s (non-owner) has no local trace engine", nonOwnerName)

	keys, err := nonOwner.TraceKeys(ctx, "default", 0, 0)
	require.NoError(t, err)

	got := traceKeyScopes(keys)
	assert.Equal(t, KeyScopeResource, got["service.name"], "resource attribute (a stream label)")
	assert.Equal(t, KeyScopeRecord, got["http.method"], "span attribute served via fan-out")
	assert.Equal(t, KeyScopeRecord, got["db.system"])
}

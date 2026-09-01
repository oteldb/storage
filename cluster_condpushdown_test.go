package storage

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/cluster"
	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/signal"
	"github.com/oteldb/storage/signal/trace"
)

// traceIDCondition is the predicate a trace-by-id lookup carries: an equality on the trace_id
// column and no identity matcher at all (see [Storage.fetchByEquality]).
func traceIDCondition(id string) fetch.Condition {
	want := []byte(id)

	return fetch.Condition{
		Column: trace.ColTraceID,
		Match:  func(v signal.Value) bool { return bytes.Equal(v.Str(), want) },
		Equal:  &fetch.EqualMatcher{Name: trace.ColTraceID, Value: id},
	}
}

// servePeerFetch mounts s behind the cluster read RPC and returns a fetcher over it, the way a
// non-owner node reads a shard an owner holds.
func servePeerFetch(t *testing.T, s *Storage, sig signal.Signal) fetch.Fetcher {
	t.Helper()

	mux := http.NewServeMux()
	mux.Handle(cluster.ReadPath, cluster.NewReadHandler(s.serveFetch))
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	return cluster.NewRemoteFetcher(sig, strings.TrimPrefix(srv.URL, "http://"), srv.Client())
}

// TestPeerFetchPrunesByCondition pins the routed trace-by-id fix (#447): the whole predicate is a
// columnar condition over an unbounded window, so a peer that never sees it answers with every span
// it holds. The requester still narrows to the right trace, so the symptom is cost, not wrong rows —
// which is why this asserts on what the peer *shipped*, not only on the final answer.
func TestPeerFetchPrunesByCondition(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	s, err := InMemory()
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close(ctx) })

	_, err = s.WriteTraces(ctx, traceBatch("frontend",
		spanSpec{traceID: "trace-A", spanID: "root", name: "GET /", start: 100, end: 900}))
	require.NoError(t, err)
	_, err = s.WriteTraces(ctx, traceBatch("backend",
		spanSpec{traceID: "trace-A", spanID: "child", parent: "root", name: "rpc", start: 200, end: 400}))
	require.NoError(t, err)

	// The rest of the tenant's history: what an unpushed condition drags across the wire.
	noise := make([]spanSpec, 0, 256)
	for i := range cap(noise) {
		noise = append(noise, spanSpec{
			traceID: "trace-B" + strconv.Itoa(i), spanID: "x" + strconv.Itoa(i),
			name: "GET /other", start: int64(i), end: int64(i) + 10,
		})
	}

	_, err = s.WriteTraces(ctx, traceBatch("frontend", noise...))
	require.NoError(t, err)

	// Flush so the trace_id equality bloom can prune parts, as it does on the local path.
	s.maintain(ctx)

	it, err := servePeerFetch(t, s, signal.Trace).Fetch(ctx, fetch.Request{
		Signal: signal.Trace, Tenant: "default", Start: 0, End: 1<<63 - 1,
		Conditions: []fetch.Condition{traceIDCondition("trace-A")}, AllConditions: true,
	})
	require.NoError(t, err)

	got, err := fetch.Drain(ctx, it)
	require.NoError(t, err)

	var (
		names = make([]string, 0, 2)
		rows  int
	)

	for _, b := range got {
		names = append(names, spanNames(b)...)
		rows += len(b.Timestamps)
	}

	assert.ElementsMatch(t, []string{"GET /", "rpc"}, names,
		"the peer answers with the trace, not its whole span history")
	assert.Equal(t, 2, rows, "no unrelated span crosses the wire")
}

// TestPeerFetchKeepsAdvisoryConditions: without AllConditions a fetcher may ignore the conditions
// entirely, so they are not pushed — the peer answers with the window and the caller filters.
func TestPeerFetchKeepsAdvisoryConditions(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	s, err := InMemory()
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close(ctx) })

	_, err = s.WriteTraces(ctx, traceBatch("frontend",
		spanSpec{traceID: "trace-A", spanID: "root", name: "GET /", start: 100, end: 900},
		spanSpec{traceID: "trace-B", spanID: "x", name: "GET /other", start: 100, end: 200}))
	require.NoError(t, err)

	s.maintain(ctx)

	it, err := servePeerFetch(t, s, signal.Trace).Fetch(ctx, fetch.Request{
		Signal: signal.Trace, Tenant: "default", Start: 0, End: 1<<63 - 1,
		Conditions: []fetch.Condition{traceIDCondition("trace-A")},
	})
	require.NoError(t, err)

	got, err := fetch.Drain(ctx, it)
	require.NoError(t, err)

	names := make([]string, 0, 2)
	for _, b := range got {
		names = append(names, spanNames(b)...)
	}

	assert.ElementsMatch(t, []string{"GET /", "GET /other"}, names)
}

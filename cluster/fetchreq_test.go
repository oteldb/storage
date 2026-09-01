package cluster_test

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/cluster"
	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/signal"
)

func TestFetchRequestConditionCodec(t *testing.T) {
	t.Parallel()

	in := cluster.FetchRequest{
		Signal: signal.Trace, Tenant: "acme", Start: 0, End: 1<<63 - 1,
		Equal:      []fetch.EqualMatcher{{Name: "service.name", Value: "api"}},
		Conditions: []cluster.ConditionHint{{Column: "trace_id", Equal: fetch.EqualMatcher{Name: "trace_id", Value: "t-1"}}},
	}

	got, err := cluster.ParseFetchRequest(in.Encode())
	require.NoError(t, err)
	assert.Equal(t, in, got, "the condition hints round-trip alongside the matchers")
}

// TestFetchRequestWireCompatibility pins both directions of a rolling upgrade: a request carrying
// condition hints stays readable by a peer that predates them (the old decoder stops after the
// matchers), and one written without them decodes as no conditions rather than an error.
func TestFetchRequestWireCompatibility(t *testing.T) {
	t.Parallel()

	base := cluster.FetchRequest{
		Signal: signal.Trace, Tenant: "acme", Start: -5, End: 99,
		Equal: []fetch.EqualMatcher{{Name: "job", Value: "api"}},
	}

	withHints := base
	withHints.Conditions = []cluster.ConditionHint{{Column: "trace_id", Equal: fetch.EqualMatcher{Name: "trace_id", Value: "t-1"}}}

	encoded := withHints.Encode()
	assert.True(t, bytes.HasPrefix(encoded, base.Encode()),
		"the hints are an append-only tail, so a peer that predates them decodes the prefix it knows")

	sig, tenant, start, end, eq, err := cluster.DecodeFetchRequest(encoded)
	require.NoError(t, err, "an old peer reads the request it understands and ignores the tail")
	assert.Equal(t, signal.Trace, sig)
	assert.Equal(t, "acme", tenant)
	assert.Equal(t, int64(-5), start)
	assert.Equal(t, int64(99), end)
	assert.Equal(t, []fetch.EqualMatcher{{Name: "job", Value: "api"}}, eq)

	old := cluster.EncodeFetchRequest(signal.Log, "acme", 1, 2, []fetch.EqualMatcher{{Name: "job", Value: "api"}})
	got, err := cluster.ParseFetchRequest(old)
	require.NoError(t, err)
	assert.Empty(t, got.Conditions, "a request from an old peer carries no conditions")
	assert.Equal(t, []fetch.EqualMatcher{{Name: "job", Value: "api"}}, got.Equal)

	_, err = cluster.ParseFetchRequest(append(old, 0x02))
	require.Error(t, err, "a truncated condition tail is rejected, not silently dropped")
}

func TestConditionHints(t *testing.T) {
	t.Parallel()

	conds := []fetch.Condition{
		{Column: "body", Tokens: [][]byte{[]byte("oops")}},
		{Column: "trace_id", Equal: &fetch.EqualMatcher{Name: "trace_id", Value: "t-1"}},
	}

	assert.Equal(t,
		[]cluster.ConditionHint{{Column: "trace_id", Equal: fetch.EqualMatcher{Name: "trace_id", Value: "t-1"}}},
		cluster.ConditionHints(conds, true),
		"only the exact equality is pushable; a full-text condition is re-checked by the requester")
	assert.Nil(t, cluster.ConditionHints(conds, false),
		"without AllConditions a producer may ignore conditions, so ANDing them on the peer could drop rows")
}

// TestFetchRequestKeepsConditionsOutOfMatchers pins the pitfall of the pushdown: a condition names a
// record column (trace_id), while a matcher is resolved against the *identity* index — feeding one
// into the other would match no stream at all and answer empty.
func TestFetchRequestKeepsConditionsOutOfMatchers(t *testing.T) {
	t.Parallel()

	r := cluster.FetchRequest{
		Signal:     signal.Trace,
		Conditions: []cluster.ConditionHint{{Column: "trace_id", Equal: fetch.EqualMatcher{Name: "trace_id", Value: "t-1"}}},
	}.Request()

	assert.Empty(t, r.Matchers, "a column equality is not a series-identity matcher")
	require.Len(t, r.Conditions, 1)
	assert.True(t, r.AllConditions, "the peer ANDs what it was given, so its answer stays a superset")
	assert.Equal(t, "trace_id", r.Conditions[0].Column)
	require.NotNil(t, r.Conditions[0].Equal, "the hint drives the per-part bloom prune")
	assert.Equal(t, "t-1", r.Conditions[0].Equal.Value)
	require.NotNil(t, r.Conditions[0].Match, "a nil Match would panic the engine's filtered path")
	assert.True(t, r.Conditions[0].Match(signal.StringValue([]byte("t-1"))))
	assert.False(t, r.Conditions[0].Match(signal.StringValue([]byte("t-2"))))
	assert.False(t, r.Conditions[0].Match(signal.EmptyValue()), "an equality asserts the column is present")
}

// TestRemoteFetcherPushesConditions: the requester's condition hints reach the peer's fetch, which
// is what lets a by-id lookup prune parts instead of streaming the peer's whole span history.
func TestRemoteFetcherPushesConditions(t *testing.T) {
	t.Parallel()

	var got fetch.Request

	mux := http.NewServeMux()
	mux.Handle(cluster.ReadPath, cluster.NewReadHandler(func(_ context.Context, r fetch.Request) ([]*fetch.Batch, error) {
		got = r

		return nil, nil
	}))
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	rf := cluster.NewRemoteFetcher(signal.Trace, strings.TrimPrefix(srv.URL, "http://"), srv.Client())

	const want = "t-1"

	_, err := rf.Fetch(context.Background(), fetch.Request{
		Signal: signal.Trace, Tenant: "acme", Start: 0, End: 1<<63 - 1,
		Matchers:      []fetch.Matcher{{Name: []byte("job"), Spec: &fetch.EqualMatcher{Name: "job", Value: "api"}}},
		AllConditions: true,
		Conditions: []fetch.Condition{{
			Column: "trace_id",
			Match:  func(v signal.Value) bool { return string(v.Str()) == want },
			Equal:  &fetch.EqualMatcher{Name: "trace_id", Value: want},
		}},
	})
	require.NoError(t, err)

	require.Len(t, got.Conditions, 1, "the condition crossed the wire")
	assert.Equal(t, "trace_id", got.Conditions[0].Column)
	require.NotNil(t, got.Conditions[0].Equal)
	assert.Equal(t, want, got.Conditions[0].Equal.Value)
	assert.True(t, got.AllConditions)
	require.Len(t, got.Matchers, 1, "the identity matcher is still pushed down separately")
	assert.Equal(t, []byte("job"), got.Matchers[0].Name)
}

// TestSeriesHandlerIgnoresConditionTail: the enumeration RPCs share the fetch request encoding, so a
// body carrying condition hints must still decode to the same matchers there.
func TestSeriesHandlerIgnoresConditionTail(t *testing.T) {
	t.Parallel()

	var gotMatchers int

	mux := http.NewServeMux()
	mux.Handle(cluster.SeriesPath, cluster.SeriesHandler(
		func(_ context.Context, _ signal.Signal, _ string, _, _ int64, matchers []fetch.Matcher) ([]signal.Series, error) {
			gotMatchers = len(matchers)

			return nil, nil
		}))
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	body := cluster.FetchRequest{
		Signal: signal.Log, Tenant: "acme", Start: 1, End: 2,
		Equal:      []fetch.EqualMatcher{{Name: "job", Value: "api"}},
		Conditions: []cluster.ConditionHint{{Column: "trace_id", Equal: fetch.EqualMatcher{Name: "trace_id", Value: "t-1"}}},
	}.Encode()

	req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, srv.URL+cluster.SeriesPath, bytes.NewReader(body))
	require.NoError(t, err)

	resp, err := srv.Client().Do(req)
	require.NoError(t, err)
	t.Cleanup(func() { _ = resp.Body.Close() })
	require.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, 1, gotMatchers)
}

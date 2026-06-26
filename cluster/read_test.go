package cluster_test

import (
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

func TestFetchRequestCodec(t *testing.T) {
	t.Parallel()

	eq := []fetch.EqualMatcher{{Name: "__name__", Value: "http_requests"}, {Name: "job", Value: "api"}}
	sig, tenant, start, end, gotEq, err := cluster.DecodeFetchRequest(cluster.EncodeFetchRequest(signal.Log, "acme", -5, 1_700_000_000, eq))
	require.NoError(t, err)
	assert.Equal(t, signal.Log, sig)
	assert.Equal(t, "acme", tenant)
	assert.Equal(t, int64(-5), start)
	assert.Equal(t, int64(1_700_000_000), end)
	assert.Equal(t, eq, gotEq, "equality matchers round-trip")

	_, _, _, _, _, err = cluster.DecodeFetchRequest([]byte{byte(signal.Metric), 0xff}) //nolint:dogsled // only the error matters
	require.Error(t, err)
}

func TestLogBatchesCodec(t *testing.T) {
	t.Parallel()

	mk := func(svc string, ts []int64, bodies []string, sev []int64) *fetch.Batch {
		s := signal.Series{Resource: signal.Resource{Attributes: signal.NewAttributes(
			signal.KeyValue{Key: []byte("service.name"), Value: signal.StringValue([]byte(svc))},
		)}}
		body := make([][]byte, len(bodies))
		for i, b := range bodies {
			body[i] = []byte(b)
		}

		return &fetch.Batch{ID: s.Hash(), Series: s, Timestamps: ts, Columns: []fetch.NamedColumn{
			{Name: "body", Bytes: body},
			{Name: "severity", Int64: sev},
		}}
	}

	in := []*fetch.Batch{
		mk("api", []int64{100, 200}, []string{"first", "second"}, []int64{9, 17}),
		mk("web", []int64{150}, []string{"web"}, []int64{9}),
	}

	out, err := cluster.DecodeLogBatches(cluster.EncodeLogBatches(in))
	require.NoError(t, err)
	require.Len(t, out, 2)

	for i := range in {
		assert.Equal(t, in[i].ID, out[i].ID, "id recomputed from identity")
		assert.True(t, in[i].Series.Equal(out[i].Series))
		assert.Equal(t, in[i].Timestamps, out[i].Timestamps)
		require.Len(t, out[i].Columns, 2)
		assert.Equal(t, in[i].Columns[0].Bytes, out[i].Columns[0].Bytes, "body column round-trips")
		assert.Equal(t, in[i].Columns[1].Int64, out[i].Columns[1].Int64, "severity column round-trips")
	}

	_, err = cluster.DecodeLogBatches([]byte{0xff})
	require.Error(t, err)
}

func batch(job string, samples ...[2]int64) *fetch.Batch {
	s := signal.Series{Attributes: signal.NewAttributes(
		signal.KeyValue{Key: []byte("job"), Value: signal.StringValue([]byte(job))},
	)}
	b := &fetch.Batch{ID: s.Hash(), Series: s}
	for _, sm := range samples {
		b.Timestamps = append(b.Timestamps, sm[0])
		b.Values = append(b.Values, float64(sm[1]))
	}

	return b
}

func TestBatchesCodec(t *testing.T) {
	t.Parallel()

	in := []*fetch.Batch{
		batch("api", [2]int64{100, 1}, [2]int64{200, 2}),
		batch("web", [2]int64{100, 9}),
	}

	out, err := cluster.DecodeBatches(cluster.EncodeBatches(in))
	require.NoError(t, err)
	require.Len(t, out, 2)

	for i := range in {
		assert.Equal(t, in[i].ID, out[i].ID, "id recomputed from identity")
		assert.True(t, in[i].Series.Equal(out[i].Series), "labels preserved")
		assert.Equal(t, in[i].Timestamps, out[i].Timestamps)
		assert.Equal(t, in[i].Values, out[i].Values)
	}

	_, err = cluster.DecodeBatches([]byte{0xff})
	require.Error(t, err)
}

func TestRemoteFetcherOverHTTP(t *testing.T) {
	t.Parallel()

	want := []*fetch.Batch{batch("api", [2]int64{100, 1}, [2]int64{200, 2})}

	var gotTenant string
	var gotStart, gotEnd int64
	var gotMatchers int
	metricFn := func(_ context.Context, tenant string, start, end int64, matchers []fetch.Matcher) ([]*fetch.Batch, error) {
		gotTenant, gotStart, gotEnd, gotMatchers = tenant, start, end, len(matchers)

		return want, nil
	}
	logFn := func(context.Context, string, int64, int64, []fetch.Matcher) ([]*fetch.Batch, error) { return nil, nil }
	handler := cluster.ReadHandler(metricFn, logFn)

	mux := http.NewServeMux()
	mux.Handle(cluster.ReadPath, handler)
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	addr := strings.TrimPrefix(srv.URL, "http://")

	rf := cluster.NewRemoteFetcher(signal.Metric, addr, nil)
	it, err := rf.Fetch(context.Background(), fetch.Request{
		Tenant: "acme", Start: 10, End: 20,
		Matchers: []fetch.Matcher{{Name: []byte("job"), Spec: &fetch.EqualMatcher{Name: "job", Value: "api"}}},
	})
	require.NoError(t, err)
	got, err := fetch.Drain(context.Background(), it)
	require.NoError(t, err)

	assert.Equal(t, "acme", gotTenant)
	assert.Equal(t, int64(10), gotStart)
	assert.Equal(t, int64(20), gotEnd)
	assert.Equal(t, 1, gotMatchers, "the equality matcher was pushed to the peer")
	require.Len(t, got, 1)
	assert.True(t, want[0].Series.Equal(got[0].Series))
	assert.Equal(t, want[0].Timestamps, got[0].Timestamps)
}

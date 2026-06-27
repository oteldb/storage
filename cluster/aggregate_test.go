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
	"github.com/oteldb/storage/engine"
	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/signal"
)

func TestAggregateRequestCodec(t *testing.T) {
	t.Parallel()

	eq := []fetch.EqualMatcher{{Name: "__name__", Value: "http_requests"}, {Name: "job", Value: "api"}}
	tenant, start, end, step, gotEq, err := cluster.DecodeAggregateRequest(
		cluster.EncodeAggregateRequest("acme", -5, 1_700_000_000, 60_000, eq))
	require.NoError(t, err)
	assert.Equal(t, "acme", tenant)
	assert.Equal(t, int64(-5), start)
	assert.Equal(t, int64(1_700_000_000), end)
	assert.Equal(t, int64(60_000), step)
	assert.Equal(t, eq, gotEq)

	_, _, _, _, _, err = cluster.DecodeAggregateRequest([]byte{0xff}) //nolint:dogsled // only the error matters
	require.Error(t, err)
}

func aggSeries(job string) signal.Series {
	return signal.Series{Attributes: signal.NewAttributes(
		signal.KeyValue{Key: []byte("job"), Value: signal.StringValue([]byte(job))})}
}

func TestAggregatesCodec(t *testing.T) {
	t.Parallel()

	in := []engine.NamedAgg{
		{Series: aggSeries("api"), Buckets: []engine.BucketAgg{
			{Start: 0, SeriesAgg: engine.SeriesAgg{Count: 2, Sum: 5, Min: 1, Max: 4}},
			{Start: 60, SeriesAgg: engine.SeriesAgg{Count: 1, Sum: 9, Min: 9, Max: 9}},
		}},
		{Series: aggSeries("web"), Buckets: []engine.BucketAgg{
			{Start: 0, SeriesAgg: engine.SeriesAgg{Count: 1, Sum: 3, Min: 3, Max: 3}},
		}},
	}

	out, err := cluster.DecodeAggregates(cluster.EncodeAggregates(in))
	require.NoError(t, err)
	require.Len(t, out, 2)

	for i := range in {
		assert.True(t, in[i].Series.Equal(out[i].Series), "identity round-trips")
		assert.Equal(t, in[i].Buckets, out[i].Buckets)
	}

	_, err = cluster.DecodeAggregates([]byte{0xff})
	require.Error(t, err)
}

func TestRemoteAggregatorOverHTTP(t *testing.T) {
	t.Parallel()

	want := []engine.NamedAgg{{Series: aggSeries("api"), Buckets: []engine.BucketAgg{
		{Start: 0, SeriesAgg: engine.SeriesAgg{Count: 2, Sum: 5, Min: 1, Max: 4}},
	}}}

	var (
		gotTenant        string
		gotStart, gotEnd int64
		gotStep          int64
		gotMatchers      int
	)
	fn := func(_ context.Context, tenant string, start, end, step int64, matchers []fetch.Matcher) ([]engine.NamedAgg, error) {
		gotTenant, gotStart, gotEnd, gotStep, gotMatchers = tenant, start, end, step, len(matchers)

		return want, nil
	}

	mux := http.NewServeMux()
	mux.Handle(cluster.AggregatePath, cluster.AggregateHandler(fn))
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	addr := strings.TrimPrefix(srv.URL, "http://")

	got, err := cluster.NewRemoteAggregator(addr, nil).Aggregate(
		context.Background(), "acme", 10, 20, 60, []fetch.EqualMatcher{{Name: "job", Value: "api"}})
	require.NoError(t, err)

	assert.Equal(t, "acme", gotTenant)
	assert.Equal(t, int64(10), gotStart)
	assert.Equal(t, int64(20), gotEnd)
	assert.Equal(t, int64(60), gotStep)
	assert.Equal(t, 1, gotMatchers, "the equality matcher was pushed to the peer")
	require.Len(t, got, 1)
	assert.True(t, want[0].Series.Equal(got[0].Series))
	assert.Equal(t, want[0].Buckets, got[0].Buckets)
}

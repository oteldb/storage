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
)

func TestAggregateWindowRequestCodec(t *testing.T) {
	t.Parallel()

	eq := []fetch.EqualMatcher{{Name: "__name__", Value: "http_requests"}, {Name: "job", Value: "api"}}

	tenant, start, end, step, window, gotEq, err := cluster.DecodeAggregateWindowRequest(
		cluster.EncodeAggregateWindowRequest("acme", -5, 1_700_000_000, 300_000, 3_600_000, eq))
	require.NoError(t, err)
	assert.Equal(t, "acme", tenant)
	assert.Equal(t, int64(-5), start)
	assert.Equal(t, int64(1_700_000_000), end)
	assert.Equal(t, int64(300_000), step)
	assert.Equal(t, int64(3_600_000), window, "the window rides independently of the step")
	assert.Equal(t, eq, gotEq)

	_, _, _, _, _, _, err = cluster.DecodeAggregateWindowRequest([]byte{0xff}) //nolint:dogsled // only the error matters
	require.Error(t, err)
}

// TestAggregateWindowRequestIsNotABucketRequest guards the choice of a separate endpoint: the two
// request framings are not interchangeable, so a peer must never read one as the other.
func TestAggregateWindowRequestIsNotABucketRequest(t *testing.T) {
	t.Parallel()

	eq := []fetch.EqualMatcher{{Name: "job", Value: "api"}}
	payload := cluster.EncodeAggregateWindowRequest("acme", 0, 100, 10, 50, eq)

	_, _, _, step, gotEq, err := cluster.DecodeAggregateRequest(payload) //nolint:dogsled // only step and the matchers matter
	if err == nil {
		assert.NotEqual(t, eq, gotEq, "a window request must not decode as an equivalent bucket request")
		assert.Equal(t, int64(10), step)
	}
}

func TestWindowAggregatesCodec(t *testing.T) {
	t.Parallel()

	in := []engine.NamedWindowAgg{
		{Series: aggSeries("api"), Windows: []engine.WindowAgg{
			{End: 60, SeriesAgg: engine.SeriesAgg{Count: 2, Sum: 5, Min: 1, Max: 4}},
			{End: 120, SeriesAgg: engine.SeriesAgg{Count: 3, Sum: 14, Min: 1, Max: 9}},
		}},
		{Series: aggSeries("web"), Windows: []engine.WindowAgg{
			{End: 60, SeriesAgg: engine.SeriesAgg{Count: 1, Sum: 3, Min: 3, Max: 3}},
		}},
	}

	out, err := cluster.DecodeWindowAggregates(cluster.EncodeWindowAggregates(in))
	require.NoError(t, err)
	require.Len(t, out, 2)

	for i := range in {
		assert.True(t, in[i].Series.Equal(out[i].Series), "identity round-trips")
		assert.Equal(t, in[i].Windows, out[i].Windows)
	}

	_, err = cluster.DecodeWindowAggregates([]byte{0xff})
	require.Error(t, err)

	// Truncated mid-window: bounds-checked, not a panic.
	full := cluster.EncodeWindowAggregates(in)
	_, err = cluster.DecodeWindowAggregates(full[:len(full)-8])
	require.Error(t, err)
}

func TestRemoteAggregatorWindowOverHTTP(t *testing.T) {
	t.Parallel()

	want := []engine.NamedWindowAgg{{Series: aggSeries("api"), Windows: []engine.WindowAgg{
		{End: 60, SeriesAgg: engine.SeriesAgg{Count: 2, Sum: 5, Min: 1, Max: 4}},
	}}}

	var (
		gotTenant                        string
		gotStart, gotEnd, gotStep, gotWn int64
		gotMatchers                      int
	)

	fn := func(
		_ context.Context, tenant string, start, end, step, window int64, matchers []fetch.Matcher,
	) ([]engine.NamedWindowAgg, error) {
		gotTenant, gotStart, gotEnd, gotStep, gotWn, gotMatchers = tenant, start, end, step, window, len(matchers)

		return want, nil
	}

	mux := http.NewServeMux()
	mux.Handle(cluster.AggregateWindowPath, cluster.AggregateWindowHandler(fn))
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	addr := strings.TrimPrefix(srv.URL, "http://")

	got, err := cluster.NewRemoteAggregator(addr, nil).AggregateWindow(
		context.Background(), "acme", 10, 200, 20, 60, []fetch.EqualMatcher{{Name: "job", Value: "api"}})
	require.NoError(t, err)

	assert.Equal(t, "acme", gotTenant)
	assert.Equal(t, int64(10), gotStart)
	assert.Equal(t, int64(200), gotEnd)
	assert.Equal(t, int64(20), gotStep)
	assert.Equal(t, int64(60), gotWn)
	assert.Equal(t, 1, gotMatchers, "the equality matcher was pushed to the peer")
	require.Len(t, got, 1)
	assert.True(t, want[0].Series.Equal(got[0].Series))
	assert.Equal(t, want[0].Windows, got[0].Windows)
}

// TestRemoteAggregatorWindowFailsOnBucketOnlyPeer pins the compatibility story: a peer that serves
// only the disjoint endpoint answers 404, which surfaces as an error the coordinator fails over on
// — never as a silently disjoint answer to an overlapping question.
func TestRemoteAggregatorWindowFailsOnBucketOnlyPeer(t *testing.T) {
	t.Parallel()

	mux := http.NewServeMux()
	mux.Handle(cluster.AggregatePath, cluster.AggregateHandler(
		func(context.Context, string, int64, int64, int64, []fetch.Matcher) ([]engine.NamedAgg, error) {
			return nil, nil
		}))
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	_, err := cluster.NewRemoteAggregator(strings.TrimPrefix(srv.URL, "http://"), nil).
		AggregateWindow(context.Background(), "acme", 0, 100, 10, 50, nil)
	require.Error(t, err)
}

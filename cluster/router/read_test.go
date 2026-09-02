package router_test

import (
	"context"
	"net"
	"net/http"
	"regexp"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/cluster"
	"github.com/oteldb/storage/cluster/router"
	"github.com/oteldb/storage/engine"
	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/signal"
)

// peer is a fake storage node: it serves the cluster read endpoints from a fixed answer, so a test
// exercises the router's fan-out, failover and re-filtering against the real wire codecs.
type peer struct {
	addr string

	series []signal.Series
	keys   []cluster.KeyInfo
	values [][]byte
	side   map[string][]byte
	aggs   []engine.NamedAgg

	// records is the record-signal (log/trace) answer, carrying per-record columns. A peer answers
	// with the unfiltered window here (a superset the router must narrow), whatever it was asked.
	records []*fetch.Batch

	mu        sync.Mutex
	lastFetch fetch.Request

	// absent makes every endpoint disclaim the shard, the way an owner that holds no data for it
	// answers.
	absent bool
}

// serve mounts the peer's endpoints and returns its address.
func (p *peer) serve(t *testing.T) string {
	t.Helper()

	p.addr = freeAddr(t)

	batches := make([]*fetch.Batch, 0, len(p.series))
	for i := range p.series {
		s := p.series[i]
		batches = append(batches, &fetch.Batch{ID: s.Hash(), Series: s, Timestamps: []int64{1}, Values: []float64{1}})
	}

	mux := http.NewServeMux()
	mux.Handle(cluster.ReadPath, cluster.NewReadHandler(func(_ context.Context, r fetch.Request) ([]*fetch.Batch, error) {
		p.mu.Lock()
		p.lastFetch = r
		p.mu.Unlock()

		if p.absent {
			return nil, cluster.ErrShardAbsent
		}

		if r.Signal == signal.Metric {
			return batches, nil
		}

		return p.records, nil
	}))
	mux.Handle(cluster.SeriesPath, cluster.SeriesHandler(
		func(context.Context, signal.Signal, string, int64, int64, []fetch.Matcher) ([]signal.Series, error) {
			if p.absent {
				return nil, cluster.ErrShardAbsent
			}

			return p.series, nil
		}))
	mux.Handle(cluster.KeysPath, cluster.KeysHandler(
		func(context.Context, signal.Signal, string, int64, int64) ([]cluster.KeyInfo, error) {
			if p.absent {
				return nil, cluster.ErrShardAbsent
			}

			return p.keys, nil
		}))
	mux.Handle(cluster.ValuesPath, cluster.ValuesHandler(
		func(context.Context, cluster.ValuesRequest) ([][]byte, error) {
			if p.absent {
				return nil, cluster.ErrShardAbsent
			}

			return p.values, nil
		}))
	mux.Handle(cluster.SidePath, cluster.SideHandler(func(context.Context, string) (map[string][]byte, error) {
		if p.absent {
			return nil, cluster.ErrShardAbsent
		}

		return p.side, nil
	}))
	mux.Handle(cluster.AggregatePath, cluster.AggregateHandler(
		func(context.Context, string, int64, int64, int64, []fetch.Matcher) ([]engine.NamedAgg, error) {
			if p.absent {
				return nil, cluster.ErrShardAbsent
			}

			return p.aggs, nil
		}))

	srv := &http.Server{Handler: mux, ReadHeaderTimeout: 5 * time.Second}

	var lc net.ListenConfig

	ln, err := lc.Listen(t.Context(), "tcp", p.addr)
	require.NoError(t, err)

	go func() { _ = srv.Serve(ln) }()

	t.Cleanup(func() { _ = srv.Close() })

	return p.addr
}

// openRouter registers each peer as a member and returns a router that resolves every one of them
// as an owner (RF equal to the member count, one shard per tenant).
func openRouter(t *testing.T, peers ...*peer) *router.Router {
	t.Helper()

	const root = "/test"

	endpoint := startEtcd(t)
	for i, p := range peers {
		joinNode(t, endpoint, root, "node-"+string(rune('a'+i)), p.serve(t))
	}

	r, err := router.Open(t.Context(), router.Config{Etcd: []string{endpoint}, Root: root, RF: len(peers)})
	require.NoError(t, err)
	t.Cleanup(func() { _ = r.Close(context.WithoutCancel(t.Context())) })

	require.Eventually(t, func() bool { return len(r.Owners("acme")) == len(peers) },
		10*time.Second, 10*time.Millisecond, "router resolves every peer as an owner")

	return r
}

func svcSeries(name string) signal.Series {
	return signal.Series{Resource: signal.Resource{Attributes: signal.NewAttributes(
		signal.KeyValue{Key: []byte("service.name"), Value: signal.StringValue([]byte(name))},
	)}}
}

// regexMatcher is the kind of matcher that cannot cross the wire, so a peer answers a superset and
// the caller must narrow it.
func regexMatcher(name, pattern string) fetch.Matcher {
	re := regexp.MustCompile(pattern)

	return fetch.Matcher{
		Name:  []byte(name),
		Match: func(v signal.Value) bool { return re.Match(v.AppendText(nil)) },
	}
}

// TestFetcherNarrowsOwnerSuperset pins gap 2's fix: a peer applies only the serializable matchers,
// so the fetcher must re-apply the full set instead of handing its caller a superset to narrow.
func TestFetcherNarrowsOwnerSuperset(t *testing.T) {
	t.Parallel()

	r := openRouter(t, &peer{series: []signal.Series{svcSeries("api"), svcSeries("web")}})

	it, err := r.Fetcher(signal.Metric, "acme").Fetch(t.Context(), fetch.Request{
		Tenant: "acme", Start: 0, End: 10,
		Matchers: []fetch.Matcher{regexMatcher("service.name", "^a.*$")},
	})
	require.NoError(t, err)

	batches, err := fetch.Drain(t.Context(), it)
	require.NoError(t, err)
	require.Len(t, batches, 1, "the non-matching series is dropped, not returned as a superset")
	assert.Equal(t, svcSeries("api").Hash(), batches[0].ID)
}

func TestSeriesNarrowsAndFailsOver(t *testing.T) {
	t.Parallel()

	// The first owner asked disclaims the shard, so the enumeration must fail over rather than
	// accept its empty answer — whichever of the two the ring puts first.
	r := openRouter(t,
		&peer{absent: true},
		&peer{series: []signal.Series{svcSeries("api"), svcSeries("web")}},
	)

	got, err := r.Series(t.Context(), signal.Log, "acme",
		[]fetch.Matcher{regexMatcher("service.name", "^w")}, 0, 10)
	require.NoError(t, err)
	require.Len(t, got, 1)
	assert.Equal(t, svcSeries("web").Hash(), got[0].Hash())
}

func TestEnumerationFailOver(t *testing.T) {
	t.Parallel()

	r := openRouter(t,
		&peer{absent: true},
		&peer{
			keys:   []cluster.KeyInfo{{Key: []byte("http.method"), Scope: 0b101}},
			values: [][]byte{[]byte("GET")},
			side:   map[string][]byte{"stacks": []byte("abc")},
		},
	)

	keys, err := r.Keys(t.Context(), signal.Log, "acme", 0, 10)
	require.NoError(t, err)
	require.Len(t, keys, 1)
	assert.Equal(t, []byte("http.method"), keys[0].Key)
	assert.Equal(t, uint8(0b101), keys[0].Scope)

	values, err := r.Values(t.Context(),
		cluster.ValuesRequest{Signal: signal.Log, AttrKey: []byte("http.method")}, "acme")
	require.NoError(t, err)
	assert.Equal(t, [][]byte{[]byte("GET")}, values)

	side, err := r.Side(t.Context(), signal.Profile, "acme")
	require.NoError(t, err)
	assert.Equal(t, []byte("abc"), side["stacks"])
}

func TestAggregateNarrowsOwnerSuperset(t *testing.T) {
	t.Parallel()

	r := openRouter(t, &peer{aggs: []engine.NamedAgg{
		{Series: svcSeries("api"), Buckets: []engine.BucketAgg{{SeriesAgg: engine.SeriesAgg{Count: 2, Sum: 3}}}},
		{Series: svcSeries("web"), Buckets: []engine.BucketAgg{{SeriesAgg: engine.SeriesAgg{Count: 5, Sum: 7}}}},
	}})

	got, err := r.Aggregate(t.Context(), "acme", 0, 10, 0,
		[]fetch.Matcher{regexMatcher("service.name", "^a")})
	require.NoError(t, err)
	require.Len(t, got, 1)
	assert.Equal(t, svcSeries("api").Hash(), got[0].Series.Hash())
	require.Len(t, got[0].Buckets, 1)
	assert.Equal(t, int64(2), got[0].Buckets[0].Count)
}

// TestReadsEmptyWhenEveryOwnerDisclaims pins the contract that an all-absent shard reads as empty
// rather than as an error: absence is a failover signal, and once every owner has given it the
// shard genuinely holds nothing.
func TestReadsEmptyWhenEveryOwnerDisclaims(t *testing.T) {
	t.Parallel()

	r := openRouter(t, &peer{absent: true}, &peer{absent: true})

	series, err := r.Series(t.Context(), signal.Log, "acme", nil, 0, 10)
	require.NoError(t, err)
	assert.Empty(t, series)

	keys, err := r.Keys(t.Context(), signal.Log, "acme", 0, 10)
	require.NoError(t, err)
	assert.Empty(t, keys)

	values, err := r.Values(t.Context(), cluster.ValuesRequest{Signal: signal.Log, Column: "body"}, "acme")
	require.NoError(t, err)
	assert.Empty(t, values)

	side, err := r.Side(t.Context(), signal.Profile, "acme")
	require.NoError(t, err)
	assert.Empty(t, side)

	aggs, err := r.Aggregate(t.Context(), "acme", 0, 10, 0, nil)
	require.NoError(t, err)
	assert.Empty(t, aggs)

	it, err := r.Fetcher(signal.Metric, "acme").Fetch(t.Context(), fetch.Request{Tenant: "acme", End: 10})
	require.NoError(t, err)

	batches, err := fetch.Drain(t.Context(), it)
	require.NoError(t, err)
	assert.Empty(t, batches)
}

// TestReadsEmptyOnEmptyRing: with nobody to ask, a read is empty rather than an error — that is how
// the ring reports a shard nothing holds. A write, by contrast, must fail loudly.
func TestReadsEmptyOnEmptyRing(t *testing.T) {
	t.Parallel()

	endpoint := startEtcd(t)

	r, err := router.Open(t.Context(), router.Config{Etcd: []string{endpoint}, Root: "/test"})
	require.NoError(t, err)
	t.Cleanup(func() { _ = r.Close(context.WithoutCancel(t.Context())) })

	series, err := r.Series(t.Context(), signal.Log, "acme", nil, 0, 10)
	require.NoError(t, err)
	assert.Empty(t, series)

	aggs, err := r.Aggregate(t.Context(), "acme", 0, 10, 0, nil)
	require.NoError(t, err)
	assert.Empty(t, aggs)
}

// TestFetcherAppliesConditions pins trace-by-id over the cluster. Its whole predicate is a columnar
// condition on the trace id — there are no matchers — so the peer is asked for the id's equality
// hint (which prunes its parts) and its Match closure, which cannot cross the wire, is re-applied
// here. Narrowing only by matchers returned the peer's entire window as if it were one trace.
func TestFetcherAppliesConditions(t *testing.T) {
	t.Parallel()

	series := svcSeries("api")
	p := &peer{records: []*fetch.Batch{{
		ID:         series.Hash(),
		Series:     series,
		Timestamps: []int64{1, 2, 3},
		Columns: []fetch.NamedColumn{
			fetch.BytesColumn("trace_id", [][]byte{[]byte("aaa"), []byte("bbb"), []byte("aaa")}),
		},
	}}}
	r := openRouter(t, p)

	want := "aaa"
	it, err := r.Fetcher(signal.Trace, "acme").Fetch(t.Context(), fetch.Request{
		Tenant: "acme", Start: 0, End: 10,
		AllConditions: true,
		Conditions: []fetch.Condition{{
			Column: "trace_id",
			Match:  func(v signal.Value) bool { return string(v.Str()) == want },
			Equal:  &fetch.EqualMatcher{Name: "trace_id", Value: want},
		}},
	})
	require.NoError(t, err)

	batches, err := fetch.Drain(t.Context(), it)
	require.NoError(t, err)
	require.Len(t, batches, 1)
	assert.Equal(t, []int64{1, 3}, batches[0].Timestamps, "the other trace's span is dropped, not returned")

	p.mu.Lock()
	defer p.mu.Unlock()

	require.Len(t, p.lastFetch.Conditions, 1, "the peer was told which trace to prune to")
	require.NotNil(t, p.lastFetch.Conditions[0].Equal)
	assert.Equal(t, want, p.lastFetch.Conditions[0].Equal.Value)
	assert.Equal(t, "trace_id", p.lastFetch.Conditions[0].Column)
	assert.Empty(t, p.lastFetch.Matchers, "a column equality is not pushed as a series-identity matcher")
}

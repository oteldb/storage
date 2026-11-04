package storage

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/http/httptrace"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/cluster"
	"github.com/oteldb/storage/cluster/etcd/etcdtest"
	"github.com/oteldb/storage/engine"
	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/signal"
	"github.com/oteldb/storage/tenant"
)

func TestRetentionFetchRPC(t *testing.T) {
	t.Parallel()
	s, err := InMemory(WithTenant(tenantByService), WithTenancy(tenant.ResolverFunc(func(tid signal.TenantID) tenant.Policy {
		if tid == "real" {
			return tenant.Policy{Retention: tenant.Retention{MaxAge: 300}}
		}
		return tenant.Policy{}
	})))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, s.Close(context.Background())) })
	s.now = func() int64 { return 1000 }
	writeRetentionSignals(t, s, "real", []int64{100, 800})
	mux := http.NewServeMux()
	mux.Handle(cluster.ReadPath, cluster.NewReadHandler(s.serveFetch))
	mux.Handle(cluster.AggregatePath, cluster.AggregateHandler(s.localAggregate))
	mux.Handle(cluster.AggregateWindowPath, cluster.AggregateWindowHandler(s.localAggregateWindow))
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)
	addr := strings.TrimPrefix(server.URL, "http://")
	for _, sig := range []signal.Signal{signal.Metric, signal.Log, signal.Trace, signal.Profile, signal.Exemplar} {
		t.Run(sig.String(), func(t *testing.T) {
			t.Parallel()
			remote := cluster.NewRemoteFetcher(sig, addr, server.Client())
			it, err := remote.Fetch(t.Context(), fetch.Request{Tenant: "real", Signal: sig, Start: 0, End: 1000})
			require.NoError(t, err)
			batches, err := fetch.Drain(t.Context(), it)
			require.NoError(t, err)
			require.NotEmpty(t, batches)
			for _, b := range batches {
				require.Equal(t, []int64{800}, b.Timestamps)
			}
		})
	}
	agg := cluster.NewRemoteAggregator(addr, server.Client())
	named, err := agg.Aggregate(t.Context(), "real", 0, 1000, 0, []fetch.EqualMatcher{{Name: "__name__", Value: "retention"}})
	require.NoError(t, err)
	require.Len(t, named, 1)
	require.Len(t, named[0].Buckets, 1)
	require.InEpsilon(t, 20.0, named[0].Buckets[0].Sum, 1e-12)
	windows, err := agg.AggregateWindow(t.Context(), "real", 0, 1000, engine.WindowSpec{Step: 200, Window: 400}, []fetch.EqualMatcher{{Name: "__name__", Value: "retention"}})
	require.NoError(t, err)
	require.Len(t, windows, 1)
	require.Len(t, windows[0].Windows, 2)
}

//nolint:paralleltest // owns an embedded etcd and mutates the nodes' test clocks.
func TestRetentionFetchClusterFanout(t *testing.T) {
	endpoint := etcdtest.Start(t)
	var age atomic.Int64
	policy := WithTenancy(tenant.ResolverFunc(func(tid signal.TenantID) tenant.Policy {
		if tid != "real" {
			return tenant.Policy{}
		}
		bounds := make(map[signal.Signal]tenant.SignalRetention)
		for _, sig := range []signal.Signal{signal.Metric, signal.Log, signal.Trace, signal.Profile, signal.Exemplar} {
			bounds[sig] = tenant.SignalRetention{MaxAge: time.Duration(age.Load())}
		}
		return tenant.Policy{Retention: tenant.Retention{PerSignal: bounds}}
	}))

	nodes := map[string]*Storage{}
	for _, name := range []string{"node-a", "node-b", "node-c"} {
		nodes[name] = openClusterNodeSharded(t, endpoint, name, 4, WithTenant(tenantByService), policy, WithFlushInterval(-1))
		nodes[name].now = func() int64 { return 1000 }
	}
	awaitMembership(t, nodes)
	writeRetentionSignals(t, nodes["node-a"], "real", []int64{100, 800})
	age.Store(300)
	var connections atomic.Int64
	ctx := httptrace.WithClientTrace(t.Context(), &httptrace.ClientTrace{GotConn: func(httptrace.GotConnInfo) { connections.Add(1) }})
	for _, node := range nodes {
		for _, sig := range []signal.Signal{signal.Metric, signal.Log, signal.Trace, signal.Profile, signal.Exemplar} {
			request := fetch.Request{Start: 0, End: 1000}
			if sig == signal.Metric {
				request.Matchers = []fetch.Matcher{nameMatcher("retention")}
			}
			it, err := retentionSignalFetcher(node, sig, "real").Fetch(ctx, request)
			require.NoError(t, err)
			batches, err := fetch.Drain(ctx, it)
			require.NoError(t, err)
			require.NotEmpty(t, batches)
			var times []int64
			for _, b := range batches {
				times = append(times, b.Timestamps...)
			}
			slices.Sort(times)
			require.Equal(t, []int64{800}, times)
		}
		result, err := node.AggregateMetricsStepNamed(ctx, "real", fetch.Request{Start: 0, End: 1000, Matchers: []fetch.Matcher{nameMatcher("retention")}}, 0)
		require.NoError(t, err)
		require.Len(t, result, 1)
		require.InEpsilon(t, 20.0, result[0].Buckets[0].Sum, 1e-12)
	}
	require.Positive(t, connections.Load(), "non-owner reads must exercise actual RPC fan-out")
	// Read clamping has not physically removed rows from either replica.
	total := 0
	for _, node := range nodes {
		for _, eng := range node.engineSnapshot() {
			total += eng.HeadSampleCount()
		}
	}
	require.Positive(t, total)
}

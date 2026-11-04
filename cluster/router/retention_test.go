package router_test

import (
	"context"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/cluster"
	"github.com/oteldb/storage/cluster/etcd/etcdtest"
	"github.com/oteldb/storage/cluster/router"
	"github.com/oteldb/storage/signal"
	"github.com/oteldb/storage/signal/log"
	"github.com/oteldb/storage/signal/metric"
)

func TestRouterWritesCountRetentionRejects(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name  string
		sig   signal.Signal
		write func(context.Context, *router.Router, cluster.TenantFunc) (router.Written, error)
	}{
		{
			name: "metrics", sig: signal.Metric,
			write: func(ctx context.Context, r *router.Router, tenantOf cluster.TenantFunc) (router.Written, error) {
				var md metric.Metrics
				for _, name := range []string{"a", "b"} {
					scope := md.AddResource().AddScope()
					scope.Scope.Name = []byte(name)
					m := scope.AddMetric()
					m.Name = []byte("requests")
					for i := range 8 {
						m.AddPoint().Ts = int64(i + 1)
					}
				}
				return r.WriteMetrics(ctx, md, tenantOf)
			},
		},
		{
			name: "records", sig: signal.Log,
			write: func(ctx context.Context, r *router.Router, tenantOf cluster.TenantFunc) (router.Written, error) {
				var ld log.Logs
				for _, name := range []string{"a", "b"} {
					scope := ld.AddResource().AddScope()
					scope.Scope.Name = []byte(name)
					for i := range 8 {
						scope.AddRecord().Timestamp = int64(i + 1)
					}
				}
				return r.WriteLogs(ctx, ld, tenantOf)
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			reject := cluster.Reject{OOO: 1, Cardinality: 1, InFlight: 1, Retention: 2}
			var mu sync.Mutex
			calls := map[string]int{}
			srv := httptest.NewServer(cluster.PrimaryWriteHandler(func(_ context.Context, sig signal.Signal, key string, _ []byte) (cluster.Reject, error) {
				assert.Equal(t, tc.sig, sig)
				mu.Lock()
				calls[key]++
				mu.Unlock()
				return reject, nil
			}))
			t.Cleanup(srv.Close)

			endpoint := etcdtest.Start(t)
			joinNode(t, endpoint, "/test", "node-a", strings.TrimPrefix(srv.URL, "http://"))
			r, err := router.Open(t.Context(), router.Config{Etcd: []string{endpoint}, Root: "/test", RF: 1, ShardsPerTenant: 1})
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, r.Close(context.WithoutCancel(t.Context()))) })

			out, err := tc.write(t.Context(), r, func(_ signal.Resource, scope signal.Scope) signal.TenantID {
				return signal.TenantID(scope.Name)
			})
			require.NoError(t, err)
			assert.Equal(t, cluster.Reject{OOO: 2, Cardinality: 2, InFlight: 2, Retention: 4}, out.Rejected)
			assert.Equal(t, 6, out.Accepted)
			assert.Equal(t, 16, out.Accepted+out.Rejected.Total())
			mu.Lock()
			defer mu.Unlock()
			assert.Equal(t, map[string]int{"a": 1, "b": 1}, calls)
		})
	}
}

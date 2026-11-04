package storage

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/cluster/etcd/etcdtest"
	"github.com/oteldb/storage/signal"
	"github.com/oteldb/storage/tenant"
)

//nolint:paralleltest // owns an embedded etcd; runs serially
func TestRetentionAdmissionPrimaryDecidesAllSignals(t *testing.T) {
	endpoint := etcdtest.Start(t)
	policy := WithTenancy(tenant.ResolverFunc(func(signal.TenantID) tenant.Policy {
		return tenant.Policy{Retention: tenant.Retention{MaxAge: time.Hour}}
	}))
	nodes := map[string]*Storage{
		"node-a": openClusterNodeWith(t, endpoint, "node-a", backend.Memory(), WithFlushInterval(-1), WithTenant(tenantByService), policy, WithRejectOutOfRetention()),
		"node-b": openClusterNodeWith(t, endpoint, "node-b", backend.Memory(), WithFlushInterval(-1), WithTenant(tenantByService), policy, WithRejectOutOfRetention()),
	}
	awaitMembership(t, nodes)
	const now = int64(10 * time.Hour)
	for _, s := range nodes {
		s.now = func() int64 { return now }
	}
	primary, ok := nodes["node-a"].cluster.membership.Ring().Primary([]byte("api"))
	require.True(t, ok)
	origin := nodes["node-a"]
	if primary.ID == "node-a" {
		origin = nodes["node-b"]
	}
	for sig := signal.Metric; sig <= signal.Exemplar; sig++ {
		t.Run(sig.String(), func(t *testing.T) {
			acc := writeRetentionPair(t, origin, sig, now-int64(2*time.Hour), now-int64(time.Minute))
			require.EqualValues(t, 1, acc.Accepted)
			if sig == signal.Exemplar {
				require.Zero(t, acc.Rejected)
			} else {
				require.EqualValues(t, 1, acc.Rejected)
				require.Equal(t, "out_of_retention", acc.RejectedReason)
			}
			for _, s := range nodes {
				if sig == signal.Metric {
					e, ok := s.lookupEngine("api")
					require.True(t, ok)
					require.Equal(t, 1, e.HeadSampleCount())
				} else {
					e, ok := s.lookupRecordEngine(sig, "api")
					require.True(t, ok)
					require.Equal(t, 1, e.HeadRecordCount())
				}
			}
		})
	}
	require.EqualValues(t, 5, nodes[primary.ID].AdmissionStats("api").RejectedRetention)
}

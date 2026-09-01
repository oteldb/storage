package storage

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/signal"
	"github.com/oteldb/storage/tenant"
)

// TestClusterAdmissionStatsTallied pins the cluster admission tally (issue #451): a clustered write
// is admitted by the shard primary, so the counters must advance there — and there only. The
// cluster-wide sum is asserted exactly, which is what catches the two ways of getting this wrong:
// zero if nobody records, RF times the truth if the replicas record their verbatim replay too.
//
//nolint:paralleltest // owns an embedded etcd; runs serially
func TestClusterAdmissionStatsTallied(t *testing.T) {
	endpoint := startEtcd(t)
	ctx := context.Background()

	// MaxSeries is enforced per engine by the primary, so one metric series and one log stream both
	// fit while a second metric series is shed as cardinality.
	limits := WithTenancy(tenant.ResolverFunc(func(signal.TenantID) tenant.Policy {
		return tenant.Policy{Limits: tenant.Limits{MaxSeries: 1}}
	}))

	nodes := map[string]*Storage{
		"node-a": openClusterNodeWith(t, endpoint, "node-a", backend.Memory(), limits),
		"node-b": openClusterNodeWith(t, endpoint, "node-b", backend.Memory(), limits),
	}
	awaitMembership(t, nodes)

	a := nodes["node-a"]

	acc, err := a.WriteMetrics(ctx, gaugeBatch("api", "m", []int64{100, 200, 300}, []float64{1, 2, 3}))
	require.NoError(t, err)
	require.Equal(t, int64(3), acc.Accepted)

	shed, err := a.WriteMetrics(ctx, gaugeBatch("api", "m2", []int64{100}, []float64{1}))
	require.NoError(t, err)
	require.Equal(t, int64(1), shed.Rejected)
	require.Equal(t, "max_series", shed.RejectedReason)

	logs, err := a.WriteLogs(ctx, logBatch("api", [3]any{100, 9, "one"}, [3]any{200, 9, "two"}))
	require.NoError(t, err)
	require.Equal(t, int64(2), logs.Accepted)

	var total AdmissionStats

	for _, s := range nodes {
		st := s.AdmissionStats("default")
		total.Accepted += st.Accepted
		total.RejectedCardinality += st.RejectedCardinality
		total.RejectedOOO += st.RejectedOOO
		total.RejectedInFlight += st.RejectedInFlight
		total.RejectedRate += st.RejectedRate
	}

	assert.Equal(t, int64(5), total.Accepted, "3 points + 2 log records, counted once cluster-wide")
	assert.Equal(t, int64(1), total.RejectedCardinality)
	assert.Zero(t, total.RejectedOOO)
	assert.Zero(t, total.RejectedInFlight)
	assert.Zero(t, total.RejectedRate)
}

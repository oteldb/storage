package storage

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/cluster"
	"github.com/oteldb/storage/cluster/etcd/etcdtest"
	"github.com/oteldb/storage/recordengine"
	"github.com/oteldb/storage/signal"
	"github.com/oteldb/storage/signal/exemplar"
	"github.com/oteldb/storage/signal/log"
	"github.com/oteldb/storage/signal/metric"
	"github.com/oteldb/storage/signal/profile"
	"github.com/oteldb/storage/signal/trace"
)

//nolint:paralleltest // owns an embedded etcd; runs serially
func TestPrimaryClaimRefusesPeerClaim(t *testing.T) {
	endpoint := etcdtest.Start(t)
	nodes := map[string]*Storage{
		"node-a": openClusterNodeWith(t, endpoint, "node-a", backend.Memory(), WithFlushInterval(-1)),
		"node-b": openClusterNodeWith(t, endpoint, "node-b", backend.Memory(), WithFlushInterval(-1)),
	}
	awaitMembership(t, nodes)
	for _, sig := range []signal.Signal{signal.Metric, signal.Log, signal.Trace, signal.Profile, signal.Exemplar} {
		t.Run(sig.String(), func(t *testing.T) {
			ctx := t.Context()
			shard := signal.TenantID("claim-" + sig.String())
			primary, ok := nodes["node-a"].cluster.membership.Ring().Primary([]byte(shard))
			require.True(t, ok)
			victim := nodes[primary.ID]
			peer := nodes["node-a"]
			if peer == victim {
				peer = nodes["node-b"]
			}
			term, held, err := peer.cluster.ownership.Acquire(ctx, string(shard))
			require.NoError(t, err)
			require.True(t, held)
			require.Positive(t, term)
			require.False(t, victim.fenced())
			require.False(t, victim.claimsShard(shard))
			payload := primaryClaimPayload(sig, shard)
			_, err = victim.primaryWrite(ctx, sig, string(shard), payload)
			require.ErrorIs(t, err, cluster.ErrNotPrimary)
			_, err = peer.sendPrimaryWrite(ctx, victim.cluster.self, cluster.EncodeWrite(sig, string(shard), payload))
			require.ErrorIs(t, err, cluster.ErrNotPrimary)
			assertPrimaryHeadCount(t, victim, sig, shard, false, 0)
			require.NoError(t, peer.cluster.ownership.Release(ctx, string(shard)))
			_, err = victim.primaryWrite(ctx, sig, string(shard), payload)
			require.NoError(t, err, "a cold shard must acquire its claim without a prior maintenance tick")
			require.True(t, victim.claimsShard(shard))
			assertPrimaryHeadCount(t, victim, sig, shard, true, 1)
			assertPrimaryHeadCount(t, peer, sig, shard, true, 1)
		})
	}
}

func primaryClaimPayload(sig signal.Signal, shard signal.TenantID) []byte {
	tenantOf := func(signal.Resource, signal.Scope) signal.TenantID { return shard }
	if sig == signal.Metric {
		md := gaugeBatch("claim", "metric", []int64{100}, []float64{1})
		return cluster.FrameMetrics(md, 1, tenantOf, nil).Shards[shard]
	}
	project := func(emit func(*recordengine.Batch)) int {
		switch sig {
		case signal.Log:
			return log.Project(logBatch("claim", [3]any{100, 9, "record"}), emit)
		case signal.Trace:
			return trace.Project(traceBatch("claim", spanSpec{traceID: "trace", spanID: "span", name: "op", start: 100, end: 200}), emit)
		case signal.Profile:
			batch := profileBatch("claim", 100, sampleSpec{typ: "cpu", unit: "ns", value: 1})
			return profile.Project(&batch, emit)
		case signal.Exemplar:
			batch := exemplarBatch("claim", "metric", 100, map[string][]exemplarSpec{"/": {{ts: 100, value: 1, traceID: "trace"}}})
			rows := 0
			metric.Project(batch, func(b *metric.Batch) { rows += exemplar.Project(b, emit) })
			return rows
		case signal.Metric:
			return 0
		default:
			return 0
		}
	}
	return cluster.FrameRecords(project, 1, tenantOf, nil).Shards[shard]
}

func assertPrimaryHeadCount(t *testing.T, s *Storage, sig signal.Signal, shard signal.TenantID, exists bool, rows int) {
	t.Helper()
	if sig == signal.Metric {
		e, ok := s.lookupEngine(shard)
		require.Equal(t, exists, ok)
		if ok {
			assert.Equal(t, rows, e.HeadSampleCount())
		}
		return
	}
	e, ok := s.lookupRecordEngine(sig, shard)
	require.Equal(t, exists, ok)
	if ok {
		assert.Equal(t, rows, e.HeadRecordCount())
	}
}

//nolint:paralleltest // owns an embedded etcd; runs serially
func TestPrimaryClaimRefusesMisroutedClaimHolder(t *testing.T) {
	endpoint := etcdtest.Start(t)
	nodes := map[string]*Storage{
		"node-a": openClusterNodeWith(t, endpoint, "node-a", backend.Memory(), WithFlushInterval(-1)),
		"node-b": openClusterNodeWith(t, endpoint, "node-b", backend.Memory(), WithFlushInterval(-1)),
	}
	awaitMembership(t, nodes)
	shard := signal.TenantID("misrouted")
	primary, ok := nodes["node-a"].cluster.membership.Ring().Primary([]byte(shard))
	require.True(t, ok)
	target := nodes["node-a"]
	origin := nodes[primary.ID]
	if target == origin {
		target = nodes["node-b"]
	}
	_, held, err := target.cluster.ownership.Ensure(t.Context(), string(shard))
	require.NoError(t, err)
	require.True(t, held)
	require.True(t, target.claimsShard(shard))
	require.False(t, target.fenced())
	for _, sig := range []signal.Signal{signal.Metric, signal.Log, signal.Trace, signal.Profile, signal.Exemplar} {
		t.Run(sig.String(), func(t *testing.T) {
			payload := primaryClaimPayload(sig, shard)
			_, err := target.primaryWrite(t.Context(), sig, string(shard), payload)
			require.ErrorIs(t, err, cluster.ErrNotPrimary)
			_, err = origin.sendPrimaryWrite(t.Context(), target.cluster.self, cluster.EncodeWrite(sig, string(shard), payload))
			require.ErrorIs(t, err, cluster.ErrNotPrimary, "HTTP routing must not bypass placement checks")
			assertPrimaryHeadCount(t, target, sig, shard, false, 0)
		})
	}
}

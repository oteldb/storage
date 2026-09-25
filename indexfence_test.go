package storage

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/backend/bucketindex"
	"github.com/oteldb/storage/cluster"
	"github.com/oteldb/storage/cluster/etcd/etcdtest"
	"github.com/oteldb/storage/internal/partid"
	"github.com/oteldb/storage/signal"
)

// plantCorruptPart commits an index entry at enginePrefix for a part whose manifest is garbage, and
// returns its prefix: the next load of that engine fails on it.
func plantCorruptPart(ctx context.Context, t *testing.T, be backend.Backend, enginePrefix string) string {
	t.Helper()

	key := enginePrefix + "/" + bucketindex.Object
	bad := enginePrefix + "/" + partid.New().String()

	require.NoError(t, be.Write(ctx, bad+"/manifest", []byte("not a manifest")))

	ix, version, err := bucketindex.LoadVersioned(ctx, be, key)
	require.NoError(t, err)
	ix.Add(bucketindex.Entry{Prefix: bad, MinTime: 1, MaxTime: 2})
	_, err = ix.Save(ctx, be, key, version)
	require.NoError(t, err)

	return bad
}

// TestMaintainRetriesFencedLoad: an owner reloads its index only after a backfill, so the
// maintenance cycle itself must retry a failed load, or one failure refuses its commits for good.
func TestMaintainRetriesFencedLoad(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s, err := InMemory()
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close(ctx) })

	write := func() {
		t.Helper()

		_, err := s.WriteMetrics(ctx, gaugeBatch("api", "m", []int64{time.Now().UnixNano()}, []float64{1}))
		require.NoError(t, err)
	}

	write()
	s.maintain(ctx)

	eng, ok := s.lookupEngine(defaultTenant)
	require.True(t, ok)

	be := s.backendFor(defaultTenant)
	bad := plantCorruptPart(ctx, t, be, string(defaultTenant)+metricsPrefix)

	require.Error(t, eng.LoadParts(ctx))
	require.True(t, metricStat(t, s).IndexFenced)

	write()
	s.maintain(ctx)

	st := metricStat(t, s)
	assert.True(t, st.IndexFenced, "a part that stays unreadable keeps the engine fenced")
	assert.Positive(t, st.HeadItems, "and its head unflushed")

	require.NoError(t, be.Delete(ctx, bad+"/manifest"))
	s.maintain(ctx)

	st = metricStat(t, s)
	assert.False(t, st.IndexFenced, "the cycle's retry lifted the fence")
	assert.Zero(t, st.HeadItems, "and the flush it held back landed")
}

// TestSingleNodeFencedServesOldPartsAndSaysWhy: a store with no cluster has no owner to fail over
// to, so a fenced engine keeps answering from the part set that last loaded, and Inspect names the
// failure. The maintenance retries then take the corrupt part out as a want.
func TestSingleNodeFencedServesOldPartsAndSaysWhy(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s, err := InMemory()
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close(ctx) })

	_, err = s.WriteMetrics(ctx, gaugeBatch("api", "m", []int64{100}, []float64{1}))
	require.NoError(t, err)
	s.maintain(ctx)

	eng, ok := s.lookupEngine(defaultTenant)
	require.True(t, ok)

	bad := plantCorruptPart(ctx, t, s.backendFor(defaultTenant), string(defaultTenant)+metricsPrefix)
	require.Error(t, eng.LoadParts(ctx))

	st := metricStat(t, s)
	assert.True(t, st.IndexFenced)
	assert.Contains(t, st.IndexLoadError, bad, "the health error names the part")
	assert.Equal(t, []float64{1}, readSamples(t, s, "m"))

	for range 3 {
		s.maintain(ctx)
	}

	st = metricStat(t, s)
	assert.False(t, st.IndexFenced, "a part corrupt over three loads stops fencing the engine")
	assert.Empty(t, st.IndexLoadError)
	assert.Equal(t, 1, st.WantedParts, "it is owed as a want instead")
}

// TestClusterFencedOwnerDisclaimsReads: a fenced cluster owner's part set may lack what the stored
// index gained, so it disclaims the whole shard and a read fails over to the other owner. With every
// owner fenced no complete answer exists, and the read fails rather than serving the stale sets.
//
//nolint:paralleltest // owns an embedded etcd; runs serially
func TestClusterFencedOwnerDisclaimsReads(t *testing.T) {
	endpoint := etcdtest.Start(t)
	ctx := context.Background()

	c := newReadPolicyCluster(t, endpoint)
	c.write(t)

	fence := func(id string) {
		t.Helper()

		plantCorruptPart(ctx, t, c.backends[id], string(c.shard)+logsPrefix)

		eng, ok := c.nodes[id].lookupRecordEngine(signal.Log, c.shard)
		require.True(t, ok)
		require.Error(t, eng.LoadParts(ctx))
		require.True(t, eng.IndexFenced())
	}

	owners := c.owners(t)
	fenced := c.nodes[owners[0]]

	fence(owners[0])

	require.ErrorIs(t, fenced.canAnswer(ctx, rpcOpRead, signal.Log, c.shard, true, 300, 400),
		cluster.ErrShardIncomplete, "a fenced engine disclaims every window, not only a lost part's")

	got, err := fetchLogs(ctx, fenced, 0, 1<<62)
	require.NoError(t, err, "one fenced owner is a failover")
	assert.Equal(t, 2, logRows(got))

	fence(owners[1])

	for id, s := range c.nodes {
		_, err := fetchLogs(ctx, s, 0, 1<<62)
		require.ErrorIs(t, err, cluster.ErrShardIncomplete, "%s", id)
	}
}

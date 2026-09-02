package storage

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/signal"
	"github.com/oteldb/storage/tenant"
)

// samplingNodes opens a three-node cluster whose tenant policy carries the given per-second row
// budget (0 ⇒ sampling off), returning the nodes and the one that is not an owner of "default".
// Reading back from the non-owner forces the real read fan-out, so the assertions cover the whole
// origin → primary → secondary → fan-out path rather than one node's head.
func samplingNodes(t *testing.T, budget int64) (nodes map[string]*Storage, nonOwner *Storage) {
	t.Helper()

	endpoint := startEtcd(t)
	pol := WithTenancy(tenant.ResolverFunc(func(signal.TenantID) tenant.Policy {
		return tenant.Policy{Sampling: tenant.Sampling{MaxRowsPerSecond: budget}}
	}))

	nodes = map[string]*Storage{
		"node-a": openClusterNodeWith(t, endpoint, "node-a", backend.Memory(), pol),
		"node-b": openClusterNodeWith(t, endpoint, "node-b", backend.Memory(), pol),
		"node-c": openClusterNodeWith(t, endpoint, "node-c", backend.Memory(), pol),
	}

	awaitMembership(t, nodes)

	owners := nodes["node-a"].cluster.membership.Ring().Lookup([]byte("default"), 2)
	require.Len(t, owners, 2)

	ownerID := map[string]bool{owners[0].ID: true, owners[1].ID: true}
	for name, s := range nodes {
		if !ownerID[name] {
			nonOwner = s
		}
	}

	require.NotNil(t, nonOwner, "one of three nodes is not an owner of the tenant")

	return nodes, nonOwner
}

// rampSeries is a run of ts/value pairs on one series, all values 1 so a weighted sum reads as a
// weighted count.
func rampSeries(base int64, n int) (ts []int64, values []float64) {
	ts, values = make([]int64, n), make([]float64, n)
	for i := range ts {
		ts[i], values[i] = base+int64(i), 1
	}

	return ts, values
}

// fetchDefault reads every sample of metric name back from s through the tenant fetcher, which on a
// non-owner is the cluster read fan-out.
func fetchDefault(t *testing.T, s *Storage, name string, start, end int64) []*fetch.Batch {
	t.Helper()

	it, err := s.Fetcher("default").Fetch(context.Background(), fetch.Request{
		Start: start, End: end, Matchers: []fetch.Matcher{nameMatcher(name)},
	})
	require.NoError(t, err)

	got, err := fetch.Drain(context.Background(), it)
	require.NoError(t, err)

	return got
}

// TestClusteredWriteAppliesBudgetedSampling is the ingest capstone for #463: a tenant with a
// sampling budget writing through the *clustered* path must actually be sampled at the origin, and
// the resulting weights must survive framing, the primary's admission re-frame, replication and the
// read fan-out — so the weighted count a non-owner reads back recovers the total that was written.
// Before the fix the clustered path returned before the sampler ran, so every row was stored at
// weight 1 and the budget was silently inert.
//
//nolint:paralleltest // owns an embedded etcd; runs serially
func TestClusteredWriteAppliesBudgetedSampling(t *testing.T) {
	const (
		sec    = int64(1_000_000_000)
		budget = int64(10)
		rows   = 100
	)

	nodes, nonOwner := samplingNodes(t, budget)
	origin := nodes["node-a"]
	ctx := context.Background()

	// The sampler adapts once per 1-second window from the prior window's observed rate. Drive that
	// clock by hand instead of sleeping: the test crosses the window boundary by advancing origin's
	// injected clock, so it neither waits nor races a real second.
	now := 5 * sec
	origin.now = func() int64 { return now }

	// Window 1 has no prior history, so nothing is sampled and every row lands unweighted. This is
	// also the lossless-default pin for a budgeted tenant that is not yet over budget.
	ts1, vals1 := rampSeries(1, rows)
	acc1, err := origin.WriteMetrics(ctx, gaugeBatch("api", "sampled.metric", ts1, vals1))
	require.NoError(t, err)
	require.Equal(t, int64(rows), acc1.Accepted)
	require.Zero(t, acc1.Rejected)

	first := fetchDefault(t, nonOwner, "sampled.metric", 0, 1000)
	require.Len(t, first, 1)
	assert.Len(t, first[0].Timestamps, rows, "window 1 is under budget: nothing sampled out")
	assert.Nil(t, first[0].ScaleFactors, "an unsampled batch carries no scale-factor column")

	// Window 2: the prior window observed 100 rows/s against a budget of 10, so the sampler keeps
	// roughly one in ten and weights each kept row by 10.
	now += sec

	ts2, vals2 := rampSeries(10_000, rows)
	acc2, err := origin.WriteMetrics(ctx, gaugeBatch("api", "sampled.metric", ts2, vals2))
	require.NoError(t, err)
	assert.Equal(t, int64(rows), acc2.Accepted, "sampled-out rows still count as accepted (represented)")
	assert.Zero(t, acc2.Rejected, "sampling is not rejection")

	st := origin.AdmissionStats("default")
	require.Positive(t, st.SampledDropped, "the clustered write path sampled")
	require.Less(t, st.SampledDropped, int64(rows))

	got := fetchDefault(t, nonOwner, "sampled.metric", 10_000, 20_000)
	require.Len(t, got, 1)

	b := got[0]
	kept := int64(len(b.Timestamps))

	assert.Less(t, kept, int64(rows), "fewer rows stored than written")
	assert.Equal(t, int64(rows), kept+st.SampledDropped, "kept + sampled-dropped == observed")
	require.NotNil(t, b.ScaleFactors, "the kept rows carry their weights across the fan-out")
	require.Len(t, b.ScaleFactors, int(kept))

	// Unbiasedness is the reason the feature exists: the weighted count and the weighted sum of a
	// sampled series must both recover the original total, not the kept subset's.
	var weightedCount, weightedSum float64

	for i := range b.Timestamps {
		w := b.ScaleFactor(i)
		assert.Greater(t, w, 1.0, "every kept row is weighted above 1")

		weightedCount += w
		weightedSum += b.Values[i] * w
	}

	// The estimator moves in steps of the scale factor (ceil(100/10) == 10), so the tolerance is a
	// few of those: an unweighted read would land on kept (~10), an order of magnitude outside it.
	assert.InDelta(t, float64(rows), weightedCount, 30, "weighted count recovers the written total")
	assert.InDelta(t, float64(rows), weightedSum, 30, "weighted sum recovers the written total")
}

// TestClusteredWriteWithoutSamplingBudgetIsLossless pins the negative case: the same clustered write
// with no MaxRowsPerSecond stores every row and attaches no scale factors at all. A nil ScaleFactors
// (not a run of 1s) is what tells every reader downstream that no sampling occurred, so the default
// has to stay byte-for-byte the pre-#463 behavior.
//
//nolint:paralleltest // owns an embedded etcd; runs serially
func TestClusteredWriteWithoutSamplingBudgetIsLossless(t *testing.T) {
	const (
		sec  = int64(1_000_000_000)
		rows = 100
	)

	nodes, nonOwner := samplingNodes(t, 0)
	origin := nodes["node-a"]
	ctx := context.Background()

	now := 5 * sec
	origin.now = func() int64 { return now }

	ts1, vals1 := rampSeries(1, rows)
	_, err := origin.WriteMetrics(ctx, gaugeBatch("api", "unsampled.metric", ts1, vals1))
	require.NoError(t, err)

	// Cross the window boundary the sampling test uses: with no budget the second window is not
	// sampled either, so the two writes differ only in the policy.
	now += sec

	ts2, vals2 := rampSeries(10_000, rows)
	acc, err := origin.WriteMetrics(ctx, gaugeBatch("api", "unsampled.metric", ts2, vals2))
	require.NoError(t, err)
	assert.Equal(t, int64(rows), acc.Accepted)
	assert.Zero(t, acc.Rejected)
	assert.Zero(t, origin.AdmissionStats("default").SampledDropped, "no budget ⇒ no sampling")

	got := fetchDefault(t, nonOwner, "unsampled.metric", 10_000, 20_000)
	require.Len(t, got, 1)
	assert.Len(t, got[0].Timestamps, rows, "every row is stored")
	assert.Nil(t, got[0].ScaleFactors, "no budget ⇒ no scale-factor column")
}

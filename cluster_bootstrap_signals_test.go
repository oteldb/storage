package storage

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/signal"
)

// TestClusterGainedOwnerBootstrapsEverySignal pins the gained-owner bootstrap's per-signal
// contract. Engines are created lazily per signal by the write path, so under live ingest the
// first replicated write of ONE signal lands on a gained owner before its first maintenance tick.
// A shard-wide "has any engine" gate reads that as "already bootstrapped" and never mirrors the
// shard's other signals — on any owner. Reads fail over while an original owner survives, and once
// the owner set has fully turned over every owner disclaims and the query returns an empty result
// with no error: committed data, silently unreachable.
//
//nolint:paralleltest // owns an embedded etcd; runs serially
func TestClusterGainedOwnerBootstrapsEverySignal(t *testing.T) {
	endpoint := startEtcd(t)
	ctx := context.Background()

	nodes := map[string]*Storage{
		"n1": openClusterNodePrivate(t, endpoint, "n1", 2),
		"n2": openClusterNodePrivate(t, endpoint, "n2", 2),
		"n3": openClusterNodePrivate(t, endpoint, "n3", 2),
	}

	require.Eventually(t, func() bool {
		for _, s := range nodes {
			if ringSize(s) != 3 {
				return false
			}
		}

		return true
	}, 15*time.Second, 50*time.Millisecond)

	owners := nodes["n1"].cluster.membership.Ring().Lookup([]byte("default"), 2)
	require.Len(t, owners, 2)

	var spareID string

	for id := range nodes {
		if id != owners[0].ID && id != owners[1].ID {
			spareID = id
		}
	}

	require.NotEmpty(t, spareID)
	primary, secondary, spare := nodes[owners[0].ID], nodes[owners[1].ID], nodes[spareID]

	// The shard carries two signals; both are flushed by the compaction owner and mirrored to the
	// secondary, so both are committed on every owner before the turnover starts.
	ts := []int64{100, 200, 300}
	_, err := primary.WriteMetrics(ctx, gaugeBatch("api", "http.requests", ts, []float64{1, 2, 3}))
	require.NoError(t, err)

	_, err = primary.WriteLogs(ctx, logBatch("api",
		[3]any{100, 9, "first"}, [3]any{200, 9, "second"}, [3]any{300, 9, "third"}))
	require.NoError(t, err)

	primary.maintain(ctx)
	secondary.maintain(ctx)

	require.False(t, spare.hasEngine(signal.Metric, "default"), "the spare holds nothing yet")
	require.False(t, spare.hasEngine(signal.Log, "default"), "the spare holds nothing yet")

	// The secondary is permanently lost; the ring promotes the spare into the owner set.
	require.NoError(t, secondary.Close(ctx))

	require.Eventually(t, func() bool {
		return ringSize(primary) == 2 && ringSize(spare) == 2
	}, 15*time.Second, 100*time.Millisecond, "membership drops the lost owner")

	require.True(t, spare.ownsShard("default"), "the spare is now an owner")

	// Live ingest: a replicated METRIC write reaches the new owner set and creates the spare's
	// metric engine before its first maintenance tick. This is the poison — the shard now holds an
	// engine while its logs have never been mirrored.
	require.Eventually(t, func() bool {
		_, _ = primary.WriteMetrics(ctx, gaugeBatch("api", "http.requests", []int64{400}, []float64{4}))

		return spare.hasEngine(signal.Metric, "default")
	}, 15*time.Second, 100*time.Millisecond, "the replicated write creates the gained owner's metric engine")

	require.False(t, spare.hasEngine(signal.Log, "default"), "only the written signal has an engine")

	// Drive maintenance until the spare has mirrored what it is going to. The metric side converges
	// through the ordinary replica path (it has an engine, so the maintenance loop sees it), so this
	// condition holds either way — the assertions after the turnover are the test.
	require.Eventually(t, func() bool {
		primary.maintain(ctx)
		spare.maintain(ctx)

		eng, ok := spare.lookupEngine("default")

		return ok && eng.PartCount() > 0
	}, 15*time.Second, 200*time.Millisecond, "the spare mirrors the shard's flushed metric parts")

	// Full turnover: the last original owner leaves, so the shard's only owner is the node that
	// gained it and nothing but its own backend can answer for the logs.
	require.NoError(t, primary.Close(ctx))

	require.Eventually(t, func() bool {
		return ringSize(spare) == 1
	}, 15*time.Second, 100*time.Millisecond, "the owner set has fully turned over")

	require.True(t, spare.ownsShard("default"))

	// The window stops short of the flushed maximum: past it the gained owner legitimately
	// disclaims, since the previous owners' unflushed head never reached it.
	bodies := logBodies(t, spare.LogFetcher("default"), fetch.Request{
		Signal: signal.Log, Start: 0, End: ts[len(ts)-1] - 1,
	})
	assert.Equal(t, []string{"first", "second"}, bodies, "the gained owner serves the stranded signal")
	assert.True(t, spare.hasEngine(signal.Log, "default"), "and it does so from a local engine")
}

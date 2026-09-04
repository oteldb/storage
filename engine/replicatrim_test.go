package engine_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/engine"
	"github.com/oteldb/storage/query/fetch"
)

const replicaPrefix = "default/metrics"

// flushedApartFromEachOther seeds a backend with two parts: one holding job="api" up to t=100, a
// later one holding only job="web" at t=100000. The part set's newest time therefore belongs to a
// series that says nothing about api's durability — the shape the replica trim has to tell apart.
func flushedApartFromEachOther(t *testing.T, be backend.Backend) {
	t.Helper()

	ctx := context.Background()
	owner := engine.New(engine.Config{Backend: be, Prefix: replicaPrefix})

	mustAppend(t, owner, mkSeries("job", "api"), 100, 1)
	require.NoError(t, owner.Flush(ctx))

	mustAppend(t, owner, mkSeries("job", "web"), 100000, 2)
	require.NoError(t, owner.Flush(ctx))
}

func fetchJob(t *testing.T, e *engine.Engine, job string) []*fetch.Batch {
	t.Helper()

	return fetchAll(t, e, fetch.Request{
		Start: 0, End: 1 << 60, Matchers: []fetch.Matcher{eqMatcher("job", job)},
	})
}

// TestRefreshReplicaTrimsPerSeries: the replica head is trimmed against each series' own newest
// flushed sample, not the newest across the part set. A sample for a series whose own flushed data
// is older survives the refresh; one already durable for its own series does not.
func TestRefreshReplicaTrimsPerSeries(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	be := backend.Memory()
	flushedApartFromEachOther(t, be)

	replica := engine.New(engine.Config{Backend: be, Prefix: replicaPrefix})
	// Replicated, acked, and not yet flushed by the owner: legal under api's own lateness bound
	// (its newest flushed sample is t=100), and older than what web has flushed.
	mustAppend(t, replica, mkSeries("job", "api"), 500, 3)
	// Already durable in web's own part — the trim must still drop this one.
	mustAppend(t, replica, mkSeries("job", "web"), 100000, 2)
	require.Equal(t, 2, replica.HeadSampleCount())

	require.NoError(t, replica.RefreshReplica(ctx))
	require.Equal(t, 2, replica.PartCount())
	assert.Equal(t, 1, replica.HeadSampleCount(),
		"web's flushed sample is dropped, api's unflushed one is kept")

	got := fetchJob(t, replica, "api")
	require.Len(t, got, 1)
	assert.Equal(t, []int64{100, 500}, got[0].Timestamps,
		"the late sample another series' flush does not cover is still served")
	assert.Equal(t, []float64{1, 3}, got[0].Values)

	web := fetchJob(t, replica, "web")
	require.Len(t, web, 1)
	assert.Equal(t, []int64{100000}, web[0].Timestamps, "the trimmed sample is served from the part, once")
}

// TestPromotedReplicaKeepsLateSample is the loss case: a replica that trimmed a late sample and is
// then promoted before the owner's next flush has nowhere to recover it from. Promotion is modeled
// by the replica flushing its own head — the promoted node's first flush — and a fresh reader
// loading the resulting parts.
func TestPromotedReplicaKeepsLateSample(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	be := backend.Memory()
	flushedApartFromEachOther(t, be)

	replica := engine.New(engine.Config{Backend: be, Prefix: replicaPrefix})
	mustAppend(t, replica, mkSeries("job", "api"), 500, 3)
	require.NoError(t, replica.RefreshReplica(ctx))

	require.NoError(t, replica.Flush(ctx))

	promoted := engine.New(engine.Config{Backend: be, Prefix: replicaPrefix})
	require.NoError(t, promoted.LoadParts(ctx))

	got := fetchJob(t, promoted, "api")
	require.Len(t, got, 1)
	assert.Equal(t, []int64{100, 500}, got[0].Timestamps,
		"the sample the promoted node flushed is durable, not lost with the old owner's head")
	assert.Equal(t, []float64{1, 3}, got[0].Values)
}

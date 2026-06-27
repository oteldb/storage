package storage

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-faster/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/internal/obs"
	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/reliability"
	"github.com/oteldb/storage/signal"
)

// fakeFetcher is a fetch.Fetcher that simulates a remote owner: an optional delay (slow/stuck peer,
// abandoned when ctx is canceled), an optional error (down peer), and an id so the winner is
// identifiable. It counts how many times it was called.
type fakeFetcher struct {
	id    uint64
	delay time.Duration
	err   error
	calls atomic.Int32
}

func (f *fakeFetcher) Fetch(ctx context.Context, _ fetch.Request) (fetch.Iterator, error) {
	f.calls.Add(1)

	if f.delay > 0 {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(f.delay):
		}
	}

	if f.err != nil {
		return nil, f.err
	}

	return fetch.NewSliceIterator([]*fetch.Batch{{ID: signal.SeriesID{Lo: f.id}}}), nil
}

// hedgeTestStore builds a minimal Storage carrying just the obs handle and a cluster node with the
// given retry profile — enough to exercise hedgedFetcher without a real cluster.
func hedgeTestStore(rc reliability.RetryConfig) *Storage {
	return &Storage{obs: obs.NewNop(), cluster: &clusterNode{retry: rc}}
}

func wonByID(t *testing.T, it fetch.Iterator, err error) uint64 {
	t.Helper()
	require.NoError(t, err)

	batches, derr := fetch.Drain(context.Background(), it)
	require.NoError(t, derr)
	require.Len(t, batches, 1)

	return batches[0].ID.Lo
}

// TestHedgedFetcherSlowOwnerRacedByFast: a slow first owner is bypassed by the hedge and the fast
// second owner wins — tail latency is bounded by the hedge delay, not the slow owner.
func TestHedgedFetcherSlowOwnerRacedByFast(t *testing.T) {
	t.Parallel()

	slow := &fakeFetcher{id: 1, delay: 3 * time.Second}
	fast := &fakeFetcher{id: 2}
	s := hedgeTestStore(reliability.RetryConfig{HedgeDelay: 30 * time.Millisecond, PerTryTimeout: 5 * time.Second, MaxAttempts: 2})

	h := hedgedFetcher{store: s, op: "read", remotes: []fetch.Fetcher{slow, fast}}

	start := time.Now()
	it, err := h.Fetch(context.Background(), fetch.Request{})
	won := wonByID(t, it, err)

	assert.Equal(t, uint64(2), won, "the hedged fast owner won")
	assert.Less(t, time.Since(start), time.Second, "did not wait for the slow owner")
	assert.Equal(t, int32(1), fast.calls.Load())
}

// TestHedgedFetcherFailsOverFromDownOwner: a down first owner (immediate error) fails over to the
// live second owner at once — durability without waiting for the hedge delay.
func TestHedgedFetcherFailsOverFromDownOwner(t *testing.T) {
	t.Parallel()

	down := &fakeFetcher{id: 1, err: errors.New("connection refused")}
	live := &fakeFetcher{id: 2}
	s := hedgeTestStore(reliability.RetryConfig{HedgeDelay: time.Hour, PerTryTimeout: 5 * time.Second, MaxAttempts: 2})

	h := hedgedFetcher{store: s, op: "read", remotes: []fetch.Fetcher{down, live}}

	start := time.Now()
	it, err := h.Fetch(context.Background(), fetch.Request{})
	won := wonByID(t, it, err)

	assert.Equal(t, uint64(2), won)
	assert.Less(t, time.Since(start), time.Second, "failover did not wait for the (1h) hedge delay")
}

// TestHedgedFetcherSingleOwnerRetries: one owner that fails transiently then succeeds is retried
// (the single-owner path uses bounded sequential retry, not hedging).
func TestHedgedFetcherSingleOwnerRetries(t *testing.T) {
	t.Parallel()

	flaky := &flakyFetcher{failFor: 1, id: 5}
	s := hedgeTestStore(reliability.RetryConfig{MaxAttempts: 3, PerTryTimeout: time.Second})

	h := hedgedFetcher{store: s, op: "read", remotes: []fetch.Fetcher{flaky}}

	it, err := h.Fetch(context.Background(), fetch.Request{})
	won := wonByID(t, it, err)
	assert.Equal(t, uint64(5), won)
	assert.Equal(t, int32(2), flaky.calls.Load(), "retried once after the transient failure")
}

func TestHedgedFetcherAllOwnersDown(t *testing.T) {
	t.Parallel()

	a := &fakeFetcher{id: 1, err: errors.New("refused")}
	b := &fakeFetcher{id: 2, err: errors.New("refused")}
	s := hedgeTestStore(reliability.RetryConfig{HedgeDelay: 10 * time.Millisecond, PerTryTimeout: time.Second, MaxAttempts: 2})

	h := hedgedFetcher{store: s, op: "read", remotes: []fetch.Fetcher{a, b}}
	_, err := h.Fetch(context.Background(), fetch.Request{})
	require.Error(t, err)
}

func TestHedgedFetcherNoOwners(t *testing.T) {
	t.Parallel()

	s := hedgeTestStore(reliability.Default())
	h := hedgedFetcher{store: s, op: "read", remotes: nil}
	_, err := h.Fetch(context.Background(), fetch.Request{})
	require.ErrorContains(t, err, "no reachable owners")
}

// flakyFetcher fails its first failFor calls (transiently) then succeeds.
type flakyFetcher struct {
	id      uint64
	failFor int32
	calls   atomic.Int32
}

func (f *flakyFetcher) Fetch(_ context.Context, _ fetch.Request) (fetch.Iterator, error) {
	if f.calls.Add(1) <= f.failFor {
		return nil, errors.New("transient")
	}

	return fetch.NewSliceIterator([]*fetch.Batch{{ID: signal.SeriesID{Lo: f.id}}}), nil
}

// partition abruptly stops a node's cluster HTTP server (simulating a network partition or crash)
// while it is still registered in membership, so peers keep routing to it and must fail over.
func partition(t *testing.T, s *Storage) {
	t.Helper()
	require.NoError(t, s.cluster.server.Close())
}

// TestClusterReadSurvivesDownOwner is the end-to-end durability check: with a write replicated to
// two owners, partitioning one owner still lets a non-owner read succeed — the hedged fetcher fails
// over to the live replica instead of surfacing the dead peer's error.
//
//nolint:paralleltest // owns an embedded etcd; runs serially
func TestClusterReadSurvivesDownOwner(t *testing.T) {
	endpoint := startEtcd(t)
	ctx := context.Background()

	ids := []string{"node-a", "node-b", "node-c"}
	nodes := make(map[string]*Storage, len(ids))
	for _, id := range ids {
		nodes[id] = openClusterNodeWith(t, endpoint, id, backend.Memory(), WithRetry(reliability.LossyEnvironment()))
	}

	a := nodes["node-a"]
	require.Eventually(t, func() bool {
		return len(a.cluster.membership.Members()) == 3
	}, 10*time.Second, 50*time.Millisecond)

	_, err := a.WriteMetrics(ctx, gaugeBatch("api", "http.requests", []int64{100, 200}, []float64{1, 2}))
	require.NoError(t, err)

	owners := a.cluster.membership.Ring().Lookup([]byte("default"), 2)
	ownerSet := map[string]bool{owners[0].ID: true, owners[1].ID: true}

	var requesterID string
	for _, id := range ids {
		if !ownerSet[id] {
			requesterID = id
		}
	}
	require.NotEmpty(t, requesterID, "exactly one non-owner with RF=2 over 3 nodes")

	// Take down the first owner the read would try; the fetcher must fail over to the second.
	partition(t, nodes[owners[0].ID])

	start := time.Now()
	it, err := nodes[requesterID].Fetcher("default").Fetch(ctx, fetch.Request{
		Start: 0, End: 1 << 60, Matchers: []fetch.Matcher{nameMatcher("http.requests")},
	})
	require.NoError(t, err)
	batches, err := fetch.Drain(ctx, it)
	require.NoError(t, err)

	require.NotEmpty(t, batches, "data still readable from the live replica")
	assert.Less(t, time.Since(start), 3*time.Second, "failover was prompt, not a full-timeout stall")
}

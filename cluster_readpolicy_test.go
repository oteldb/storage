package storage

import (
	"context"
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/cluster"
	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/signal"
)

// A read overlapping a part an owner cannot read is not answerable by that owner, and the interesting
// half of the policy is what happens when *no* owner can answer: the fan-out must fail rather than
// merge nothing into a successful empty result. These tests pin both halves against a real want —
// a flushed part whose objects are destroyed and whose index reload records the obligation.

// readPolicyCluster is three nodes over their own memory backends (RF 2, so a shard has two owners),
// with the backends handed back so a test can destroy a part behind an owner's back.
type readPolicyCluster struct {
	nodes    map[string]*Storage
	backends map[string]backend.Backend
	shard    signal.TenantID
}

func newReadPolicyCluster(t *testing.T, endpoint string) *readPolicyCluster {
	t.Helper()

	c := &readPolicyCluster{
		nodes:    make(map[string]*Storage, 3),
		backends: make(map[string]backend.Backend, 3),
	}

	for _, id := range []string{"node-a", "node-b", "node-c"} {
		be := backend.Memory()
		c.backends[id] = be
		c.nodes[id] = openClusterNodeWith(t, endpoint, id, be)
	}

	awaitMembership(t, c.nodes)

	a := c.nodes["node-a"]
	c.shard = shardKeyOf("default", 0, a.cluster.shardCount())

	return c
}

// write ingests two log records at 100 and 200 and waits until both of the shard's owners hold them
// as a part: a want can only name a part that is on disk.
func (c *readPolicyCluster) write(t *testing.T) {
	t.Helper()
	ctx := context.Background()

	_, err := c.nodes["node-a"].WriteLogs(ctx,
		logBatch("api", [3]any{100, 9, "first"}, [3]any{200, 17, "second"}))
	require.NoError(t, err)

	// The replica reaches the shard asynchronously (a replicated write, then part sync), so one
	// maintenance pass is not enough.
	require.Eventually(t, func() bool {
		for _, s := range c.nodes {
			_ = s.Admin().MaintainNow(ctx)
		}

		return len(c.partHolders()) >= 2
	}, 20*time.Second, 100*time.Millisecond, "the shard's two owners each flush a part")
}

// partHolders is the nodes holding the shard's log engine with at least one part.
func (c *readPolicyCluster) partHolders() []string {
	var out []string

	for id, s := range c.nodes {
		if eng, ok := s.lookupRecordEngine(signal.Log, c.shard); ok && len(eng.Parts()) > 0 {
			out = append(out, id)
		}
	}

	slices.Sort(out)

	return out
}

// loseOldestPart destroys the objects of the node's oldest log part and reloads its index, which is
// how an owner really discovers a part it cannot read: the entry is still named, the bytes are gone,
// and the reload records the repair obligation. Reports whether the node had a part to lose.
func (c *readPolicyCluster) loseOldestPart(t *testing.T, id string) bool {
	t.Helper()
	ctx := context.Background()

	eng, ok := c.nodes[id].lookupRecordEngine(signal.Log, c.shard)
	if !ok {
		return false
	}

	parts := eng.Parts()
	if len(parts) == 0 {
		return false
	}

	oldest := parts[0]
	for _, p := range parts {
		if p.MinTime < oldest.MinTime {
			oldest = p
		}
	}

	be := c.backends[id]

	keys, err := be.List(ctx, oldest.ID)
	require.NoError(t, err)
	require.NotEmpty(t, keys)

	for _, k := range keys {
		require.NoError(t, be.Delete(ctx, k))
	}

	require.NoError(t, eng.LoadParts(ctx))
	require.Positive(t, eng.Stats().WantedParts, "the reload records what it could not read")

	return true
}

// owners is the nodes a read can be served by, and so the ones a want has to be planted on to
// starve it.
func (c *readPolicyCluster) owners(t *testing.T) []string {
	t.Helper()

	out := c.partHolders()
	require.GreaterOrEqual(t, len(out), 2, "RF 2 puts the shard on two owners")

	return out
}

// logRows counts the records a fetch returned.
func logRows(batches []*fetch.Batch) int {
	n := 0
	for _, b := range batches {
		n += len(b.Timestamps)
	}

	return n
}

func fetchLogs(ctx context.Context, s *Storage, start, end int64) ([]*fetch.Batch, error) {
	it, err := s.LogFetcher("default").Fetch(ctx, fetch.Request{Signal: signal.Log, Start: start, End: end})
	if err != nil {
		return nil, err
	}

	return fetch.Drain(ctx, it)
}

// TestReadOverlappingWantFailsOverToCompleteOwner is the good outcome and the common one: one owner
// cannot read a part, so it disclaims the window, and the peer that still has it answers. The query
// succeeds with everything in it — losing a part on one node is not a user-visible fault.
//
//nolint:paralleltest // owns an embedded etcd; runs serially
func TestReadOverlappingWantFailsOverToCompleteOwner(t *testing.T) {
	endpoint := startEtcd(t)
	ctx := context.Background()

	c := newReadPolicyCluster(t, endpoint)
	c.write(t)

	before, err := fetchLogs(ctx, c.nodes["node-a"], 0, 1<<62)
	require.NoError(t, err)
	require.Equal(t, 2, logRows(before))

	damaged := c.owners(t)[0]
	require.True(t, c.loseOldestPart(t, damaged))

	got, err := fetchLogs(ctx, c.nodes[damaged], 0, 1<<62)
	require.NoError(t, err, "one damaged owner is a failover, not a query error")
	assert.Equal(t, 2, logRows(got), "the complete owner answers what the damaged one cannot")
}

// TestReadFailsWhenEveryOwnerIsIncomplete is Decision 7, and the reason the two disclaim kinds have
// to stay apart. Every owner holds the shard and every owner is missing the same part, so there is no
// complete answer anywhere: the read must fail. Collapsing it to an empty success — which is what a
// fan-out does when it reads "every owner disclaims" as "nothing holds this shard" — would hand the
// caller a wrong number it cannot tell from real absence.
//
//nolint:paralleltest // owns an embedded etcd; runs serially
func TestReadFailsWhenEveryOwnerIsIncomplete(t *testing.T) {
	endpoint := startEtcd(t)
	ctx := context.Background()

	c := newReadPolicyCluster(t, endpoint)
	c.write(t)

	for _, id := range c.owners(t) {
		require.True(t, c.loseOldestPart(t, id))
	}

	for id := range c.nodes {
		_, err := fetchLogs(ctx, c.nodes[id], 0, 1<<62)
		require.Error(t, err, "%s: no owner can answer the window, so the read fails", id)
		require.ErrorIs(t, err, cluster.ErrShardIncomplete, "%s", id)
	}
}

// TestReadOutsideTheWantIsServedLocally: the want carries the lost part's own bounds, so a window
// clear of it is answered here even while the shard is unrepaired. Without that bound the policy
// would take a shard offline for every query rather than for the range it actually lost.
//
//nolint:paralleltest // owns an embedded etcd; runs serially
func TestReadOutsideTheWantIsServedLocally(t *testing.T) {
	endpoint := startEtcd(t)
	ctx := context.Background()

	c := newReadPolicyCluster(t, endpoint)
	c.write(t)

	for _, id := range c.owners(t) {
		require.True(t, c.loseOldestPart(t, id))
	}

	a := c.nodes["node-a"]

	require.Error(t, a.canAnswer(ctx, rpcOpRead, signal.Log, c.shard, true, 0, 150),
		"the lost part's own range is disclaimed")
	require.NoError(t, a.canAnswer(ctx, rpcOpRead, signal.Log, c.shard, true, 300, 400),
		"a window past it is not")

	got, err := fetchLogs(ctx, a, 300, 400)
	require.NoError(t, err, "and the query is served, not failed over into a cluster-wide failure")
	assert.Zero(t, logRows(got))
}

// TestShardNoOwnerHoldsStillReadsEmpty guards the regression the policy could cause: an empty result
// is still the right answer when the disclaim is genuine absence. A tenant nothing was ever written
// for holds no data anywhere, and must read as empty rather than as a failure.
//
//nolint:paralleltest // owns an embedded etcd; runs serially
func TestShardNoOwnerHoldsStillReadsEmpty(t *testing.T) {
	endpoint := startEtcd(t)
	ctx := context.Background()

	c := newReadPolicyCluster(t, endpoint)
	c.write(t)

	for _, id := range c.owners(t) {
		require.True(t, c.loseOldestPart(t, id))
	}

	it, err := c.nodes["node-a"].LogFetcher("untouched").Fetch(ctx,
		fetch.Request{Signal: signal.Log, Start: 0, End: 1 << 62})
	require.NoError(t, err, "a shard no owner holds is absence, not incompleteness")

	got, err := fetch.Drain(ctx, it)
	require.NoError(t, err)
	assert.Zero(t, logRows(got))
}

// TestSingleNodeReadFailsWithNothingToFailOverTo is the same policy with the failover removed: the
// one node holding the shard cannot answer the window and there is no peer to ask, so the read
// fails. Serving the local engine here — which is what a guard that requires a remote does — would
// answer the query short with no error, and nothing downstream could tell.
//
//nolint:paralleltest // owns an embedded etcd; runs serially
func TestSingleNodeReadFailsWithNothingToFailOverTo(t *testing.T) {
	endpoint := startEtcd(t)
	ctx := context.Background()

	s := openClusterNodePrivate(t, endpoint, "solo", 1)

	_, err := s.WriteLogs(ctx, logBatch("api", [3]any{100, 9, "first"}, [3]any{200, 17, "second"}))
	require.NoError(t, err)
	require.NoError(t, s.Admin().MaintainNow(ctx))

	shard := shardKeyOf("default", 0, s.cluster.shardCount())

	eng, ok := s.lookupRecordEngine(signal.Log, shard)
	require.True(t, ok)

	parts := eng.Parts()
	require.NotEmpty(t, parts)

	keys, err := s.backend.List(ctx, parts[0].ID)
	require.NoError(t, err)
	require.NotEmpty(t, keys)

	for _, k := range keys {
		require.NoError(t, s.backend.Delete(ctx, k))
	}

	require.NoError(t, eng.LoadParts(ctx))
	require.Positive(t, eng.Stats().WantedParts)

	_, err = fetchLogs(ctx, s, 0, 1<<62)
	require.ErrorIs(t, err, cluster.ErrShardIncomplete, "with no owner to ask, the read fails here")
}

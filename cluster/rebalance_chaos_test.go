package cluster_test

// Rebalance-under-load fault testing for the object-store-native ownership handoff. When the ring
// membership changes, a shard's owners change but its data does NOT move — it lives at a
// shard-derived backend prefix, and the node that gains the shard serves it by opening a fresh
// engine over that prefix and reconstructing the part set from the shared backend
// ([engine.Engine.LoadParts]). These tests assert that handoff is lossless: across single and
// rolling membership changes (with ongoing ingest), the new owner reads every flushed sample, and
// the move stays minimal. They complement the pure minimal-move checks in cluster/rebalance.

import (
	"context"
	"math"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/cluster/rebalance"
	"github.com/oteldb/storage/cluster/ring"
	"github.com/oteldb/storage/engine"
	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/signal"
)

func ringNodes(ids ...string) []ring.Node {
	out := make([]ring.Node, len(ids))
	for i, id := range ids {
		out[i] = ring.Node{ID: id}
	}

	return out
}

// clusterSeries is the single series every shard's samples carry (matched by chaosMatcher).
func clusterSeries() signal.Series {
	return signal.Series{Attributes: signal.NewAttributes(
		signal.KeyValue{Key: []byte("job"), Value: signal.StringValue([]byte("chaos"))})}
}

// engineAt opens a fresh engine over (be, prefix) and reconstructs its durable parts from the
// shared backend — exactly what a node does when it gains ownership of a shard (no in-memory state
// carried from the writer).
func engineAt(t *testing.T, be backend.Backend, prefix string) *engine.Engine {
	t.Helper()

	e := engine.New(engine.Config{Backend: be, Prefix: prefix})
	require.NoError(t, e.LoadParts(context.Background()))

	return e
}

// countSamples fetches the whole series from e and returns the number of samples.
func countSamples(t *testing.T, e *engine.Engine) int {
	t.Helper()

	it, err := e.Fetch(context.Background(), fetch.Request{
		Start: math.MinInt64, End: math.MaxInt64, Matchers: []fetch.Matcher{chaosMatcher()},
	})
	require.NoError(t, err)

	batches, err := fetch.Drain(context.Background(), it)
	require.NoError(t, err)

	n := 0
	for _, b := range batches {
		n += len(b.Timestamps)
	}

	return n
}

// TestRebalanceHandoffPreservesData ingests + flushes a set of shards under one membership, adds a
// node, and asserts every shard that gained an owner is fully readable by a *fresh* engine the new
// owner opens over the shared backend — the object-store-native handoff, lossless and stateless.
func TestRebalanceHandoffPreservesData(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	be := backend.Memory()
	series := clusterSeries()
	const rf, perShard = 3, 50

	shards := make([]string, 24)
	for i := range shards {
		shards[i] = "shard-" + strconv.Itoa(i)
	}

	// The writer for each shard flushes its samples to the shared backend.
	for _, sh := range shards {
		w := engine.New(engine.Config{Backend: be, Prefix: sh})
		for ts := int64(1); ts <= perShard; ts++ {
			ok, err := w.Append(series, ts, float64(ts))
			require.NoError(t, err)
			require.True(t, ok)
		}
		require.NoError(t, w.Flush(ctx))
	}

	old := ring.New(ringNodes("a", "b", "c", "d", "e")...)
	next := old.With(ring.Node{ID: "f"})

	plan := rebalance.Plan(shards, old, next, rf)
	require.NotEmpty(t, plan, "adding a node must reassign some shards")
	assert.Less(t, len(plan), len(shards), "rebalance must be minimal, not a full reshuffle")

	gained := 0
	for _, r := range plan {
		for _, owner := range r.Added {
			// The node that gained this shard opens it fresh from the shared backend.
			e := engineAt(t, be, r.Shard)
			assert.Equalf(t, perShard, countSamples(t, e),
				"new owner %s of %s must read all flushed data after handoff", owner, r.Shard)
			gained++
		}
	}
	require.Positive(t, gained, "the plan should have at least one gained owner")
}

// TestRebalanceRollingHandoffNoLoss runs ongoing ingest across a sequence of membership changes:
// on each change, shards whose primary moved are handed off (the old owner drains via Close, the
// new owner opens fresh from the backend), then load continues. After every change, each shard's
// current owner must read every sample ever flushed to it — no data lost across rolling rebalance.
func TestRebalanceRollingHandoffNoLoss(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	be := backend.Memory()
	series := clusterSeries()
	const rf = 3

	shards := make([]string, 8)
	for i := range shards {
		shards[i] = "shard-" + strconv.Itoa(i)
	}

	primaryOf := func(r *ring.Ring, sh string) string {
		owners := r.Lookup([]byte(sh), rf)
		require.NotEmpty(t, owners)

		return owners[0].ID
	}

	cur := ring.New(ringNodes("a", "b", "c")...)
	live := make(map[string]*engine.Engine, len(shards))
	for _, sh := range shards {
		live[sh] = engine.New(engine.Config{Backend: be, Prefix: sh})
	}

	var nextTs int64
	expected := map[string]int{}
	ingest := func(sh string, n int) {
		e := live[sh]
		for range n {
			nextTs++
			ok, err := e.Append(series, nextTs, float64(nextTs))
			require.NoError(t, err)
			require.True(t, ok)
		}
		require.NoError(t, e.Flush(ctx)) // flush so the data is durable before any handoff
		expected[sh] += n
	}

	for _, sh := range shards {
		ingest(sh, 10)
	}

	// A membership sequence that moves primaries around: add two nodes, then remove one.
	grow1 := cur.With(ring.Node{ID: "d"})
	grow2 := grow1.With(ring.Node{ID: "e"})
	shrink := grow2.Without("a")
	changes := []*ring.Ring{grow1, grow2, shrink}

	handoffs := 0
	for _, next := range changes {
		for _, sh := range shards {
			if primaryOf(cur, sh) == primaryOf(next, sh) {
				continue
			}
			// Primary moved: the old owner drains its head (Close flushes), the new owner opens fresh.
			require.NoError(t, live[sh].Close(ctx))
			live[sh] = engineAt(t, be, sh)
			handoffs++
		}
		cur = next

		for _, sh := range shards {
			ingest(sh, 10) // load continues on the (possibly new) owners
		}

		for _, sh := range shards {
			assert.Equalf(t, expected[sh], countSamples(t, live[sh]),
				"shard %s lost data after a rebalance handoff", sh)
		}
	}

	require.Positive(t, handoffs, "the membership changes should have moved at least one shard's primary")
}

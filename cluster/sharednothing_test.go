package cluster_test

// Shared-nothing (cluster.Config.PrivateBackend) counterpart of rebalance_chaos_test.go. That
// harness gives every node ONE backend, so a shard reassigned from A to B is readable from B for
// free — the object store already held the parts. Here each node gets a backend only it can read,
// served to peers over the real partsync endpoints, which is what PrivateBackend declares. The
// handoff is then a copy, and the tests below pin that it actually happens:
//
//   - a gainer that does not mirror reads NOTHING for the shard it now owns (the silent failure
//     mode of running private disks with PrivateBackend unset — see oteldb/oteldb#1262);
//   - a gainer that mirrors from the shard's previous owners reads every flushed sample;
//   - a replica restarted with only its flushed parts converges back to the owner via the syncer.
//
// Everything is driven by explicit calls — no maintenance loops, no notifies, no waiting — so the
// ordering is fixed and the tests are deterministic.

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/cluster/partsync"
	"github.com/oteldb/storage/cluster/rebalance"
	"github.com/oteldb/storage/cluster/ring"
	"github.com/oteldb/storage/engine"
)

// privateNode is one shared-nothing node: a backend no peer can read directly, the partsync
// endpoints serving it, and a syncer mirroring peer prefixes into it.
type privateNode struct {
	id     string
	be     backend.Backend
	addr   string
	syncer *partsync.Syncer
}

func newPrivateNode(t *testing.T, id string) *privateNode {
	t.Helper()

	be := backend.Memory()

	mux := http.NewServeMux()
	mux.Handle(partsync.ListPath, partsync.ListHandler(be))
	mux.Handle(partsync.ObjectPath, partsync.ObjectHandler(be))

	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	return &privateNode{
		id:     id,
		be:     be,
		addr:   strings.TrimPrefix(srv.URL, "http://"),
		syncer: partsync.New(be, &partsync.Client{}),
	}
}

// mirror pulls prefix from peers into this node's backend, as the maintenance loop's replica pass
// does (strict=false: any differing peer copy at least as new is installed).
func (n *privateNode) mirror(t *testing.T, prefix string, peers []*privateNode) partsync.Stats {
	t.Helper()

	addrs := make([]string, 0, len(peers))
	for _, p := range peers {
		if p.id != n.id {
			addrs = append(addrs, p.addr)
		}
	}

	st, err := n.syncer.Sync(context.Background(), prefix, addrs, false, nil)
	require.NoError(t, err)

	return st
}

// enginePrefix is where a shard's parts live on whichever node holds them.
func enginePrefix(shard string) string { return shard + "/metrics" }

// flushInto appends n samples starting at first into a fresh engine over node's own backend and
// flushes them, returning the engine (the shard's writer).
func flushInto(t *testing.T, node *privateNode, shard string, first int64, n int) *engine.Engine {
	t.Helper()

	e := engineAt(t, node.be, enginePrefix(shard))
	appendFlush(t, e, first, n)

	return e
}

func appendFlush(t *testing.T, e *engine.Engine, first int64, n int) {
	t.Helper()

	series := clusterSeries()
	for i := range n {
		ok, err := e.Append(series, first+int64(i), float64(first+int64(i)))
		require.NoError(t, err)
		require.True(t, ok)
	}

	require.NoError(t, e.Flush(context.Background()))
}

func nodeIDs(nodes map[string]*privateNode) []ring.Node {
	ids := make([]string, 0, len(nodes))
	for id := range nodes {
		ids = append(ids, id)
	}

	return ringNodes(ids...)
}

// TestSharedNothingRebalanceHandoffMirrorsParts is property 1 of shared-nothing mode: a shard
// reassigned to a node by a ring change is fully readable from that node afterwards, because the
// parts were mirrored — not because they were assumed to sit in a store both nodes can see.
//
// The pre-mirror assertion is the important half. It is exactly what a cluster running private
// disks WITHOUT PrivateBackend does at every handoff: the gainer opens an engine over its own empty
// backend, finds no parts, and answers the shard with silence.
func TestSharedNothingRebalanceHandoffMirrorsParts(t *testing.T) {
	t.Parallel()

	const rf, perShard = 3, 40

	nodes := map[string]*privateNode{}
	for _, id := range []string{"a", "b", "c", "d", "e"} {
		nodes[id] = newPrivateNode(t, id)
	}

	shards := make([]string, 16)
	for i := range shards {
		shards[i] = "shard-" + strconv.Itoa(i)
	}

	old := ring.New(nodeIDs(nodes)...)

	// Under the old membership every owner holds the shard's parts on its own disk: the primary
	// flushes, the other owners mirror from it.
	for _, sh := range shards {
		owners := old.Lookup([]byte(sh), rf)
		require.Len(t, owners, rf)

		flushInto(t, nodes[owners[0].ID], sh, 1, perShard)

		for _, o := range owners[1:] {
			st := nodes[o.ID].mirror(t, enginePrefix(sh), []*privateNode{nodes[owners[0].ID]})
			require.Truef(t, st.Synced, "owner %s mirrors shard %s before the rebalance", o.ID, sh)
		}
	}

	nodes["f"] = newPrivateNode(t, "f")
	next := old.With(ring.Node{ID: "f"})

	plan := rebalance.Plan(shards, old, next, rf)
	require.NotEmpty(t, plan, "adding a node must reassign some shards")

	gained := 0

	for _, r := range plan {
		prev := old.Lookup([]byte(r.Shard), rf)
		peers := make([]*privateNode, 0, len(prev))

		for _, o := range prev {
			peers = append(peers, nodes[o.ID])
		}

		for _, id := range r.Added {
			gainer := nodes[id]

			// Before mirroring: the gainer owns the shard and holds nothing for it. With a shared
			// store this read would already return everything; here it is empty, and a node that
			// believes its backend is shared would serve that emptiness as the whole shard.
			assert.Zerof(t, countSamples(t, engineAt(t, gainer.be, enginePrefix(r.Shard))),
				"%s must hold nothing for %s before the parts are mirrored", id, r.Shard)

			st := gainer.mirror(t, enginePrefix(r.Shard), peers)
			require.Truef(t, st.Synced, "%s mirrors %s from its previous owners", id, r.Shard)
			assert.Positivef(t, st.Copied, "%s copied objects for %s", id, r.Shard)

			assert.Equalf(t, perShard, countSamples(t, engineAt(t, gainer.be, enginePrefix(r.Shard))),
				"%s must read every flushed sample of %s after the handoff", id, r.Shard)

			gained++
		}
	}

	require.Positive(t, gained, "the plan should have at least one gained owner")
}

// TestSharedNothingRollingHandoffNoLoss runs the handoff across a sequence of membership changes
// with ongoing ingest: after every change each shard's owners mirror from the previous owners, and
// every owner must read every sample ever flushed to the shard. This is the shared-nothing analog
// of TestRebalanceRollingHandoffNoLoss, where the same guarantee comes free from the shared store.
func TestSharedNothingRollingHandoffNoLoss(t *testing.T) {
	t.Parallel()

	const rf, perRound = 2, 10

	nodes := map[string]*privateNode{
		"a": newPrivateNode(t, "a"),
		"b": newPrivateNode(t, "b"),
		"c": newPrivateNode(t, "c"),
	}

	shards := []string{"shard-0", "shard-1", "shard-2", "shard-3"}

	cur := ring.New(nodeIDs(nodes)...)
	nextTs := int64(1)
	expected := map[string]int{}

	// converge makes every current owner of every shard hold the shard's full part set, mirroring
	// from the other owners of the ring that produced those parts.
	converge := func(prev, now *ring.Ring) {
		t.Helper()

		for _, sh := range shards {
			sources := make([]*privateNode, 0, rf)
			for _, o := range prev.Lookup([]byte(sh), rf) {
				sources = append(sources, nodes[o.ID])
			}

			for _, o := range now.Lookup([]byte(sh), rf) {
				nodes[o.ID].mirror(t, enginePrefix(sh), sources)
			}
		}
	}

	// Round 0: the primary of the initial ring flushes, the other owners mirror.
	for _, sh := range shards {
		primary := cur.Lookup([]byte(sh), rf)[0]
		flushInto(t, nodes[primary.ID], sh, nextTs, perRound)
		nextTs += perRound
		expected[sh] += perRound
	}
	converge(cur, cur)

	nodes["d"] = newPrivateNode(t, "d")
	grow := cur.With(ring.Node{ID: "d"})
	shrink := grow.Without("a")

	gained := 0

	for _, next := range []*ring.Ring{grow, shrink} {
		gained += len(rebalance.Plan(shards, cur, next, rf))

		converge(cur, next) // the gainers mirror from the owners that produced the parts

		// Ingest continues on the new primary, which appends to the part set it just adopted.
		for _, sh := range shards {
			primary := next.Lookup([]byte(sh), rf)[0]
			e := engineAt(t, nodes[primary.ID].be, enginePrefix(sh))
			appendFlush(t, e, nextTs, perRound)
			nextTs += perRound
			expected[sh] += perRound
		}

		converge(next, next) // the secondaries mirror the new flush

		for _, sh := range shards {
			for _, o := range next.Lookup([]byte(sh), rf) {
				assert.Equalf(t, expected[sh], countSamples(t, engineAt(t, nodes[o.ID].be, enginePrefix(sh))),
					"owner %s of %s lost data across the shared-nothing handoff", o.ID, sh)
			}
		}

		cur = next
	}

	require.Positive(t, gained, "the membership changes must actually reassign shards")
}

// TestSharedNothingReplicaRestartConverges is property 2: a replica that comes back holding only
// its flushed parts — a restart drops the in-memory head, and a secondary never logs one — catches
// up to the owner through the syncer alone, with no shared store to fall back on.
func TestSharedNothingReplicaRestartConverges(t *testing.T) {
	t.Parallel()

	const shard = "shard-restart"

	owner, replica := newPrivateNode(t, "owner"), newPrivateNode(t, "replica")

	oe := flushInto(t, owner, shard, 1, 10)

	replica.mirror(t, enginePrefix(shard), []*privateNode{owner})
	require.Equal(t, 10, countSamples(t, engineAt(t, replica.be, enginePrefix(shard))))

	// The restart: the replica's engine is dropped and rebuilt from its own backend, so whatever
	// was only in the head is gone and the flushed parts are all it has.
	restarted := engineAt(t, replica.be, enginePrefix(shard))
	require.Equal(t, 10, countSamples(t, restarted), "the flushed parts survived the restart")

	// The owner moves on while the replica is behind.
	appendFlush(t, oe, 11, 10)
	appendFlush(t, oe, 21, 10)

	st := replica.mirror(t, enginePrefix(shard), []*privateNode{owner})
	require.True(t, st.Synced, "the restarted replica mirrors the owner's newer parts")

	require.NoError(t, restarted.RefreshReplica(context.Background()))
	assert.Equal(t, 30, countSamples(t, restarted),
		"the restarted replica converges to the owner's data through the syncer")
}

// TestSharedNothingOwnerBackfillsStrictly pins the owner half of the sync contract: a compaction
// owner backfills only strictly-newer peer copies, so a stale peer can never regress the owner's
// own index, while a genuinely newer peer copy (the previous owner's, after a handoff) is adopted.
func TestSharedNothingOwnerBackfillsStrictly(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	const shard = "shard-strict"

	ahead, behind := newPrivateNode(t, "ahead"), newPrivateNode(t, "behind")

	flushInto(t, ahead, shard, 1, 10)

	behind.mirror(t, enginePrefix(shard), []*privateNode{ahead})

	// The owner ("ahead") flushes again, so the peer copy is now strictly older.
	ae := engineAt(t, ahead.be, enginePrefix(shard))
	appendFlush(t, ae, 11, 10)

	st, err := ahead.syncer.Sync(ctx, enginePrefix(shard), []string{behind.addr}, true, nil)
	require.NoError(t, err)
	assert.False(t, st.Synced, "an older peer copy never overwrites the owner's index")
	assert.Equal(t, 20, countSamples(t, engineAt(t, ahead.be, enginePrefix(shard))))

	// The other direction: "behind" is promoted to owner and backfills the strictly newer copy.
	st, err = behind.syncer.Sync(ctx, enginePrefix(shard), []string{ahead.addr}, true, nil)
	require.NoError(t, err)
	require.True(t, st.Synced, "a strictly newer peer copy is adopted by the gaining owner")
	assert.Equal(t, 20, countSamples(t, engineAt(t, behind.be, enginePrefix(shard))))
}

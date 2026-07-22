package storage

import (
	"context"
	"fmt"
	"math/rand/v2"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/query/fetch"
)

// TestClusterECChaos is the closing proof of the shared-nothing + erasure-coding line
// (issue 108, phase 2): a 6-node ec(4,2) cluster over private per-node backends, spread over
// three racks, driven with randomized maintenance scheduling —
//
//  1. ingests several high-entropy series and converges to the coded steady state (each node
//     holding exactly its shard slot, the storage target);
//  2. permanently kills two RANDOM nodes — the scheme's full tolerance, possibly including the
//     compaction primary and both nodes of one rack — and proves every surviving node still
//     serves every sample (reads reconstruct; repair rebuilds the reachable slots);
//  3. joins a fresh replacement node and proves it bootstraps from the claims (engine, mirror,
//     slot) and serves every sample itself.
//
// Scheduling order is deterministic per seed but adversarial: every settle round visits the
// nodes in a different shuffled order, the pattern that surfaced the prune/livePart bug.
//
//nolint:paralleltest // owns an embedded etcd; runs serially
func TestClusterECChaos(t *testing.T) {
	if testing.Short() {
		t.Skip("multi-node chaos e2e")
	}

	// Each seed produces a different scheduling order and kill-set (which two nodes die —
	// possibly a whole rack, possibly the compaction primary).
	for _, seed := range []uint64{42, 7, 1001} {
		t.Run(fmt.Sprintf("seed=%d", seed), func(t *testing.T) { runECChaos(t, seed) })
	}
}

//nolint:contextcheck,maintidx // one linear chaos narrative; queryEngine is a ctx-less test helper
func runECChaos(t *testing.T, seed uint64) {
	t.Helper()

	endpoint := startEtcd(t)
	ctx := context.Background()
	rng := rand.New(rand.NewPCG(seed, seed+1))

	// 6 nodes, 3 racks × 2 — rack-safe for ec(4,2): MinZones=3, ≤ Parity shards per rack.
	nodes := map[string]*Storage{}
	for i := range 6 {
		id := fmt.Sprintf("n%d", i+1)
		rack := fmt.Sprintf("rack%d", i/2+1)
		nodes[id] = openClusterNodeECDomains(t, endpoint, id, []string{rack}, 4, 2)
	}

	require.Eventually(t, func() bool {
		for _, s := range nodes {
			if len(s.cluster.membership.Members()) != 6 {
				return false
			}
		}

		return true
	}, 20*time.Second, 100*time.Millisecond, "all six nodes see the full membership")

	anyNode := nodes["n1"]

	// settle runs one randomized maintenance round across the given nodes.
	settle := func(pool map[string]*Storage) {
		ids := make([]string, 0, len(pool))
		for id := range pool {
			ids = append(ids, id)
		}

		rng.Shuffle(len(ids), func(i, j int) { ids[i], ids[j] = ids[j], ids[i] })

		for _, id := range ids {
			pool[id].maintain(ctx)
		}
	}

	// Ingest three series, each flushed into its own part (a maintenance round between writes),
	// written to the tenant's primary directly.
	series := map[string][]float64{}

	for i := range 3 {
		name := fmt.Sprintf("chaos.metric.%d", i)
		ts, vals := ecPayload(4096, uint64(21+i))
		series[name] = vals

		primary, ok := anyNode.cluster.membership.Ring().Primary([]byte("default"))
		require.True(t, ok)
		_, err := nodes[primary.ID].WriteMetrics(ctx, gaugeBatch("api", name, ts, vals))
		require.NoError(t, err)
		settle(nodes)
	}

	// Converge to the coded steady state: every node holds exactly one shard slot AND serves
	// every series (parts distribute asynchronously, so slot count alone is not convergence).
	require.Eventually(t, func() bool {
		settle(nodes)

		for _, s := range nodes {
			if distinctSlots(t, ctx, s.backend) != 1 {
				return false
			}

			eng, ok := s.lookupEngine("default")
			if !ok {
				return false
			}

			for name, vals := range series {
				got := queryEngine(t, eng, nameMatcher(name))
				if len(got) != 1 || len(got[0].Values) != len(vals) {
					return false
				}
			}
		}

		return true
	}, 30*time.Second, 200*time.Millisecond, "steady state: one slot per node, all series served")

	// Pre-kill sanity: every node serves every series.
	requireAllSeries := func(pool map[string]*Storage, phase string) {
		for id, s := range pool {
			eng, ok := s.lookupEngine("default")
			require.Truef(t, ok, "%s: %s has the engine", phase, id)

			for name, vals := range series {
				got := queryEngine(t, eng, nameMatcher(name))
				require.Lenf(t, got, 1, "%s: %s serves %s", phase, id, name)
				require.Equalf(t, vals, got[0].Values, "%s: %s values of %s", phase, id, name)
			}
		}
	}
	requireAllSeries(nodes, "pre-kill")

	// Permanently kill two random nodes — any two, the scheme's full tolerance.
	ids := make([]string, 0, len(nodes))
	for id := range nodes {
		ids = append(ids, id)
	}

	rng.Shuffle(len(ids), func(i, j int) { ids[i], ids[j] = ids[j], ids[i] })
	killed := ids[:2]

	for _, id := range killed {
		require.NoError(t, nodes[id].Close(ctx))
		delete(nodes, id)
	}
	t.Logf("killed %v; %d survivors", killed, len(nodes))

	var survivor *Storage
	for _, s := range nodes {
		survivor = s

		break
	}

	require.Eventually(t, func() bool {
		return len(survivor.cluster.membership.Members()) == 4
	}, 25*time.Second, 200*time.Millisecond, "membership drops both killed nodes")

	// Every survivor still serves every sample: reads reconstruct from any Data reachable
	// shards while repair (content-discovery) restores the positional slots for the new,
	// smaller owner set. Randomized settle rounds drive convergence.
	require.Eventually(t, func() bool {
		settle(nodes)

		for _, s := range nodes {
			eng, ok := s.lookupEngine("default")
			if !ok {
				return false
			}

			for name, vals := range series {
				got := queryEngine(t, eng, nameMatcher(name))
				if len(got) != 1 || len(got[0].Values) != len(vals) {
					return false
				}
			}
		}

		return true
	}, 45*time.Second, 300*time.Millisecond, "all survivors serve all series after losing two nodes")
	requireAllSeries(nodes, "post-kill")

	// A fresh replacement joins (a rack that lost a node) and must bootstrap from the claims:
	// engine, mirrored data, and serving every series from its own engine.
	replacement := openClusterNodeECDomains(t, endpoint, "n7", []string{"rack1"}, 4, 2)
	nodes["n7"] = replacement

	require.Eventually(t, func() bool {
		return len(replacement.cluster.membership.Members()) == 5
	}, 20*time.Second, 200*time.Millisecond, "the replacement joins the membership")

	require.Eventually(t, func() bool {
		settle(nodes)

		eng, ok := replacement.lookupEngine("default")
		if !ok {
			return false
		}

		for name, vals := range series {
			got := queryEngine(t, eng, nameMatcher(name))
			if len(got) != 1 || len(got[0].Values) != len(vals) {
				return false
			}
		}

		return true
	}, 45*time.Second, 300*time.Millisecond, "the replacement bootstraps and serves every series")

	// The replacement participates in storage too: it converges to holding shard data of its
	// own (a slot), not just proxied reads.
	require.Eventually(t, func() bool {
		settle(nodes)

		return distinctSlots(t, ctx, replacement.backend) >= 1
	}, 30*time.Second, 300*time.Millisecond, "the replacement holds its shard slot")

	// Final integrity sweep across the whole (recovered) cluster.
	requireAllSeries(nodes, "post-join")

	// And the fetch fan-out path agrees: a full query through the public Fetcher on the
	// replacement returns every series.
	for name, vals := range series {
		it, err := replacement.Fetcher("default").Fetch(ctx, fetch.Request{
			Start: 0, End: 1 << 60, Matchers: []fetch.Matcher{nameMatcher(name)},
		})
		require.NoError(t, err)
		got, err := fetch.Drain(ctx, it)
		require.NoError(t, err)
		require.Len(t, got, 1)
		assert.Equal(t, vals, got[0].Values, "public fetch of %s on the replacement", name)
	}
}

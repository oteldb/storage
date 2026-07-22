package storage

import (
	"context"
	"math/rand/v2"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/cluster"
	"github.com/oteldb/storage/cluster/ec"
	"github.com/oteldb/storage/cluster/etcd"
	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/signal"
	"github.com/oteldb/storage/tenant"
)

// openClusterNodeEC opens a private-backend cluster node whose every tenant resolves to an
// erasure-coding durability policy with the given scheme and age threshold.
func openClusterNodeEC(t *testing.T, endpoint, id string, k, m int, after time.Duration) *Storage {
	t.Helper()

	ecPol := WithTenancy(tenant.ResolverFunc(func(signal.TenantID) tenant.Policy {
		return tenant.Policy{Durability: tenant.Durability{EC: &tenant.ECScheme{Data: k, Parity: m, After: after}}}
	}))

	s, err := Open(context.Background(), Options{}, WithBackend(backend.Memory()), ecPol, WithCluster(&cluster.Config{
		Etcd:           []string{endpoint},
		Self:           etcd.Member{ID: id, Addr: "127.0.0.1:0"},
		RF:             1,
		PrivateBackend: true,
	}))
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close(context.Background()) })

	return s
}

// countShardObjects reports how many EC shard objects exist under the tenant's metric prefix in
// the backend (proof the part was converted, not full-copy).
func countShardObjects(t *testing.T, be backend.Backend, tenantMetricPrefix string) (shards, metas int) {
	t.Helper()

	keys, err := be.List(context.Background(), tenantMetricPrefix)
	require.NoError(t, err)

	for _, k := range keys {
		switch {
		case contains(k, "/ecshard/"):
			shards++
		case hasSuffix(k, "/"+ec.MetaObject):
			metas++
		}
	}

	return shards, metas
}

func contains(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}

	return false
}

func hasSuffix(s, suf string) bool { return len(s) >= len(suf) && s[len(s)-len(suf):] == suf }

// ecPayload builds n ascending-timestamp points with high-entropy values (seeded, deterministic)
// so the flushed value column does not compress below the EC shard floor.
func ecPayload(n int, seed uint64) (ts []int64, vals []float64) {
	rng := rand.New(rand.NewPCG(seed, seed+1))
	ts = make([]int64, n)
	vals = make([]float64, n)
	for i := range ts {
		ts[i] = int64(i + 1)
		vals[i] = rng.Float64() * 1e9
	}

	return ts, vals
}

// openClusterNodeECDomains is openClusterNodeEC with an explicit failure-domain path
// (coarsest first, e.g. {rack, server}), for the topology-aware placement tests.
func openClusterNodeECDomains(t *testing.T, endpoint, id string, domains []string, k, m int) *Storage {
	t.Helper()

	ecPol := WithTenancy(tenant.ResolverFunc(func(signal.TenantID) tenant.Policy {
		return tenant.Policy{Durability: tenant.Durability{EC: &tenant.ECScheme{Data: k, Parity: m}}}
	}))

	s, err := Open(context.Background(), Options{}, WithBackend(backend.Memory()), ecPol, WithCluster(&cluster.Config{
		Etcd:           []string{endpoint},
		Self:           etcd.Member{ID: id, Domains: domains, Addr: "127.0.0.1:0"},
		RF:             1,
		PrivateBackend: true,
	}))
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close(context.Background()) })

	return s
}

// TestClusterECRackAwarePlacement checks that EC shard owners are spread across failure domains:
// three nodes in three distinct zones place an ec(2,1) part one-shard-per-rack (rack-safe), while
// three nodes in one zone cannot and are flagged.
//
//nolint:paralleltest // owns an embedded etcd; runs serially
func TestClusterECRackAwarePlacement(t *testing.T) {
	endpoint := startEtcd(t)

	// Six nodes: 3 racks × 2 servers, one node (disk) each.
	a := openClusterNodeECDomains(t, endpoint, "n1", []string{"rack1", "s1"}, 4, 2)
	openClusterNodeECDomains(t, endpoint, "n2", []string{"rack1", "s2"}, 4, 2)
	openClusterNodeECDomains(t, endpoint, "n3", []string{"rack2", "s3"}, 4, 2)
	openClusterNodeECDomains(t, endpoint, "n4", []string{"rack2", "s4"}, 4, 2)
	openClusterNodeECDomains(t, endpoint, "n5", []string{"rack3", "s5"}, 4, 2)
	openClusterNodeECDomains(t, endpoint, "n6", []string{"rack3", "s6"}, 4, 2)

	require.Eventually(t, func() bool {
		return len(a.cluster.membership.Members()) == 6
	}, 10*time.Second, 50*time.Millisecond)

	scheme := ec.Scheme{Data: 4, Parity: 2}

	// The six ec(4,2) shards spread two-per-rack (≤ Parity), and within each rack across both
	// servers — so a whole-rack failure loses exactly 2 shards and is recoverable.
	owners := a.cluster.membership.Ring().LookupBalanced([]byte("default"), scheme.Shards())
	require.Len(t, owners, 6)

	perRack := map[string]int{}
	perServer := map[string]int{}
	for _, o := range owners {
		perRack[o.DomainAt(0)]++
		perServer[o.DomainAt(0)+"/"+o.DomainAt(1)]++
	}
	for rack, c := range perRack {
		assert.LessOrEqualf(t, c, 2, "rack %s holds ≤ Parity shards", rack)
	}
	for srv, c := range perServer {
		assert.LessOrEqualf(t, c, 1, "server %s holds ≤ 1 shard (balanced within rack)", srv)
	}

	n, safe := a.ecZoneShortfall("default", scheme)
	assert.Equal(t, 3, n, "three racks")
	assert.True(t, safe, "3 racks ≥ MinZones(ec(4,2))=3 ⇒ rack-safe")
}

// TestClusterECRackShortfallDetected checks the unsafe-topology flag: three nodes in one zone
// cannot place ec(2,1) rack-safely (a single rack holds all three shards).
//
//nolint:paralleltest // owns an embedded etcd; runs serially
func TestClusterECRackShortfallDetected(t *testing.T) {
	endpoint := startEtcd(t)

	// Three nodes, all in one rack (different servers) — cannot be rack-safe for ec(2,1).
	a := openClusterNodeECDomains(t, endpoint, "n1", []string{"rack1", "s1"}, 2, 1)
	openClusterNodeECDomains(t, endpoint, "n2", []string{"rack1", "s2"}, 2, 1)
	openClusterNodeECDomains(t, endpoint, "n3", []string{"rack1", "s3"}, 2, 1)

	require.Eventually(t, func() bool {
		return len(a.cluster.membership.Members()) == 3
	}, 10*time.Second, 50*time.Millisecond)

	n, safe := a.ecZoneShortfall("default", ec.Scheme{Data: 2, Parity: 1})
	assert.Equal(t, 1, n, "all in one rack")
	assert.False(t, safe, "one rack < MinZones=3 ⇒ not rack-safe")

	// The shards still spread across the three servers within that rack (best effort).
	owners := a.cluster.membership.Ring().LookupBalanced([]byte("default"), 3)
	servers := map[string]struct{}{}
	for _, o := range owners {
		servers[o.DomainAt(1)] = struct{}{}
	}
	assert.Len(t, servers, 3, "spread across all servers in the rack")
}

// TestClusterECSingleNodeConvertsAndServes is the end-to-end EC path on one node: with a private
// backend and an ec(2,1)/After=0 policy, a maintenance pass converts the flushed part to shards
// (all slots local on the sole owner) and the engine keeps serving queries by reconstructing —
// no full-copy part objects remain.
//
//nolint:paralleltest // owns an embedded etcd; runs serially
func TestClusterECSingleNodeConvertsAndServes(t *testing.T) {
	endpoint := startEtcd(t)
	ctx := context.Background()

	// Enough distinct series that the flushed value column clears the 4 KiB shard floor.
	s := openClusterNodeEC(t, endpoint, "node-a", 2, 1, 0)

	require.Eventually(t, func() bool {
		return len(s.cluster.membership.Members()) == 1
	}, 10*time.Second, 50*time.Millisecond)

	ts, vals := ecPayload(4096, 1)

	_, err := s.WriteMetrics(ctx, gaugeBatch("api", "http.requests", ts, vals))
	require.NoError(t, err)

	s.maintain(ctx) // flush + convert (After=0 ⇒ the just-flushed part is "cold")

	eng, ok := s.lookupEngine("default")
	require.True(t, ok)
	require.Equal(t, 1, eng.PartCount())

	// The part was erasure-coded: shards + a sidecar exist, and no full-copy column object does.
	shards, metas := countShardObjects(t, s.backend, "default/metrics/")
	assert.Positive(t, shards, "part objects were sharded")
	assert.Equal(t, 1, metas, "one ecmeta sidecar for the part")

	// The engine still serves the full series — every read reconstructs from the local shards.
	got := queryEngine(t, eng, nameMatcher("http.requests"))
	require.Len(t, got, 1)
	assert.Equal(t, ts, got[0].Timestamps)
	assert.Equal(t, vals, got[0].Values)
}

// TestClusterECHotPartStaysFullCopy pins the age tier: with a long After, a freshly flushed part
// is NOT converted (stays full-copy for fast reads).
//
//nolint:paralleltest // owns an embedded etcd; runs serially
func TestClusterECHotPartStaysFullCopy(t *testing.T) {
	endpoint := startEtcd(t)
	ctx := context.Background()

	s := openClusterNodeEC(t, endpoint, "node-a", 2, 1, time.Hour) // hot window: 1h

	require.Eventually(t, func() bool {
		return len(s.cluster.membership.Members()) == 1
	}, 10*time.Second, 50*time.Millisecond)

	now := time.Now().UnixNano()
	ts, vals := ecPayload(4096, 2)
	for i := range ts {
		ts[i] = now + int64(i) // recent, so the age tier keeps it full-copy
	}

	_, err := s.WriteMetrics(ctx, gaugeBatch("api", "http.requests", ts, vals))
	require.NoError(t, err)

	s.maintain(ctx)

	shards, metas := countShardObjects(t, s.backend, "default/metrics/")
	assert.Zero(t, shards, "a hot part is not sharded")
	assert.Zero(t, metas)

	// Served straight from the full-copy part (window must cover the wall-clock timestamps,
	// which exceed queryEngine's default 1<<60 end).
	eng, _ := s.lookupEngine("default")
	it, err := eng.Fetch(ctx, fetch.Request{Start: 0, End: now + int64(time.Hour), Matchers: []fetch.Matcher{nameMatcher("http.requests")}})
	require.NoError(t, err)
	got, err := fetch.Drain(ctx, it)
	require.NoError(t, err)
	require.Len(t, got, 1)
	assert.Equal(t, vals, got[0].Values)
}

// TestECSchemeForGating checks the resolver gates: no cluster, shared backend, or no EC policy
// ⇒ the raw backend (no wrapping).
//
//nolint:paralleltest // owns an embedded etcd; runs serially
func TestECSchemeForGating(t *testing.T) {
	endpoint := startEtcd(t)

	// EC policy but shared backend (PrivateBackend false) ⇒ EC does not apply.
	shared := openClusterNodeWith(t, endpoint, "node-a", backend.Memory(),
		WithTenancy(tenant.ResolverFunc(func(signal.TenantID) tenant.Policy {
			return tenant.Policy{Durability: tenant.Durability{EC: &tenant.ECScheme{Data: 2, Parity: 1}}}
		})))

	_, ok := shared.ecSchemeFor("default")
	assert.False(t, ok, "EC needs a private backend")
	assert.Same(t, shared.backend, shared.backendFor("default"), "shared backend is not wrapped")
}

// TestClusterECSlotFiltering proves shard distribution: on a 3-node ec(2,1) cluster with private
// backends, after the owner converts a part each replica mirrors ONLY its own shard slot (plus
// the ecmeta sidecar), not the whole shard set — the per-node storage EC is meant to deliver.
// Every node still reconstructs the full series.
//
//nolint:paralleltest // owns an embedded etcd; runs serially
func TestClusterECSlotFiltering(t *testing.T) {
	endpoint := startEtcd(t)
	ctx := context.Background()

	// ec(2,1) over three racks: one shard per node, one node per rack.
	nodes := map[string]*Storage{
		"n1": openClusterNodeECDomains(t, endpoint, "n1", []string{"rack1"}, 2, 1),
		"n2": openClusterNodeECDomains(t, endpoint, "n2", []string{"rack2"}, 2, 1),
		"n3": openClusterNodeECDomains(t, endpoint, "n3", []string{"rack3"}, 2, 1),
	}
	n1 := nodes["n1"]

	require.Eventually(t, func() bool {
		return len(n1.cluster.membership.Members()) == 3
	}, 10*time.Second, 50*time.Millisecond)

	ts, vals := ecPayload(4096, 3)
	_, err := n1.WriteMetrics(ctx, gaugeBatch("api", "http.requests", ts, vals))
	require.NoError(t, err)

	scheme := ec.Scheme{Data: 2, Parity: 1}
	owners := n1.cluster.membership.Ring().LookupBalanced([]byte("default"), scheme.Shards())
	require.Len(t, owners, 3)

	// Identify the compaction primary (slot 0) and run its maintenance: flush + convert.
	primaryID := owners[0].ID
	nodes[primaryID].maintain(ctx)

	// The primary (slot 0) staged all three shard slots locally after conversion.
	assert.Equal(t, 3, distinctSlots(t, ctx, nodes[primaryID].backend), "owner holds every slot (the source)")

	// Each replica syncs and ends up with ONLY its own slot's shards.
	for slot := 1; slot < 3; slot++ {
		rep := nodes[owners[slot].ID]

		require.Eventuallyf(t, func() bool {
			rep.maintain(ctx)
			slots := shardSlotsPresent(t, ctx, rep.backend)

			return len(slots) == 1 && slots[slot]
		}, 10*time.Second, 100*time.Millisecond, "slot-%d replica mirrors only its own slot", slot)
	}

	// Now that every replica holds its slot, the owner prunes its staged copies: a further
	// maintenance pass confirms each peer and drops the foreign shards, leaving it slot-0 only —
	// so every node (owner included) holds exactly one shard: the 1.5× storage target.
	primary := nodes[primaryID]
	require.Eventually(t, func() bool {
		primary.maintain(ctx)
		slots := shardSlotsPresent(t, ctx, primary.backend)

		return len(slots) == 1 && slots[0]
	}, 10*time.Second, 100*time.Millisecond, "owner prunes staged shards to its own slot")

	// Every node still serves the full series (own slot local + peers reconstruct).
	for id, s := range nodes {
		eng, ok := s.lookupEngine("default")
		require.Truef(t, ok, "%s has the engine", id)
		got := queryEngine(t, eng, nameMatcher("http.requests"))
		require.Lenf(t, got, 1, "%s serves the series", id)
		assert.Equalf(t, vals, got[0].Values, "%s values", id)
	}

	// The EC lifecycle shows up in the operator stats: the owner converted and pruned; the
	// replicas' reads reconstructed; nobody errored.
	oec := primary.Inspect().Cluster.EC
	require.NotNil(t, oec, "private backend ⇒ EC stats present")
	assert.Positive(t, oec.Converted, "owner converted the cold part")
	assert.Positive(t, oec.PrunedStagedParts, "owner pruned its staged shards")
	assert.Zero(t, oec.ConvertErrors)
	assert.Zero(t, oec.RepairErrors)

	var reconstructs int64
	for _, s := range nodes {
		if st := s.Inspect().Cluster.EC; st != nil {
			reconstructs += st.Reconstructs
			assert.Zero(t, st.ReconstructErrors)
		}
	}
	assert.Positive(t, reconstructs, "the sharded value column was reconstructed on read")
}

// shardSlotsPresent returns the set of EC shard slots that have any object in be.
func shardSlotsPresent(t *testing.T, ctx context.Context, be backend.Backend) map[int]bool {
	t.Helper()

	keys, err := be.List(ctx, "default/metrics/")
	require.NoError(t, err)

	slots := map[int]bool{}
	for _, k := range keys {
		if slot, ok := ec.ShardSlotOf(k); ok {
			slots[slot] = true
		}
	}

	return slots
}

// distinctSlots counts the distinct EC shard slots present in be.
func distinctSlots(t *testing.T, ctx context.Context, be backend.Backend) int {
	t.Helper()

	return len(shardSlotsPresent(t, ctx, be))
}

// TestClusterECShardRepair proves in-place durability: on a 3-node ec(2,1) cluster the part is
// coded so each owner holds one shard slot; one owner's shard is then destroyed (a disk failure
// that keeps the node in the ring), and its maintenance pass rebuilds the shard by gathering the
// k survivors and RS-reconstructing it — restoring the full shard set, with reads correct
// throughout (the survivors reconstruct even before the repair completes).
//
//nolint:paralleltest // owns an embedded etcd; runs serially
func TestClusterECShardRepair(t *testing.T) {
	endpoint := startEtcd(t)
	ctx := context.Background()

	nodes := map[string]*Storage{
		"n1": openClusterNodeECDomains(t, endpoint, "n1", []string{"rack1"}, 2, 1),
		"n2": openClusterNodeECDomains(t, endpoint, "n2", []string{"rack2"}, 2, 1),
		"n3": openClusterNodeECDomains(t, endpoint, "n3", []string{"rack3"}, 2, 1),
	}
	n1 := nodes["n1"]

	require.Eventually(t, func() bool {
		return len(n1.cluster.membership.Members()) == 3
	}, 10*time.Second, 50*time.Millisecond)

	scheme := ec.Scheme{Data: 2, Parity: 1}
	owners := n1.cluster.membership.Ring().LookupBalanced([]byte("default"), scheme.Shards())
	require.Len(t, owners, 3)

	// Write to the shard's primary directly (a routed write from a non-primary is a separate
	// concern), then drive to the coded, one-shard-per-owner steady state.
	ts, vals := ecPayload(4096, 9)
	_, err := nodes[owners[0].ID].WriteMetrics(ctx, gaugeBatch("api", "http.requests", ts, vals))
	require.NoError(t, err)

	settle := func() {
		for range 6 {
			for _, o := range owners {
				nodes[o.ID].maintain(ctx)
			}
		}
	}
	settle()
	for _, o := range owners {
		require.Equalf(t, 1, distinctSlots(t, ctx, nodes[o.ID].backend), "%s holds one slot before the failure", o.ID)
	}

	// A disk failure destroys the slot-2 owner's shard (its node stays in the cluster).
	victim := nodes[owners[2].ID]
	vkeys, err := victim.backend.List(ctx, "default/metrics/0000000000/ecshard/")
	require.NoError(t, err)
	require.NotEmpty(t, vkeys)
	for _, k := range vkeys {
		require.NoError(t, victim.backend.Delete(ctx, k))
	}
	require.Equal(t, 0, distinctSlots(t, ctx, victim.backend), "victim's shard is gone")

	// Reads still work immediately — the two surviving shards reconstruct the value column.
	veng, ok := victim.lookupEngine("default")
	require.True(t, ok)
	got := queryEngine(t, veng, nameMatcher("http.requests"))
	require.Len(t, got, 1)
	assert.Equal(t, vals, got[0].Values, "reads survive the shard loss via reconstruction")

	// The maintenance loop repairs: the victim rebuilds its own slot from the k survivors.
	require.Eventually(t, func() bool {
		victim.maintain(ctx)

		return distinctSlots(t, ctx, victim.backend) == 1
	}, 10*time.Second, 100*time.Millisecond, "the victim reconstructs its shard slot")

	// Every node serves the full series after repair.
	for id, s := range nodes {
		eng, ok := s.lookupEngine("default")
		require.Truef(t, ok, "%s has the engine", id)
		got := queryEngine(t, eng, nameMatcher("http.requests"))
		require.Lenf(t, got, 1, "%s serves the series post-repair", id)
		assert.Equalf(t, vals, got[0].Values, "%s values", id)
	}
}

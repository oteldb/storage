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
		Self:           etcd.Member{ID: id, Addr: freeAddr(t)},
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

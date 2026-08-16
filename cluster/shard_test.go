package cluster

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/signal"
)

func TestShardCount(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct {
		in, want int
	}{
		{-1, 1},
		{0, 1},
		{1, 1},
		{8, 8},
	} {
		assert.Equal(t, tt.want, ShardCount(tt.in), "ShardCount(%d)", tt.in)
	}
}

func TestShardKeyRoundTrip(t *testing.T) {
	t.Parallel()

	// A single shard must stay byte-identical to the bare tenant: placement and on-disk prefixes
	// of an unsharded cluster depend on it.
	for _, n := range []int{0, 1} {
		assert.Equal(t, signal.TenantID("acme"), ShardKeyOf("acme", 0, n))
	}

	for _, tenant := range []signal.TenantID{"acme", "", "a/b", "with_s"} {
		for idx := range 4 {
			key := ShardKeyOf(tenant, idx, 4)
			assert.Equal(t, tenant, TenantOfShard(key), "key %q", key)
			assert.Contains(t, string(key), ShardSep, "key %q carries the marker", key)
		}
	}

	// A key without the marker is a tenant already.
	assert.Equal(t, signal.TenantID("acme"), TenantOfShard("acme"))
}

func TestShardOfDistributes(t *testing.T) {
	t.Parallel()

	const n = 4

	assert.Equal(t, 0, ShardOf(signal.SeriesID{Lo: 12345}, 1), "a single shard takes everything")

	counts := make([]int, n)
	for i := range uint64(1000) {
		idx := ShardOf(signal.SeriesID{Lo: i}, n)
		require.GreaterOrEqual(t, idx, 0)
		require.Less(t, idx, n)
		counts[idx]++
	}

	for i, c := range counts {
		assert.NotZero(t, c, "shard %d is unused", i)
	}
}

func TestShardKeysEnumeratesEveryShard(t *testing.T) {
	t.Parallel()

	assert.Equal(t, []signal.TenantID{"acme"}, ShardKeys("acme", 1))

	keys := ShardKeys("acme", 3)
	require.Len(t, keys, 3)

	// Every series of the tenant lands on one of the enumerated keys — the invariant a read fan-out
	// depends on.
	for i := range uint64(200) {
		want := ShardKeyOf("acme", ShardOf(signal.SeriesID{Lo: i}, 3), 3)
		assert.Contains(t, keys, want)
	}
}

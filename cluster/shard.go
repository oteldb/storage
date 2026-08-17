package cluster

import (
	"strconv"
	"strings"

	"github.com/oteldb/storage/signal"
)

// A shard key is what the ring places: a tenant, optionally split into [Config.ShardsPerTenant]
// independently-placed pieces so one large tenant is not pinned to a single owner set. Every tier
// that routes — the storage node, an ingester, a query front end — must derive it identically, or
// two processes disagree about where a shard lives and the divergence is silent. That is why this
// lives here rather than inside the node.

// ShardSep separates a tenant from its shard index in a shard key. It is chosen so a shard key is
// a valid backend path segment and never collides with a real tenant id (which the embedder keeps
// free of this marker).
const ShardSep = "/_s"

// ShardCount clamps a configured [Config.ShardsPerTenant] to the usable range: anything below one
// means a single shard.
func ShardCount(shardsPerTenant int) int {
	if shardsPerTenant < 1 {
		return 1
	}

	return shardsPerTenant
}

// ShardKeyOf returns the routing/storage key for tenant's shard idx. With a single shard it is the
// bare (already-normalized) tenant, so ring placement and on-disk prefixes are byte-identical to
// the unsharded path; with n > 1 it suffixes the shard index.
func ShardKeyOf(tenant signal.TenantID, idx, n int) signal.TenantID {
	if n <= 1 {
		return tenant
	}

	return tenant + signal.TenantID(ShardSep+strconv.Itoa(idx))
}

// TenantOfShard recovers the tenant id from a shard key (the inverse of [ShardKeyOf]), for policy
// resolution. A key without the shard marker (the single-shard case) is returned unchanged.
func TenantOfShard(shardKey signal.TenantID) signal.TenantID {
	if i := strings.LastIndex(string(shardKey), ShardSep); i >= 0 {
		return shardKey[:i]
	}

	return shardKey
}

// ShardOf maps a series id to a shard index in [0, n). The series id is already a uniform content
// hash, so the low word modulo n distributes evenly.
func ShardOf(id signal.SeriesID, n int) int {
	if n <= 1 {
		return 0
	}

	return int(id.Lo % uint64(n))
}

// ShardKeys returns every shard key of a tenant, in index order. A read cannot know which shard
// holds a series before it matches one, so it fans out across all of them.
func ShardKeys(tenant signal.TenantID, n int) []signal.TenantID {
	n = ShardCount(n)

	keys := make([]signal.TenantID, n)
	for i := range n {
		keys[i] = ShardKeyOf(tenant, i, n)
	}

	return keys
}

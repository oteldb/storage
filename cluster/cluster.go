package cluster

import (
	"time"

	"github.com/oteldb/storage/cluster/etcd"
)

// Config is the cluster configuration. It is optional: a nil [storage.Options].Cluster means
// single-node mode (the cluster layer is absent). When set, the storage facade joins the
// etcd-coordinated cluster, runs the replica server on [Config.Self].Addr, and routes writes
// to their ring-owners at replication factor [Config.RF].
type Config struct {
	// Etcd is the etcd endpoint list for membership coordination.
	Etcd []string
	// Self is this node's identity: ID (ring identity), Zone (failure domain), and Addr
	// (host:port the node listens on for replication and reaches peers at).
	Self etcd.Member
	// RF is the replication factor (replicas per write). Zero ⇒ 3.
	RF int
	// ShardsPerTenant splits each tenant into this many independently-placed shards, so a single
	// large tenant spreads its ingest, storage, and compaction across up to N nodes instead of
	// being pinned to one owner set. Metrics shard by series (hash(seriesID) % N); the record
	// signals shard by stream, so a stream's records stay together on one primary (see
	// [FrameMetrics] and [FrameRecords]). Zero or one ⇒ a single shard: the tenant is the shard,
	// and on-disk layout and placement are identical to the unsharded path.
	//
	// This is not the replication knob, and RF does not substitute for it. Every write for a
	// shard is admitted by that shard's one primary, so a single-shard tenant concentrates all of
	// its ingest and compaction on one node however many replicas RF asks for and however many
	// nodes the ring has.
	//
	// Choose it for the largest cluster this tenant will ever run on, not the current one: the
	// shard key is the routing key *and* the on-disk prefix, so changing N re-keys every shard and
	// strands the data written under the old keys. One is the only value that cannot be grown out
	// of, which is why a multi-node cluster is warned about it at maintenance time.
	ShardsPerTenant int
	// Root is the etcd key prefix for this cluster's state. Empty ⇒ "/oteldb".
	Root string
	// MemberTTL is the TTL of the etcd lease this node's membership registration hangs off:
	// how long the node may be unable to reach etcd before its peers evict it from the ring.
	// Lowering it detects a dead node sooner at the cost of evicting a live but stalled one
	// (a GC pause, a starved CPU, an etcd blip). An evicted node re-registers on its own, so
	// this sets how often that happens, not whether the cluster recovers. Zero ⇒
	// [etcd.DefaultTTL].
	MemberTTL time.Duration
	// PrivateBackend declares that this node's backend is private to it (a local disk, not a
	// shared object store): peers cannot read the parts this node flushes. The cluster then
	// replicates flushed parts node-to-node — replicas mirror their owner's backend objects
	// over the parts endpoints (cluster/partsync) instead of loading them from a shared store,
	// and an owner backfills from its peers before compacting. False (the default) keeps the
	// shared-store model: flushed parts are exchanged through the backend, never over the
	// cluster transport.
	PrivateBackend bool
}

// DefaultRF is the replication factor used when [Config.RF] is unset.
const DefaultRF = 3

// DefaultRoot is the etcd key prefix used when [Config.Root] is empty.
const DefaultRoot = "/oteldb"

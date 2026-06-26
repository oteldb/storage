package cluster

import "github.com/oteldb/storage/cluster/etcd"

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
	// Root is the etcd key prefix for this cluster's state. Empty ⇒ "/oteldb".
	Root string
}

// DefaultRF is the replication factor used when [Config.RF] is unset.
const DefaultRF = 3

// DefaultRoot is the etcd key prefix used when [Config.Root] is empty.
const DefaultRoot = "/oteldb"

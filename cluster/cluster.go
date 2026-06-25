// Package cluster implements the L0 distribution layer (DESIGN.md §3, §11, §14 M6–M7).
//
// This is a scaffold stub. The ring, etcd integration, replication, and rebalancing
// are filled in at M6. Single-node users skip this package entirely
// ([Options.Cluster] == nil ⇒ no cluster layer).
package cluster

// Config is the cluster configuration (DESIGN.md §5, §11). It is optional: a nil
// [Options.Cluster] means single-node mode, where the cluster layer is absent.
//
// Fields will be filled in at M6 (ring tokens, etcd endpoints, RF, zone config,
// rebalance policy). It is a placeholder struct now so the [storage.Options] shape
// is stable.
type Config struct {
	// TODO(M6): EtcdEndpoints []string
	// TODO(M6): ReplicationFactor int      // default 3
	// TODO(M6): RingTokens []uint32        // spread-minimizing tokens
	// TODO(M6): Zones []string             // zone-aware placement
	// TODO(M6): Rebalance RebalancePolicy
}

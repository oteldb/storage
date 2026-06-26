package etcd

import (
	"context"

	"github.com/go-faster/errors"
	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/oteldb/storage/cluster/ring"
)

// Ownership coordinates exclusive **compaction ownership** of shards across the cluster via
// etcd, so a shard (a tenant) is flushed/merged by exactly one node at a time — the rebalance
// executor. A node claims a shard with a CAS write keyed by the shard and bound to its
// membership lease; the claim auto-releases if the node dies, so a new primary can take over
// without manual handoff. Placement still comes from the ring; etcd only arbitrates the claim
// during the brief windows where nodes disagree on the ring (watch-propagation lag) or a node
// has failed.
type Ownership struct {
	client  *clientv3.Client
	prefix  string // "{root}/owners/"
	id      string // this node's ring ID
	leaseID clientv3.LeaseID
}

// NewOwnership returns an ownership coordinator for node id, claiming under root with the
// node's membership lease (see [Membership.LeaseID]).
func NewOwnership(client *clientv3.Client, root, id string, leaseID clientv3.LeaseID) *Ownership {
	return &Ownership{client: client, prefix: joinKey(root, "owners"), id: id, leaseID: leaseID}
}

// Acquire tries to claim shard for this node. It returns true if the claim is now held by this
// node (newly acquired or already ours), false if another live node holds it. The claim is a
// CAS: create the key only if absent; otherwise it belongs to whoever already created it.
func (o *Ownership) Acquire(ctx context.Context, shard string) (bool, error) {
	key := o.prefix + shard

	resp, err := o.client.Txn(ctx).
		If(clientv3.Compare(clientv3.CreateRevision(key), "=", 0)).
		Then(clientv3.OpPut(key, o.id, clientv3.WithLease(o.leaseID))).
		Else(clientv3.OpGet(key)).
		Commit()
	if err != nil {
		return false, errors.Wrapf(err, "acquire %q", shard)
	}

	if resp.Succeeded {
		return true, nil // we created the claim
	}

	// The key already exists: the claim is ours only if we wrote it.
	kvs := resp.Responses[0].GetResponseRange().Kvs

	return len(kvs) == 1 && string(kvs[0].Value) == o.id, nil
}

// Release relinquishes shard, but only if this node still holds the claim (a guarded delete,
// so it never deletes another node's claim).
func (o *Ownership) Release(ctx context.Context, shard string) error {
	key := o.prefix + shard

	_, err := o.client.Txn(ctx).
		If(clientv3.Compare(clientv3.Value(key), "=", o.id)).
		Then(clientv3.OpDelete(key)).
		Commit()
	if err != nil {
		return errors.Wrapf(err, "release %q", shard)
	}

	return nil
}

// Reconcile makes this node's claims match the ring: it acquires every shard this node is the
// primary of (ring.Primary) and releases the rest. It returns the shards this node now owns —
// the set it should flush and compact. Idempotent, so it is safe to call on every membership
// change and on a timer.
func (o *Ownership) Reconcile(ctx context.Context, r *ring.Ring, shards []string) ([]string, error) {
	var owned []string

	for _, shard := range shards {
		primary, ok := r.Primary([]byte(shard))
		if ok && primary.ID == o.id {
			acquired, err := o.Acquire(ctx, shard)
			if err != nil {
				return owned, err
			}

			if acquired {
				owned = append(owned, shard)
			}

			continue
		}

		if err := o.Release(ctx, shard); err != nil {
			return owned, err
		}
	}

	return owned, nil
}

// joinKey joins an etcd key root and a segment with a single trailing slash, tolerating a
// root with or without a leading/trailing slash.
func joinKey(root, segment string) string {
	if root == "" {
		root = "/"
	}

	if root[len(root)-1] != '/' {
		root += "/"
	}

	return root + segment + "/"
}

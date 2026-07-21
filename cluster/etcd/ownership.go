package etcd

import (
	"context"
	"sort"
	"sync"

	"github.com/go-faster/errors"
	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/oteldb/storage/cluster/rebalance"
	"github.com/oteldb/storage/cluster/ring"
)

// Ownership coordinates exclusive **compaction ownership** of shards across the cluster via
// etcd, so a shard (a tenant) is flushed/merged by exactly one node at a time — the rebalance
// executor. A node claims a shard with a CAS write keyed by the shard and bound to its
// membership lease; the claim auto-releases if the node dies, so a new primary can take over
// without manual handoff. Placement still comes from the ring; etcd only arbitrates the claim
// during the brief windows where nodes disagree on the ring (watch-propagation lag) or a node
// has failed.
//
// Reconcile is **event-driven and minimal-move**: it tracks the claims this node currently
// holds and, on each pass, only issues etcd writes for the shards whose ring-primary actually
// changed since the last pass (plus retrying any wanted-but-uncontended claim). In steady
// state — an unchanged ring with no new tenants — it makes no etcd round-trips at all, instead
// of one acquire/release per shard every tick. When the ring does change it records the
// [rebalance.Plan] it enacted (see [Ownership.LastPlan]) for observability/preview.
type Ownership struct {
	client  *clientv3.Client
	prefix  string // "{root}/owners/"
	id      string // this node's ring ID
	leaseID clientv3.LeaseID

	mu       sync.Mutex
	held     map[string]struct{}      // shards this node currently holds a claim on
	prevRing *ring.Ring               // ring at the last Reconcile (pointer-compared for "unchanged")
	lastPlan []rebalance.Reassignment // the owner-set handoffs enacted at the last ring change
	planRF   func(shard string) int   // per-shard rf for LastPlan recording; nil ⇒ 1 (primary only)
}

// NewOwnership returns an ownership coordinator for node id, claiming under root with the
// node's membership lease (see [Membership.LeaseID]).
func NewOwnership(client *clientv3.Client, root, id string, leaseID clientv3.LeaseID) *Ownership {
	return &Ownership{
		client:  client,
		prefix:  joinKey(root, "owners"),
		id:      id,
		leaseID: leaseID,
		held:    make(map[string]struct{}),
	}
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
	kvs := resp.Responses[0].GetResponseRange().GetKvs()

	return len(kvs) == 1 && string(kvs[0].GetValue()) == o.id, nil
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
// ring-primary of and releases the rest. It returns the shards this node now owns — the set it
// should flush and compact. Idempotent, so it is safe to call on every membership change and on
// a timer.
//
// The work is minimal: ring-primary lookups are pure in-memory HRW hashing (no etcd), and an
// etcd write is issued only when a claim must change — a wanted shard not yet held is acquired
// (retried every pass, which is what lets a stale claim's release converge even under an
// unchanged ring), and a held shard no longer wanted is released. Steady state issues no etcd
// writes. On a ring change the enacted primary handoffs are recorded in [Ownership.LastPlan].
func (o *Ownership) Reconcile(ctx context.Context, r *ring.Ring, shards []string) ([]string, error) {
	o.mu.Lock()
	defer o.mu.Unlock()

	// want = shards this node is the ring-primary of (in-memory, no etcd).
	want := make(map[string]struct{}, len(shards))

	for _, shard := range shards {
		if primary, ok := r.Primary([]byte(shard)); ok && primary.ID == o.id {
			want[shard] = struct{}{}
		}
	}

	// Acquire each wanted shard we do not already hold. Retrying this every pass (cheap — it is
	// empty in steady state) is what drives convergence: when a node displaced during a ring
	// disagreement finally releases a claim, the rightful primary picks it up here.
	for shard := range want {
		if _, ok := o.held[shard]; ok {
			continue
		}

		acquired, err := o.Acquire(ctx, shard)
		if err != nil {
			return o.ownedLocked(), err
		}

		if acquired {
			o.held[shard] = struct{}{}
		}
	}

	// Release each held shard we no longer want (its primary moved away, or its tenant is gone).
	for shard := range o.held {
		if _, ok := want[shard]; ok {
			continue
		}

		if err := o.Release(ctx, shard); err != nil {
			return o.ownedLocked(), err
		}

		delete(o.held, shard)
	}

	// Record the owner-set handoffs this ring change implied, so an operator can see/preview
	// what moved. With a plan-RF resolver ([Ownership.SetPlanRF]) the plan covers each shard's
	// full owner set at its tenant's replication factor — the replicas that must backfill, not
	// just the compaction primary; without one it falls back to rf=1 (primary handoffs only,
	// matching the claims this reconciler actually moves). The membership layer republishes the
	// ring via an atomic pointer only on a real change, so pointer inequality means "changed".
	if o.prevRing != nil && o.prevRing != r {
		rfOf := o.planRF
		if rfOf == nil {
			rfOf = func(string) int { return 1 }
		}

		o.lastPlan = rebalance.PlanWith(shards, o.prevRing, r, rfOf)
	}

	o.prevRing = r

	return o.ownedLocked(), nil
}

// Owned returns a sorted snapshot of the shards this node currently holds a compaction claim on.
func (o *Ownership) Owned() []string {
	o.mu.Lock()
	defer o.mu.Unlock()

	return o.ownedLocked()
}

// SetPlanRF sets the per-shard replication factor used when recording [Ownership.LastPlan]
// (e.g. the tenant durability policy's RF), so the recorded plan reflects each shard's full
// owner-set diff rather than only the primary handoff. It does not affect claim reconciliation
// — compaction ownership always tracks the primary alone. Call before the first Reconcile;
// a nil resolver (the default) records primary-only (rf=1) plans.
func (o *Ownership) SetPlanRF(rfOf func(shard string) int) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.planRF = rfOf
}

// LastPlan returns the owner-set handoffs enacted at the most recent ring change (empty if the
// ring has not changed since open). It is informational — a preview of what the last rebalance
// moved — for an operator dashboard. See [Ownership.SetPlanRF] for the owner-set breadth.
func (o *Ownership) LastPlan() []rebalance.Reassignment {
	o.mu.Lock()
	defer o.mu.Unlock()

	out := make([]rebalance.Reassignment, len(o.lastPlan))
	copy(out, o.lastPlan)

	return out
}

// ownedLocked returns a sorted snapshot of held; the caller must hold o.mu.
func (o *Ownership) ownedLocked() []string {
	owned := make([]string, 0, len(o.held))
	for shard := range o.held {
		owned = append(owned, shard)
	}

	sort.Strings(owned)

	return owned
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

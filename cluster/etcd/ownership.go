package etcd

import (
	"context"
	"sort"
	"sync"
	"sync/atomic"

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
	client *clientv3.Client
	prefix string // "{root}/owners/"
	id     string // this node's ring ID

	// leaseID is the membership lease claims bind to. It changes when the node re-registers
	// after losing its lease ([Membership.OnRejoin]), so it is read atomically.
	leaseID atomic.Int64

	// fenced reports that the lease every claim hangs off can no longer be proven live
	// ([Membership.Fenced]), so nothing in held may be acted on. Unset ⇒ never fenced.
	fenced atomic.Pointer[func() bool]

	mu sync.Mutex
	// held maps each shard this node holds a claim on to that claim's term — the etcd revision
	// the claim was created at. See [Ownership.Term].
	held     map[string]uint64
	prevRing *ring.Ring               // ring at the last Reconcile (pointer-compared for "unchanged")
	lastPlan []rebalance.Reassignment // the owner-set handoffs enacted at the last ring change
	planRF   func(shard string) int   // per-shard rf for LastPlan recording; nil ⇒ 1 (primary only)
}

// NewOwnership returns an ownership coordinator for node id, claiming under root with the
// node's membership lease (see [Membership.LeaseID]).
func NewOwnership(client *clientv3.Client, root, id string, leaseID clientv3.LeaseID) *Ownership {
	o := &Ownership{
		client: client,
		prefix: joinKey(root, "owners"),
		id:     id,
		held:   make(map[string]uint64),
	}
	o.leaseID.Store(int64(leaseID))

	return o
}

// SetLease rebinds claims to a new membership lease, after the node lost its old one and
// re-registered (wire it with [Membership.OnRejoin]). Every claim written under the old lease
// went with it, so the held set is dropped too: it would otherwise record ownership this node
// no longer has, and Reconcile only writes for shards it does not already believe it holds.
// The next Reconcile re-acquires under the new lease.
func (o *Ownership) SetLease(id clientv3.LeaseID) {
	o.mu.Lock()
	defer o.mu.Unlock()

	o.leaseID.Store(int64(id))
	o.held = make(map[string]uint64)
}

// SetFence installs the predicate that reports this node's claims unprovable — wire it to
// [Membership.Fenced]. While it reports true this node holds nothing as far as every caller is
// concerned: [Ownership.Term] disclaims, [Ownership.Owned] is empty, and [Ownership.Reconcile]
// is a no-op, so the node neither flushes nor stamps an index for a shard whose tenure it can
// no longer prove. The held set itself is kept, so a lease confirmed again — the same lease,
// without a rejoin — resumes the claims rather than re-acquiring them. nil clears the fence.
func (o *Ownership) SetFence(fenced func() bool) {
	if fenced == nil {
		o.fenced.Store(nil)

		return
	}

	o.fenced.Store(&fenced)
}

// Claims returns every currently-claimed shard across the cluster (sorted), from one etcd
// range read. It is the cluster-wide tenant/shard discovery a node needs when it is promoted
// into a shard's owner set without ever having held the shard locally (a spare): its own
// engine maps do not know the tenant, but any live shard has a compaction owner whose claim
// names it. A shard whose every owner died has no claim and is not discoverable here — its
// data is only recoverable through the backend (shared store) or peer listings.
func (o *Ownership) Claims(ctx context.Context) ([]string, error) {
	resp, err := o.client.Get(ctx, o.prefix, clientv3.WithPrefix(), clientv3.WithKeysOnly())
	if err != nil {
		return nil, errors.Wrap(err, "list claims")
	}

	out := make([]string, 0, len(resp.Kvs))
	for _, kv := range resp.Kvs {
		out = append(out, string(kv.GetKey()[len(o.prefix):]))
	}

	sort.Strings(out)

	return out, nil
}

// Acquire tries to claim shard for this node. It reports whether the claim is now held by this
// node (newly acquired or already ours), and the claim's term. The claim is a CAS: create the
// key only if absent; otherwise it belongs to whoever already created it.
//
// The term is the etcd revision the claim key was created at, which costs nothing — it is in the
// response either way. etcd revisions are cluster-wide monotonic, so a term orders every
// ownership tenure of a shard against every other, and it is *stable* for the life of one tenure:
// reacquiring a claim this node already holds reports the revision it was created at, not the
// current one, so repeated [Ownership.Reconcile] passes do not keep moving it.
//
// That is what the bucket index's commit generation needs to survive a node restored from an
// old snapshot:
// reacquiring the shard puts its writes above everything its replicas hold, and a node that lost
// the shard keeps the lower term of a tenure that has ended.
func (o *Ownership) Acquire(ctx context.Context, shard string) (term uint64, ok bool, err error) {
	key := o.prefix + shard

	resp, err := o.client.Txn(ctx).
		If(clientv3.Compare(clientv3.CreateRevision(key), "=", 0)).
		Then(clientv3.OpPut(key, o.id, clientv3.WithLease(clientv3.LeaseID(o.leaseID.Load())))).
		Else(clientv3.OpGet(key)).
		Commit()
	if err != nil {
		return 0, false, errors.Wrapf(err, "acquire %q", shard)
	}

	if resp.Succeeded {
		// We created the claim, so the transaction's revision is the one it was created at.
		return uint64(resp.Header.GetRevision()), true, nil
	}

	// The key already exists: the claim is ours only if we wrote it.
	kvs := resp.Responses[0].GetResponseRange().GetKvs()
	if len(kvs) != 1 || string(kvs[0].GetValue()) != o.id {
		return 0, false, nil
	}

	return uint64(kvs[0].GetCreateRevision()), true, nil
}

// Term returns the term of this node's claim on shard — the etcd revision it was created at —
// and whether the shard is held at all. It is what a writer stamps into the indexes it writes
// for that shard, so a shrunk index can be told from a damaged one.
func (o *Ownership) Term(shard string) (uint64, bool) {
	if o.isFenced() {
		return 0, false
	}

	o.mu.Lock()
	defer o.mu.Unlock()

	term, ok := o.held[shard]

	return term, ok
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
	if o.isFenced() {
		// Past the lease deadline every claim is another node's to take, and the etcd writes
		// this pass would issue cannot reach etcd anyway. Hold the set and own nothing.
		return nil, nil
	}

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

		term, acquired, err := o.Acquire(ctx, shard)
		if err != nil {
			return o.ownedLocked(), err
		}

		if acquired {
			o.held[shard] = term
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
	if o.isFenced() {
		return nil
	}

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

// isFenced reports whether the claim-backing lease is currently unprovable.
func (o *Ownership) isFenced() bool {
	fn := o.fenced.Load()

	return fn != nil && (*fn)()
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

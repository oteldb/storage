// Package etcd backs the L0 cluster ring with etcd: a node registers itself under a lease
// and watches the member set, so membership is live and self-healing — a crashed node's
// lease expires and it drops out of every other node's ring within the TTL, with no manual
// deregistration. The ring ([cluster/ring]) is rebuilt locally from the watched member set,
// so placement stays coordinator-free on the hot path; etcd only distributes membership.
package etcd

import (
	"context"
	"path"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-faster/errors"
	"github.com/go-faster/jx"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"

	"github.com/oteldb/storage/cluster/ring"
)

// DefaultTTL is the lease TTL: a node absent for this long (no keepalive) is evicted from the
// ring. It bounds failure-detection latency, and it is equally the blast radius of a hiccup — a
// GC pause, a CPU-starved node or a brief etcd stall longer than the TTL costs the node its
// lease. 30s trades slower failure detection for not evicting healthy nodes: the ring only has
// to be right within a rebalance, while a spurious eviction moves ownership for nothing.
// Re-registration is what makes an eviction survivable at all; the TTL only sets how often one
// happens. [Join] takes a per-cluster override.
const DefaultTTL = 30 * time.Second

// Re-registration backoff bounds. The first attempt is immediate — the common case is a lease
// expiry against a healthy etcd, which one Put repairs — and only repeated failures back off,
// up to a cap that keeps a long outage cheap without leaving the node absent for long after
// etcd returns.
const (
	rejoinMinBackoff = 100 * time.Millisecond
	rejoinMaxBackoff = 5 * time.Second
)

// Member is a cluster node's advertised identity: its ring ID, failure-domain location, and
// the Addr its peers reach it on. Zone is the single-level failure domain (a rack); Domains is
// the hierarchical form (coarsest first, e.g. {rack, server}) that supersedes Zone when set —
// erasure-coded shards balance across it so a rack/server/disk failure loses the fewest shards.
type Member struct {
	ID      string
	Zone    string
	Addr    string
	Domains []string
}

func (m Member) encode() []byte {
	e := &jx.Encoder{}
	e.ObjStart()
	e.FieldStart("id")
	e.Str(m.ID)
	e.FieldStart("zone")
	e.Str(m.Zone)
	e.FieldStart("addr")
	e.Str(m.Addr)

	if len(m.Domains) > 0 {
		e.FieldStart("domains")
		e.ArrStart()

		for _, d := range m.Domains {
			e.Str(d)
		}

		e.ArrEnd()
	}

	e.ObjEnd()

	return e.Bytes()
}

func decodeMember(data []byte) (Member, error) {
	var m Member

	d := jx.DecodeBytes(data)
	err := d.Obj(func(d *jx.Decoder, key string) error {
		var err error
		switch key {
		case "id":
			m.ID, err = d.Str()
		case "zone":
			m.Zone, err = d.Str()
		case "addr":
			m.Addr, err = d.Str()
		case "domains":
			err = d.Arr(func(d *jx.Decoder) error {
				s, err := d.Str()
				if err != nil {
					return err
				}

				m.Domains = append(m.Domains, s)

				return nil
			})
		default:
			return d.Skip()
		}

		return err
	})
	if err != nil {
		return Member{}, errors.Wrap(err, "decode member")
	}

	return m, nil
}

// Membership is a live, etcd-backed view of the cluster. It keeps this node registered (under
// a keep-alive'd lease) and watches the member set, exposing the current [ring.Ring]. Safe for
// concurrent use; [Membership.Ring] and [Membership.Members] are lock-free / cheap.
type Membership struct {
	client *clientv3.Client
	prefix string // "{root}/members/"
	self   Member
	ttl    time.Duration

	// leaseID is the lease the member key currently hangs off. Re-registration replaces it
	// rather than reusing it, so it is read atomically instead of captured once.
	leaseID atomic.Int64

	// evicted carries "this node is no longer registered" from the watch to the maintainer.
	// Buffered by one and sent to non-blockingly: it is a level, not a queue. nil for an
	// observer ([Watch]), which registers nothing and so cannot be evicted.
	evicted chan struct{}

	selfAbsent atomic.Bool
	rejoins    atomic.Int64

	onRejoin atomic.Pointer[func(clientv3.LeaseID)]

	current atomic.Pointer[ring.Ring]

	mu      sync.RWMutex
	members map[string]Member

	cancel context.CancelFunc
	wg     sync.WaitGroup

	log atomic.Pointer[zap.Logger] // membership-change logging; unset ⇒ no-op
}

// SetLogger attaches a logger that records member joins and leaves (Info on each change). It must
// be called before [Join] starts the watch loop in practice it is set immediately after Join.
// nil disables logging. Safe only before the watch observes its first event.
func (m *Membership) SetLogger(l *zap.Logger) {
	if l != nil {
		m.log.Store(l)
	}
}

// OnRejoin registers a hook invoked after this node re-registers under a fresh lease. Anything
// bound to the old lease died with it — compaction claims above all (see [Ownership.SetLease])
// — so the hook is how a dependent rebinds. It runs on the maintainer goroutine and must not
// block. A nil hook clears it.
func (m *Membership) OnRejoin(fn func(clientv3.LeaseID)) {
	if fn == nil {
		m.onRejoin.Store(nil)

		return
	}

	m.onRejoin.Store(&fn)
}

// SelfAbsent reports that this node believes it is missing from the cluster member set: its
// lease was lost (or its key deleted) and re-registration has not yet succeeded. It is a
// contradiction — the node is running — and while it holds, the node is invisible to every
// peer's ring, so it takes no writes and owns no shards. It clears on the next successful
// registration.
func (m *Membership) SelfAbsent() bool { return m.selfAbsent.Load() }

// Rejoins counts the times this node re-registered after losing its registration. A value that
// keeps climbing means the lease TTL is too tight for the environment.
func (m *Membership) Rejoins() int64 { return m.rejoins.Load() }

// Join registers self in the cluster rooted at root (an etcd key prefix) under a lease of ttl
// (≤ 0 ⇒ [DefaultTTL]), snapshots the current members, and starts watching for changes. The
// returned [Membership] must be closed to deregister.
func Join(ctx context.Context, client *clientv3.Client, root string, self Member, ttl time.Duration) (*Membership, error) {
	if self.ID == "" {
		return nil, errors.New("etcd: member ID is required")
	}

	if ttl <= 0 {
		ttl = DefaultTTL
	}

	prefix := path.Join(root, "members") + "/"

	m := &Membership{
		client:  client,
		prefix:  prefix,
		self:    self,
		ttl:     ttl,
		evicted: make(chan struct{}, 1),
		members: make(map[string]Member),
	}

	lease, err := m.register(ctx)
	if err != nil {
		return nil, err
	}

	m.leaseID.Store(int64(lease))

	// Snapshot the current members, then watch from the snapshot revision so no change is
	// missed in the gap between the Get and the Watch.
	resp, err := client.Get(ctx, prefix, clientv3.WithPrefix())
	if err != nil {
		return nil, errors.Wrap(err, "list members")
	}

	for _, kv := range resp.Kvs {
		if mem, err := decodeMember(kv.GetValue()); err == nil {
			m.members[mem.ID] = mem
		}
	}

	m.rebuild()

	// The lease keep-alive and watch must outlive this Join call, so their context is rooted
	// at Background and scoped to the Membership's own lifetime (canceled by Close), not to
	// the caller's request context.
	bg, cancel := context.WithCancel(context.Background())
	m.cancel = cancel
	m.wg.Add(2)

	go m.maintain(bg)                           //nolint:contextcheck // lifetime-scoped, see above
	go m.watch(bg, resp.Header.GetRevision()+1) //nolint:contextcheck // lifetime-scoped, see above

	return m, nil
}

// Ring returns the current ring (lock-free). It is replaced atomically as membership changes.
func (m *Membership) Ring() *ring.Ring { return m.current.Load() }

// LeaseID is this node's membership lease. Ownership claims bind to it so they auto-release
// when the node dies (the basis for the rebalance handoff).
func (m *Membership) LeaseID() clientv3.LeaseID { return clientv3.LeaseID(m.leaseID.Load()) }

// AddrOf returns the network address of the member with the given ring node ID, or "" if the
// member is unknown. It is the resolver the cluster write path uses to turn ring owners into
// transport targets.
func (m *Membership) AddrOf(id string) string {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return m.members[id].Addr
}

// Members returns the current members, sorted by ID.
func (m *Membership) Members() []Member {
	m.mu.RLock()
	defer m.mu.RUnlock()

	out := make([]Member, 0, len(m.members))
	for _, mem := range m.members {
		out = append(out, mem)
	}

	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })

	return out
}

// Close stops watching, revokes this node's lease (so its peers drop it immediately rather
// than after the TTL), and waits for the background goroutines to exit. The revoke is bounded
// by a derived timeout. An observer ([Watch]) holds no lease, so it only stops watching.
func (m *Membership) Close(ctx context.Context) error {
	m.cancel()
	m.wg.Wait()

	lease := m.LeaseID()
	if lease == clientv3.NoLease {
		return nil
	}

	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	if _, err := m.client.Revoke(ctx, lease); err != nil {
		return errors.Wrap(err, "revoke lease")
	}

	return nil
}

// register grants a fresh lease and puts this node's member key under it. That pair is the
// whole of "being in the cluster", and it is idempotent: the key is overwritten, so a stale
// copy left by an earlier registration is replaced rather than duplicated.
func (m *Membership) register(ctx context.Context) (clientv3.LeaseID, error) {
	lease, err := m.client.Grant(ctx, int64(m.ttl.Seconds()))
	if err != nil {
		return clientv3.NoLease, errors.Wrap(err, "grant lease")
	}

	if _, err := m.client.Put(ctx, m.prefix+m.self.ID, string(m.self.encode()), clientv3.WithLease(lease.ID)); err != nil {
		return clientv3.NoLease, errors.Wrap(err, "register member")
	}

	return lease.ID, nil
}

// maintain keeps this node registered for as long as it runs. Registration is not a one-shot
// startup step: any stall longer than the TTL loses the lease, etcd then deletes the member
// key, and the node is gone from every peer's ring — without this loop, permanently, since
// nothing else ever writes the key again. Losing it is therefore an ordinary event, answered
// with a new lease and a new key rather than with an exit.
//
// The node deliberately does **not** fail its own readiness while absent. It still holds every
// shard it held a moment ago and can still answer for them, and a secondary's head lives in
// memory only, so restarting it would trade a routing problem for lost writes and a read gap —
// on every node at once, since they lose their leases together. The absence is surfaced
// instead ([Membership.SelfAbsent] and a warning log); the routing tier's own health signal
// already reports the routing side of it.
func (m *Membership) maintain(ctx context.Context) {
	defer m.wg.Done()

	for {
		if !m.follow(ctx) {
			return // Deliberate shutdown.
		}

		m.selfAbsent.Store(true)
		m.logger().Warn("absent from cluster member set, re-registering",
			zap.String("id", m.self.ID), zap.Int64("lease", m.leaseID.Load()))

		if !m.rejoin(ctx) {
			return
		}
	}
}

// follow renews the current lease until this node's registration is gone, reporting whether it
// was lost (true) rather than the node shutting down (false). Two things end it: the clientv3
// keep-alive channel closing, the proactive signal that the lease is gone; and the watch
// reporting this node's own key deleted, which covers a key removed out from under a lease
// that is otherwise still healthy.
func (m *Membership) follow(ctx context.Context) bool {
	// Drop an eviction signal left over from the loss just repaired, so one loss is not
	// answered twice.
	select {
	case <-m.evicted:
	default:
	}

	ch, err := m.client.KeepAlive(ctx, m.LeaseID())
	if err != nil {
		return ctx.Err() == nil
	}

	for {
		select {
		case <-ctx.Done():
			return false
		case <-m.evicted:
			return true
		case _, ok := <-ch:
			if !ok {
				return ctx.Err() == nil
			}
		}
	}
}

// rejoin re-registers under a fresh lease, retrying with backoff until it succeeds or the node
// shuts down; it reports whether registration was restored. The superseded lease is revoked
// best-effort afterwards so whatever hung off it (compaction claims) is released now rather
// than at the end of its TTL — nothing renews it any more, so this only makes it prompt.
func (m *Membership) rejoin(ctx context.Context) bool {
	old := m.LeaseID()

	for delay := time.Duration(0); ; delay = min(max(delay*2, rejoinMinBackoff), rejoinMaxBackoff) {
		if delay > 0 && !sleepCtx(ctx, delay) {
			return false
		}

		lease, err := m.register(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return false
			}

			m.logger().Warn("re-registration failed", zap.String("id", m.self.ID), zap.Error(err))

			continue
		}

		m.leaseID.Store(int64(lease))
		m.selfAbsent.Store(false)
		m.rejoins.Add(1)
		m.revoke(ctx, old)

		m.logger().Warn("re-registered in cluster member set",
			zap.String("id", m.self.ID), zap.Int64("lease", int64(lease)),
			zap.Int64("rejoins", m.rejoins.Load()))

		if fn := m.onRejoin.Load(); fn != nil {
			(*fn)(lease)
		}

		return true
	}
}

// revoke drops a superseded lease, ignoring the error: the usual reason it fails is that the
// lease had already expired, which is the outcome being asked for.
func (m *Membership) revoke(ctx context.Context, lease clientv3.LeaseID) {
	if lease == clientv3.NoLease {
		return
	}

	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	_, _ = m.client.Revoke(ctx, lease)
}

func sleepCtx(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()

	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}

// watch applies member PUT/DELETE events to the local set and rebuilds the ring on each
// change, starting from rev.
func (m *Membership) watch(ctx context.Context, rev int64) {
	defer m.wg.Done()

	wch := m.client.Watch(ctx, m.prefix, clientv3.WithPrefix(), clientv3.WithRev(rev))
	for resp := range wch {
		if resp.Canceled {
			return
		}

		changed := false
		for _, ev := range resp.Events {
			switch ev.Type {
			case clientv3.EventTypePut:
				if mem, err := decodeMember(ev.Kv.GetValue()); err == nil {
					m.set(mem)
					changed = true
					m.logger().Info("member joined",
						zap.String("id", mem.ID), zap.String("zone", mem.Zone), zap.String("addr", mem.Addr))
				}
			case clientv3.EventTypeDelete:
				id := strings.TrimPrefix(string(ev.Kv.GetKey()), m.prefix)
				m.remove(id)
				changed = true

				if m.evicted != nil && id == m.self.ID {
					// A node is authoritative about its own liveness, so seeing its own
					// departure is a contradiction — and a free second detection point
					// alongside the keep-alive, covering an externally deleted key.
					m.logger().Warn("observed own departure from cluster member set", zap.String("id", id))

					select {
					case m.evicted <- struct{}{}:
					default:
					}

					break
				}

				m.logger().Info("member left", zap.String("id", id))
			}
		}

		if changed {
			m.rebuild()
			m.logger().Info("ring rebuilt", zap.Int("members", len(m.Members())))
		}
	}
}

func (m *Membership) set(mem Member) {
	m.mu.Lock()
	m.members[mem.ID] = mem
	m.mu.Unlock()
}

func (m *Membership) remove(id string) {
	m.mu.Lock()
	delete(m.members, id)
	m.mu.Unlock()
}

// rebuild snapshots the member set into a fresh ring and publishes it atomically.
func (m *Membership) rebuild() {
	m.mu.RLock()
	nodes := make([]ring.Node, 0, len(m.members))
	for _, mem := range m.members {
		nodes = append(nodes, ring.Node{ID: mem.ID, Zone: mem.Zone, Domains: mem.Domains})
	}
	m.mu.RUnlock()

	m.current.Store(ring.New(nodes...))
}

func (m *Membership) logger() *zap.Logger {
	if l := m.log.Load(); l != nil {
		return l
	}

	return zap.NewNop()
}

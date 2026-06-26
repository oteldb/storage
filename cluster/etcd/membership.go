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

	"github.com/oteldb/storage/cluster/ring"
)

// DefaultTTL is the lease TTL: a node absent for this long (no keepalive) is evicted from the
// ring. It bounds failure-detection latency.
const DefaultTTL = 10 * time.Second

// Member is a cluster node's advertised identity: its ring ID, failure-domain Zone, and the
// Addr its peers reach it on (used by the replication transport later).
type Member struct {
	ID   string
	Zone string
	Addr string
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
	client  *clientv3.Client
	prefix  string // "{root}/members/"
	self    Member
	leaseID clientv3.LeaseID

	current atomic.Pointer[ring.Ring]

	mu      sync.RWMutex
	members map[string]Member

	cancel context.CancelFunc
	wg     sync.WaitGroup
}

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

	lease, err := client.Grant(ctx, int64(ttl.Seconds()))
	if err != nil {
		return nil, errors.Wrap(err, "grant lease")
	}

	prefix := path.Join(root, "members") + "/"

	if _, err := client.Put(ctx, prefix+self.ID, string(self.encode()), clientv3.WithLease(lease.ID)); err != nil {
		return nil, errors.Wrap(err, "register member")
	}

	m := &Membership{
		client:  client,
		prefix:  prefix,
		self:    self,
		leaseID: lease.ID,
		members: make(map[string]Member),
	}

	// Snapshot the current members, then watch from the snapshot revision so no change is
	// missed in the gap between the Get and the Watch.
	resp, err := client.Get(ctx, prefix, clientv3.WithPrefix())
	if err != nil {
		return nil, errors.Wrap(err, "list members")
	}

	for _, kv := range resp.Kvs {
		if mem, err := decodeMember(kv.Value); err == nil {
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

	go m.keepAlive(bg)                     //nolint:contextcheck // lifetime-scoped, see above
	go m.watch(bg, resp.Header.Revision+1) //nolint:contextcheck // lifetime-scoped, see above

	return m, nil
}

// Ring returns the current ring (lock-free). It is replaced atomically as membership changes.
func (m *Membership) Ring() *ring.Ring { return m.current.Load() }

// LeaseID is this node's membership lease. Ownership claims bind to it so they auto-release
// when the node dies (the basis for the rebalance handoff).
func (m *Membership) LeaseID() clientv3.LeaseID { return m.leaseID }

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
// by a derived timeout.
func (m *Membership) Close(ctx context.Context) error {
	m.cancel()
	m.wg.Wait()

	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	if _, err := m.client.Revoke(ctx, m.leaseID); err != nil {
		return errors.Wrap(err, "revoke lease")
	}

	return nil
}

// keepAlive renews the lease until the context is canceled. If renewal ends (lease lost,
// e.g. an etcd partition longer than the TTL), the goroutine exits and the node is evicted;
// re-registration on lease loss is a later refinement.
func (m *Membership) keepAlive(ctx context.Context) {
	defer m.wg.Done()

	ch, err := m.client.KeepAlive(ctx, m.leaseID)
	if err != nil {
		return
	}

	for {
		select {
		case <-ctx.Done():
			return
		case _, ok := <-ch:
			if !ok {
				return
			}
		}
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
				if mem, err := decodeMember(ev.Kv.Value); err == nil {
					m.set(mem)
					changed = true
				}
			case clientv3.EventTypeDelete:
				m.remove(strings.TrimPrefix(string(ev.Kv.Key), m.prefix))
				changed = true
			}
		}

		if changed {
			m.rebuild()
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
		nodes = append(nodes, ring.Node{ID: mem.ID, Zone: mem.Zone})
	}
	m.mu.RUnlock()

	m.current.Store(ring.New(nodes...))
}

// Package router routes reads and writes by the cluster ring without joining it.
//
// A storage node holds shards, so it obtains the ring as a side effect of registering itself as a
// member. The other tiers — a query front end, an ingester — need the same routing view while
// owning no data: they must resolve a shard's primary and its owners, but must never be placed as
// one. [etcd.Watch] gives them a ring without a lease, and this package turns it into the clients
// that ring is for: the primary write, and the read surface — [Router.Fetcher], the enumeration
// ([Router.Series], [Router.Keys], [Router.Side]) and the metric aggregate pushdown
// ([Router.Aggregate], [Router.AggregateWindow]). Each carries the RPC over the Router's own HTTP
// client under its own retry/hedge profile, so an off-ring reader fails over between a shard's
// owners exactly as a member node does.
//
// A Router is safe for concurrent use and follows membership live: a node joining or failing is
// reflected in the next placement lookup, with no reconnect on the caller's part.
package router

import (
	"context"
	"net"
	"net/http"
	"time"

	"github.com/go-faster/errors"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"

	"github.com/oteldb/storage/cluster"
	"github.com/oteldb/storage/cluster/etcd"
	"github.com/oteldb/storage/cluster/ring"
	"github.com/oteldb/storage/reliability"
	"github.com/oteldb/storage/signal"
)

// Placement is how one shard is placed on the ring.
type Placement struct {
	// RF is the replication factor: how many owners the shard has. The ring clamps it to the
	// membership size.
	RF int
	// Balanced selects the failure-domain-balanced placement erasure-coded shards use, instead of
	// the zone-aware replica placement. It must match what the shard's tenant is configured with,
	// or the router resolves a different owner set than the node does.
	Balanced bool
}

// Config configures a [Router].
type Config struct {
	// Etcd is the etcd endpoint list the cluster coordinates membership through. Required.
	Etcd []string
	// Root is the etcd key prefix for this cluster's state. Empty ⇒ [cluster.DefaultRoot]. It must
	// match the storage nodes' [cluster.Config].Root.
	Root string
	// RF is the default replication factor. Zero ⇒ [cluster.DefaultRF]. It must match the nodes'
	// [cluster.Config].RF, or reads and writes resolve a different owner set than the nodes do.
	RF int
	// ShardsPerTenant must match the nodes' [cluster.Config].ShardsPerTenant. A mismatch routes
	// every write to the wrong shard, silently.
	ShardsPerTenant int
	// Placement, when set, resolves a shard's placement individually — for a cluster where tenants
	// override RF or use erasure coding, which the cluster-wide defaults above cannot express.
	// Nil ⇒ every shard uses RF with the replica placement.
	Placement func(shardKey signal.TenantID) Placement
	// Retry tunes the RPC reliability profile. Zero ⇒ [reliability.Default].
	Retry reliability.RetryConfig
	// HTTP is the client used for node-to-node RPCs. Nil ⇒ one built from Retry.
	HTTP *http.Client
	// Logger records membership changes. Nil ⇒ no logging.
	Logger *zap.Logger
	// DialTimeout bounds the initial etcd connection. Zero ⇒ 5s.
	DialTimeout time.Duration
}

// Router resolves shard placement from a live ring view and carries the routed RPCs.
type Router struct {
	client     *clientv3.Client
	membership *etcd.Membership
	httpc      *http.Client

	rf        int
	shards    int
	placement func(signal.TenantID) Placement
	retry     reliability.RetryConfig
	lg        *zap.Logger
}

// Open connects to etcd and starts following the cluster's membership. The Router registers
// nothing: it never appears in another node's ring and is never placed as an owner.
func Open(ctx context.Context, cfg Config) (*Router, error) {
	if len(cfg.Etcd) == 0 {
		return nil, errors.New("router: etcd endpoints are required")
	}

	dial := cfg.DialTimeout
	if dial <= 0 {
		dial = 5 * time.Second
	}

	client, err := clientv3.New(clientv3.Config{Endpoints: cfg.Etcd, DialTimeout: dial})
	if err != nil {
		return nil, errors.Wrap(err, "etcd client")
	}

	root := cfg.Root
	if root == "" {
		root = cluster.DefaultRoot
	}

	mship, err := etcd.Watch(ctx, client, root)
	if err != nil {
		_ = client.Close()

		return nil, errors.Wrap(err, "watch membership")
	}

	mship.SetLogger(cfg.Logger)

	rc := cfg.Retry
	if !rc.Enabled() {
		rc = reliability.Default()
	}

	rf := cfg.RF
	if rf <= 0 {
		rf = cluster.DefaultRF
	}

	httpc := cfg.HTTP
	if httpc == nil {
		httpc = newHTTPClient(rc)
	}

	lg := cfg.Logger
	if lg == nil {
		lg = zap.NewNop()
	}

	return &Router{
		client:     client,
		membership: mship,
		httpc:      httpc,
		rf:         rf,
		shards:     cluster.ShardCount(cfg.ShardsPerTenant),
		placement:  cfg.Placement,
		retry:      rc,
		lg:         lg,
	}, nil
}

// Close stops following membership and releases the etcd connection.
func (r *Router) Close(ctx context.Context) error {
	err := r.membership.Close(ctx)
	if cerr := r.client.Close(); cerr != nil && err == nil {
		err = cerr
	}

	return errors.Wrap(err, "close router")
}

// Members returns the cluster's current members, sorted by ID.
func (r *Router) Members() []etcd.Member { return r.membership.Members() }

// ShardCount is how many shards each tenant is split into.
func (r *Router) ShardCount() int { return r.shards }

// ShardKey returns the shard key a series routes to. Both tiers derive it the same way, so an
// ingester's routing and a node's placement agree without coordinating.
func (r *Router) ShardKey(tenant signal.TenantID, id signal.SeriesID) signal.TenantID {
	return cluster.ShardKeyOf(tenant, cluster.ShardOf(id, r.shards), r.shards)
}

// ShardKeys returns every shard key of a tenant, in index order — the fan-out set for a read,
// which cannot know which shard holds a series before matching one.
func (r *Router) ShardKeys(tenant signal.TenantID) []signal.TenantID {
	return cluster.ShardKeys(tenant, r.shards)
}

// Owners returns the addresses of a shard's ring owners, in placement order (the first is the
// primary). It is empty when the ring is empty.
func (r *Router) Owners(shardKey signal.TenantID) []string {
	nodes := r.lookup(shardKey)

	addrs := make([]string, 0, len(nodes))
	for _, n := range nodes {
		if addr := r.membership.AddrOf(n.ID); addr != "" {
			addrs = append(addrs, addr)
		}
	}

	return addrs
}

// Primary returns the address of a shard's ring primary — the single authority that admits its
// writes — or false when the ring is empty or the primary has no known address.
func (r *Router) Primary(shardKey signal.TenantID) (string, bool) {
	node, ok := r.membership.Ring().Primary([]byte(shardKey))
	if !ok {
		return "", false
	}

	addr := r.membership.AddrOf(node.ID)

	return addr, addr != ""
}

// lookup resolves a shard's owner nodes under its placement.
func (r *Router) lookup(shardKey signal.TenantID) []ring.Node {
	p := Placement{RF: r.rf}
	if r.placement != nil {
		p = r.placement(shardKey)
		if p.RF <= 0 {
			p.RF = r.rf
		}
	}

	cur := r.membership.Ring()
	if p.Balanced {
		return cur.LookupBalanced([]byte(shardKey), p.RF)
	}

	return cur.Lookup([]byte(shardKey), p.RF)
}

// newHTTPClient builds the RPC client. It sets connection-level timeouts so a dead peer fails fast,
// but leaves the request itself unbounded: per-attempt deadlines come from the retry/hedge layer
// via context, which an http.Client.Timeout would fight (it would abort an attempt the hedge layer
// still wants to race).
func newHTTPClient(c reliability.RetryConfig) *http.Client {
	dialTimeout := c.PerTryTimeout
	if dialTimeout <= 0 || dialTimeout > 5*time.Second {
		dialTimeout = 5 * time.Second
	}

	return &http.Client{
		Transport: &http.Transport{
			Proxy:                 http.ProxyFromEnvironment,
			DialContext:           (&net.Dialer{Timeout: dialTimeout, KeepAlive: 30 * time.Second}).DialContext,
			ForceAttemptHTTP2:     true,
			MaxIdleConns:          100,
			MaxIdleConnsPerHost:   8,
			IdleConnTimeout:       90 * time.Second,
			TLSHandshakeTimeout:   dialTimeout,
			ExpectContinueTimeout: time.Second,
			ResponseHeaderTimeout: c.PerTryTimeout,
		},
	}
}

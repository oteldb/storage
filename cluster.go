package storage

import (
	"bytes"
	"cmp"
	"context"
	"math"
	"net"
	"net/http"
	"slices"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-faster/errors"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"

	"github.com/oteldb/storage/cluster"
	"github.com/oteldb/storage/cluster/ec"
	"github.com/oteldb/storage/cluster/etcd"
	"github.com/oteldb/storage/cluster/partsync"
	"github.com/oteldb/storage/cluster/replica"
	"github.com/oteldb/storage/engine"
	"github.com/oteldb/storage/internal/parallel"
	"github.com/oteldb/storage/internal/retry"
	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/recordengine"
	"github.com/oteldb/storage/reliability"
	"github.com/oteldb/storage/signal"
	"github.com/oteldb/storage/signal/metric"
	"github.com/oteldb/storage/signal/profile"
	tenantpkg "github.com/oteldb/storage/tenant"
)

const httpScheme = "http"

// clusterNode is the cluster runtime a [Storage] owns in cluster mode: the etcd client and
// membership, the replica server, and the routed write path.
type clusterNode struct {
	client     *clientv3.Client
	membership *etcd.Membership
	ownership  *etcd.Ownership
	replicator *replica.Replicator // primary→secondary replication
	httpc      *http.Client        // primary-write client
	server     *http.Server
	listener   net.Listener
	self       string // this node's address
	rf         int
	shards     int                     // metric shards per tenant (≤1 ⇒ one shard = the tenant)
	retry      reliability.RetryConfig // transport reliability profile (timeouts, retries, hedging)
	private    bool                    // per-node private backend: flushed parts sync node-to-node
	psync      *partsync.Syncer        // part mirroring for private backends (nil when shared)

	notifyMu   sync.Mutex          // guards notifyBusy
	notifyBusy map[string]struct{} // engine prefixes with a notify-triggered sync in flight

	// reportedRejoins is the re-registration total already published as a counter delta.
	reportedRejoins atomic.Int64
}

// recordMembershipHealth publishes this node's standing in the cluster member set once per
// maintenance cycle. A node absent from etcd stays up and keeps serving — that is deliberate,
// since restarting it would lose a secondary's in-memory head and open a read gap — so nothing
// about the node itself reports the state. This is what makes it alertable.
func (s *Storage) recordMembershipHealth(ctx context.Context) {
	if s.cluster == nil {
		return
	}

	total := s.cluster.membership.Rejoins()
	delta := total - s.cluster.reportedRejoins.Swap(total)

	s.obs.Cluster.Record(ctx, s.cluster.membership.SelfAbsent(), delta)
}

// rfFor resolves the replication factor for one shard key: the tenant's
// [tenant.Durability] RF when set, else the cluster-wide default (cluster.Config.RF).
// Policy is per real tenant — a shard key ({tenant}/_s{idx}) collapses via tenantOfShard —
// so every shard of a tenant shares one RF. The ring clamps the result to the membership
// size at lookup time.
func (s *Storage) rfFor(shardKey signal.TenantID) int {
	d := s.tenant.Resolve(s.normalizeTenant(tenantOfShard(shardKey))).Durability

	// Erasure coding fixes the owner set at Data+Parity: the head is full-copy replicated to
	// every shard owner (so each can flush/convert or hold its shard), and RF is ignored.
	if d.EC != nil {
		if scheme := (ec.Scheme{Data: d.EC.Data, Parity: d.EC.Parity}); scheme.Validate() == nil {
			return scheme.Shards()
		}
	}

	if d.RF > 0 {
		return d.RF
	}

	return s.cluster.rf
}

// syncParts mirrors one engine prefix from the shard's peer owners into this node's private
// backend (cluster/partsync), reporting whether anything was mirrored. It is the shared-nothing
// counterpart of reading flushed parts from a shared store: a replica mirrors its owner's
// objects before each refresh (strict=false), and a compaction owner backfills strictly-newer
// peer parts before it flushes (strict=true — a stale peer copy must never overwrite the
// owner's own newer index). A no-op in single-node mode, with a shared backend, or when the
// shard has no reachable peers; errors are swallowed like the rest of the maintenance loop
// (the next pass retries).
func (s *Storage) syncParts(ctx context.Context, tid signal.TenantID, signalPrefix string, strict bool) bool {
	if s.cluster == nil || !s.cluster.private {
		return false
	}

	_, remotes := s.shardOwners(tid)
	if len(remotes) == 0 {
		return false
	}

	enginePrefix := string(s.normalizeTenant(tid)) + signalPrefix

	st, err := s.cluster.psync.Sync(ctx, enginePrefix, remotes, strict, s.ecKeepFilter(tid))
	if err != nil {
		s.obs.Logger(ctx).Warn("part sync failed",
			zap.String("prefix", enginePrefix), zap.Bool("strict", strict), zap.Error(err))

		return false
	}

	if st.Synced && st.Copied > 0 {
		s.obs.Logger(ctx).Debug("part sync mirrored peer objects",
			zap.String("prefix", enginePrefix), zap.Bool("strict", strict),
			zap.Int("copied", st.Copied), zap.Int64("bytes", st.CopiedBytes), zap.Int("pruned", st.Pruned))
	}

	return st.Synced
}

// splitEnginePrefix parses an engine prefix ("{tenant}{signalPrefix}", e.g. "default/metrics")
// back into the engine-map tenant key and its signal, rejecting anything else.
func splitEnginePrefix(prefix string) (tid signal.TenantID, sig signal.Signal, ok bool) {
	i := strings.LastIndex(prefix, "/")
	if i <= 0 { // no separator, or empty tenant
		return "", 0, false
	}

	switch "/" + prefix[i+1:] {
	case metricsPrefix:
		sig = signal.Metric
	case logsPrefix:
		sig = signal.Log
	case tracesPrefix:
		sig = signal.Trace
	case profilesPrefix:
		sig = signal.Profile
	default:
		return "", 0, false
	}

	return signal.TenantID(prefix[:i]), sig, true
}

// partsNotifyHandler receives an owner's flush notification (shared-nothing mode) and mirrors
// the named engine prefix immediately — a replica converges right after the owner's flush
// instead of on its next maintenance tick. The mirror runs asynchronously (202) and coalesces:
// a prefix with a notify-triggered sync already in flight is dropped, since the periodic pull
// is the anti-entropy source of truth anyway.
func (s *Storage) partsNotifyHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if req.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)

			return
		}

		prefix := req.URL.Query().Get("prefix")

		tid, sig, ok := splitEnginePrefix(prefix)
		if !ok || !partsync.ValidKey(prefix) {
			http.Error(w, "invalid prefix", http.StatusBadRequest)

			return
		}

		cn := s.cluster
		if cn == nil || !cn.private {
			w.WriteHeader(http.StatusOK) // shared store: nothing to mirror, flush is already visible

			return
		}

		cn.notifyMu.Lock()
		if _, busy := cn.notifyBusy[prefix]; busy {
			cn.notifyMu.Unlock()
			w.WriteHeader(http.StatusAccepted) // coalesced: a sync for this prefix is in flight

			return
		}
		cn.notifyBusy[prefix] = struct{}{}
		cn.notifyMu.Unlock()

		// The mirror deliberately detaches from the request context: the 202 returns now and
		// the sync outlives the request (bounded by its own timeout below).
		//nolint:contextcheck // intentional detach, see above
		go func() {
			defer func() {
				cn.notifyMu.Lock()
				delete(cn.notifyBusy, prefix)
				cn.notifyMu.Unlock()
			}()

			ctx, cancel := context.WithTimeout(s.obs.Base(context.Background()), time.Minute)
			defer cancel()

			if !s.syncParts(ctx, tid, "/"+prefix[strings.LastIndex(prefix, "/")+1:], false) {
				return
			}

			// Mirrored something: load it and trim the head, like the maintenance refresh.
			if sig == signal.Metric {
				if eng, ok := s.lookupEngine(tid); ok {
					_ = eng.RefreshReplica(ctx)
				}
			} else if eng, ok := s.lookupRecordEngine(sig, tid); ok {
				_ = eng.RefreshReplica(ctx)
			}
		}()

		w.WriteHeader(http.StatusAccepted)
	})
}

// notifyPeers tells the shard's secondary owners that this node just flushed/merged the engine
// prefix, so their replicas mirror immediately (shared-nothing mode only). Best-effort and
// asynchronous — an unreachable peer converges on its next maintenance tick.
func (s *Storage) notifyPeers(ctx context.Context, tid signal.TenantID, signalPrefix string) {
	if s.cluster == nil || !s.cluster.private {
		return
	}

	_, remotes := s.shardOwners(tid)
	if len(remotes) == 0 {
		return
	}

	enginePrefix := string(s.normalizeTenant(tid)) + signalPrefix
	client := &partsync.Client{HTTP: s.cluster.httpc}
	log := s.obs.Logger(ctx)

	for _, addr := range remotes {
		// Detached on purpose: the notify must not block the maintenance pass, and its
		// lifetime is its own short timeout, not the caller's.
		//nolint:gosec,contextcheck // G118 / context: intentional detach, see above
		go func() {
			nctx, cancel := context.WithTimeout(s.obs.Base(context.Background()), 10*time.Second)
			defer cancel()

			if err := client.Notify(nctx, addr, enginePrefix); err != nil {
				log.Debug("flush notify failed", zap.String("peer", addr),
					zap.String("prefix", enginePrefix), zap.Error(err))
			}
		}()
	}
}

// shardCount is the configured metric shards per tenant, clamped to a minimum of 1.
func (n *clusterNode) shardCount() int { return cluster.ShardCount(n.shards) }

// The shard-key vocabulary lives in [cluster] so every tier that routes derives it identically;
// these are the node's names for it.

func shardKeyOf(tenant signal.TenantID, idx, n int) signal.TenantID {
	return cluster.ShardKeyOf(tenant, idx, n)
}

func tenantOfShard(shardKey signal.TenantID) signal.TenantID {
	return cluster.TenantOfShard(shardKey)
}

func shardOf(id signal.SeriesID, n int) int { return cluster.ShardOf(id, n) }

// startCluster joins the etcd-coordinated cluster, runs the replica server on Self.Addr, and
// builds the routed write path. A replicated write received from a peer is applied to the
// local engine via [engine.Engine.ApplyReplicated].
func (s *Storage) startCluster(ctx context.Context, cfg *cluster.Config) error {
	rf, root := cfg.RF, cfg.Root
	if rf <= 0 {
		rf = cluster.DefaultRF
	}

	if root == "" {
		root = cluster.DefaultRoot
	}

	client, err := clientv3.New(clientv3.Config{Endpoints: cfg.Etcd, DialTimeout: 5 * time.Second})
	if err != nil {
		return errors.Wrap(err, "etcd client")
	}

	rc := s.opts.retryConfig()
	httpc := newClusterHTTPClient(rc)

	var lc net.ListenConfig

	ln, err := lc.Listen(ctx, "tcp", cfg.Self.Addr)
	if err != nil {
		_ = client.Close()

		return errors.Wrapf(err, "listen on %q", cfg.Self.Addr)
	}

	// An ephemeral bind (port 0) advertises the port the kernel actually assigned — the caller
	// asked for "any port", so the listener is the only source of truth. This also removes the
	// probe-then-rebind race from tests that need throwaway ports. An explicit port advertises
	// the configured address verbatim.
	self := cfg.Self
	if _, port, err := net.SplitHostPort(self.Addr); err == nil && port == "0" {
		self.Addr = ln.Addr().String()
	}

	// The replicator applies an inbound (or local) write to the addressed tenant's engine. Its
	// transport shares the tuned client (connection timeouts) so replication tolerates a slow peer.
	rp := replica.New(self.Addr, replica.NewHTTPTransport(httpc), s.applyReplicated)

	mux := http.NewServeMux()
	mux.Handle(replica.ReplicatePath, rp.Handler())               // secondary: trusting apply
	mux.Handle(cluster.PrimaryWritePath, s.primaryWriteHandler()) // primary: OOO apply + replicate
	// read fan-out across metric/log/trace/profile signals.
	mux.Handle(cluster.ReadPath, cluster.ReadHandler(s.localFetch,
		s.recordFetchFunc(signal.Log), s.recordFetchFunc(signal.Trace), s.recordFetchFunc(signal.Profile), s.clusterOpts...))
	// Metric aggregate pushdown: disjoint step buckets, and the overlapping-window variant.
	mux.Handle(cluster.AggregatePath, cluster.AggregateHandler(s.localAggregate, s.clusterOpts...))
	mux.Handle(cluster.AggregateWindowPath, cluster.AggregateWindowHandler(s.localAggregateWindow, s.clusterOpts...))
	// record-signal series enumeration (log/trace/profile)
	mux.Handle(cluster.SeriesPath, cluster.SeriesHandler(s.localSeries, s.clusterOpts...))
	// record-signal attribute-key enumeration
	mux.Handle(cluster.KeysPath, cluster.KeysHandler(s.localKeys, s.clusterOpts...))
	// record-signal distinct column-value enumeration
	mux.Handle(cluster.ValuesPath, cluster.ValuesHandler(s.localValues, s.clusterOpts...))
	// profile symbol store
	mux.Handle(cluster.SidePath, cluster.SideHandler(s.localProfileSymbols, s.clusterOpts...))
	// Part mirroring for per-node private backends: peers list and fetch this node's backend
	// objects. Mounted unconditionally (read-only; useful for operator inspection), used by the
	// maintenance loop only when Config.PrivateBackend is set.
	mux.Handle(partsync.ListPath, partsync.ListHandler(s.backend))
	mux.Handle(partsync.ObjectPath, partsync.ObjectHandler(s.backend))
	mux.Handle(partsync.NotifyPath, s.partsNotifyHandler())
	srv := &http.Server{
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
		// Seed the zctx base so internal handlers can log their own faults.
		BaseContext: func(net.Listener) context.Context { return s.obs.Base(context.Background()) },
	}

	go func() { _ = srv.Serve(ln) }()

	mship, err := etcd.Join(ctx, client, root, self, cfg.MemberTTL)
	if err != nil {
		_ = srv.Close()
		_ = client.Close()

		return errors.Wrap(err, "join cluster")
	}

	mship.SetLogger(s.obs.Log)
	s.obs.Logger(ctx).Info("joined cluster",
		zap.String("id", self.ID), zap.String("zone", self.Zone), zap.String("addr", self.Addr))

	own := etcd.NewOwnership(client, root, cfg.Self.ID, mship.LeaseID())

	// Losing the membership lease takes the compaction claims bound to it with it, so the
	// claims have to follow the node's new lease when it re-registers.
	mship.OnRejoin(own.SetLease)

	// A rejoin is not the only way a claim dies: a node that simply stops renewing keeps its
	// held set and would go on flushing and stamping indexes for shards etcd has reassigned.
	// The lease deadline expires the claims locally, without waiting for a rejoin that may
	// never come.
	own.SetFence(mship.Fenced)

	s.cluster = &clusterNode{
		client:     client,
		membership: mship,
		ownership:  own,
		replicator: rp,
		httpc:      httpc,
		server:     srv,
		listener:   ln,
		self:       self.Addr,
		rf:         rf,
		shards:     cfg.ShardsPerTenant,
		retry:      rc,
		private:    cfg.PrivateBackend,
		psync:      partsync.New(s.backend, &partsync.Client{HTTP: httpc}),
		notifyBusy: make(map[string]struct{}),
	}

	// Record rebalance plans at each shard's own (per-tenant) replication factor, so
	// LastRebalance in the operator stats shows the full owner-set moves — the replicas that
	// must backfill — not just the compaction-primary handoff. Claims stay primary-only.
	// Safe here: Reconcile first runs from the maintenance loop, after s.cluster is set.
	s.cluster.ownership.SetPlanRF(func(shard string) int { return s.rfFor(signal.TenantID(shard)) })

	return nil
}

// localFetch serves a peer's fetch from the local engine, pushing down the (equality) matchers
// it forwarded — the read-fan-out server's view of this node's data.
func (s *Storage) localFetch(ctx context.Context, tenant string, start, end int64, matchers []fetch.Matcher) ([]*fetch.Batch, error) {
	tid := s.normalizeTenant(signal.TenantID(tenant))

	eng, ok := s.lookupEngine(tid)
	if !ok || !s.canAnswer(ctx, rpcOpRead, signal.Metric, tid, start, end) {
		return nil, cluster.ErrShardAbsent
	}

	// Recycle: the read handler serializes the batches and discards them, so it releases them right
	// after — recycling this node's result buffers across fan-out reads.
	it, err := eng.Fetch(ctx, fetch.Request{Tenant: signal.TenantID(tenant), Start: start, End: end, Matchers: matchers, Recycle: true})
	if err != nil {
		return nil, err
	}

	return fetch.Drain(ctx, it)
}

// clusterFetcherFor returns the read seam for one tenant in cluster mode. A tenant's series are
// spread across N shards (each a separately-placed ring unit), so a query gathers across every
// shard and merges: for each shard, serve locally if this node owns it, else fan out to an owner.
// With a single shard this is exactly the unsharded owner-aware fetch.
func (s *Storage) clusterFetcherFor(tid signal.TenantID) fetch.Fetcher {
	cn := s.cluster
	tenant := s.normalizeTenant(tid)
	n := cn.shardCount()

	shardFetchers := make([]fetch.Fetcher, 0, n)
	for idx := range n {
		sk := shardKeyOf(tenant, idx, n)
		// Stamp the shard key as the request tenant so a remote peer serves the right shard engine
		// (and a local engine ignores it). scopedFetcher does the stamping.
		shardFetchers = append(shardFetchers, scopedFetcher{inner: s.shardFetcher(sk), scope: sk})
	}

	return clusterSeriesFetcher{Fetcher: fetch.Merge(shardFetchers...), store: s, tenant: tenant}
}

// clusterSeriesFetcher adds the [fetch.SeriesLister] capability to a tenant's cluster read seam.
// The shard fan-out under it is a multi-child merge, which opts out of the fetcher capabilities
// (their per-child semantics do not compose), so without this layer the label endpoints would fall
// back to draining every sample of every matching series. Series enumeration *does* compose: shards
// partition a tenant's series, so the gather is a concatenation — exactly what
// [Storage.clusterSeries] already does for the record signals.
type clusterSeriesFetcher struct {
	fetch.Fetcher

	store  *Storage
	tenant signal.TenantID
}

func (f clusterSeriesFetcher) Series(ctx context.Context, r fetch.Request) ([]signal.Series, error) {
	series, err := f.store.clusterSeries(ctx, signal.Metric, f.tenant, r.Matchers, r.Start, r.End)
	if err != nil {
		return nil, err
	}

	// The gather concatenates per-shard results (each ascending, the shards disjoint); the
	// capability promises one ascending stream.
	return fetch.SortSeries(series), nil
}

// shardFetcher returns the read seam for one metric shard: the local engine if this node holds the
// shard (full matcher pushdown), else a fail-over across the shard's other owners (each owner's
// copy is complete; matchers are re-applied to the returned superset).
func (s *Storage) shardFetcher(shardKey signal.TenantID) fetch.Fetcher {
	local, remotes, absent := s.shardReadTargets(signal.Metric, shardKey)
	if local {
		if e, ok := s.lookupEngine(shardKey); ok {
			return s.gapGuarded(signal.Metric, shardKey, e, remotes)
		}
	}

	if len(remotes) == 0 {
		return fetch.Merge() // no owner holds the shard: it has no data anywhere
	}

	return fetch.Filter(hedgedFetcher{
		store: s, op: rpcOpRead, remotes: remotes, absentShard: absentOf(absent, signal.Metric, shardKey),
	})
}

// shardReadTargets resolves a shard's read targets: whether this node holds the shard itself, and a
// remote fetcher per other owner to fail over to. A node the ring points at but that holds no data
// reports local=false and absent=true, so the read reaches an owner that does instead of answering
// empty (the caller reports the anomaly once it has a request context).
func (s *Storage) shardReadTargets(
	sig signal.Signal, shardKey signal.TenantID,
) (local bool, remotes []fetch.Fetcher, absent bool) {
	cn := s.cluster

	for _, o := range s.ownerLookup(shardKey) {
		addr := cn.membership.AddrOf(o.ID)

		switch {
		case addr == cn.self:
			if s.holdsShard(sig, shardKey) {
				local = true
			} else {
				absent = true
			}
		case addr != "":
			remotes = append(remotes, cluster.NewRemoteFetcher(sig, addr, cn.httpc, s.clusterOpts...))
		}
	}

	return local, remotes, absent
}

// localAggregate serves a peer's metric aggregate from the local shard engine, pushing down the
// (equality) matchers it forwarded — the receiving side of [cluster.AggregateHandler].
func (s *Storage) localAggregate(
	ctx context.Context, tenant string, start, end, step int64, matchers []fetch.Matcher,
) ([]engine.NamedAgg, error) {
	tid := s.normalizeTenant(signal.TenantID(tenant))

	eng, ok := s.lookupEngine(tid)
	if !ok || !s.canAnswer(ctx, rpcOpRead, signal.Metric, tid, start, end) {
		return nil, cluster.ErrShardAbsent
	}

	return eng.AggregateStepNamed(ctx, fetch.Request{
		Tenant: signal.TenantID(tenant), Start: start, End: end, Matchers: matchers,
	}, step)
}

// localAggregateWindow serves a peer's overlapping-window aggregate from the local shard engine —
// the receiving side of [cluster.AggregateWindowHandler], the window form of [Storage.localAggregate].
func (s *Storage) localAggregateWindow(
	ctx context.Context, tenant string, start, end int64, spec engine.WindowSpec, matchers []fetch.Matcher,
) ([]engine.NamedWindowAgg, error) {
	tid := s.normalizeTenant(signal.TenantID(tenant))

	eng, ok := s.lookupEngine(tid)
	if !ok || !s.canAnswer(ctx, rpcOpRead, signal.Metric, tid, start, end) {
		return nil, cluster.ErrShardAbsent
	}

	return eng.AggregateWindowNamed(ctx, fetch.Request{
		Tenant: signal.TenantID(tenant), Start: start, End: end, Matchers: matchers,
	}, spec)
}

// mergeWindowLists combines two per-series window lists by aligned evaluation timestamp, summing
// counts/sums and taking min/max — used only where a series surfaces from more than one shard. Each
// input window is non-empty (Count > 0), so its Min/Max are valid.
func mergeWindowLists(a, b []engine.WindowAgg) []engine.WindowAgg {
	byEnd := make(map[int64]engine.SeriesAgg, len(a)+len(b))

	fold := func(x engine.WindowAgg) {
		e, ok := byEnd[x.End]
		if !ok {
			byEnd[x.End] = x.SeriesAgg

			return
		}

		byEnd[x.End] = engine.SeriesAgg{
			Count: e.Count + x.Count,
			Sum:   e.Sum + x.Sum,
			Min:   min(e.Min, x.Min),
			Max:   max(e.Max, x.Max),
		}
	}

	for _, x := range a {
		fold(x)
	}

	for _, x := range b {
		fold(x)
	}

	out := make([]engine.WindowAgg, 0, len(byEnd))
	for end, agg := range byEnd {
		out = append(out, engine.WindowAgg{End: end, SeriesAgg: agg})
	}

	slices.SortFunc(out, func(x, y engine.WindowAgg) int { return cmp.Compare(x.End, y.End) })

	return out
}

// clusterAggregateFor computes a tenant's step-bucketed aggregate across all its shards in cluster
// mode, preserving the pushdown: each shard's owner runs the aggregate locally (from its stats
// sidecar where it applies) and ships compact per-series buckets, which the coordinator re-checks
// against the full matcher set and unions. Series are shard-partitioned, so the union rarely needs
// to merge, but it does so defensively.
func (s *Storage) clusterAggregateFor(
	ctx context.Context, tid signal.TenantID, r fetch.Request, step int64,
) (map[signal.SeriesID][]engine.BucketAgg, error) {
	cn := s.cluster
	tenant := s.normalizeTenant(tid)
	n := cn.shardCount()

	out := make(map[signal.SeriesID][]engine.BucketAgg)

	for idx := range n {
		sk := shardKeyOf(tenant, idx, n)

		named, err := s.shardAggregate(ctx, sk, r, step)
		if err != nil {
			return nil, err
		}

		unionNamed(out, named, r.Matchers)
	}

	return out, nil
}

// clusterAggregateNamedFor is the labeled variant of [Storage.clusterAggregateFor]: it computes a
// tenant's step-bucketed aggregate across all its shards but keeps each series' identity (so the
// coordinator can render labels). It backs the labeled [Storage.AggregateMetricsNamed] pushdown
// path in cluster mode.
func (s *Storage) clusterAggregateNamedFor(
	ctx context.Context, tid signal.TenantID, r fetch.Request, step int64,
) ([]engine.NamedAgg, error) {
	return gatherShards(s, tid, r.Matchers,
		func(sk signal.TenantID) ([]engine.NamedAgg, error) { return s.shardAggregate(ctx, sk, r, step) },
		func(a *engine.NamedAgg) signal.Series { return a.Series },
		func(dst, src *engine.NamedAgg) { dst.Buckets = mergeBucketLists(dst.Buckets, src.Buckets) })
}

// clusterAggregateWindowNamedFor computes a tenant's overlapping-window aggregate across all its
// shards, the window form of [Storage.clusterAggregateNamedFor]: each shard's owner slides its own
// windows (keeping the sidecar pushdown local) and ships one compact entry per series. A series that
// surfaces from more than one shard has its windows merged by evaluation timestamp — exact, since
// the shards hold disjoint samples.
func (s *Storage) clusterAggregateWindowNamedFor(
	ctx context.Context, tid signal.TenantID, r fetch.Request, spec engine.WindowSpec,
) ([]engine.NamedWindowAgg, error) {
	return gatherShards(s, tid, r.Matchers,
		func(sk signal.TenantID) ([]engine.NamedWindowAgg, error) {
			return s.shardAggregateWindow(ctx, sk, r, spec)
		},
		func(a *engine.NamedWindowAgg) signal.Series { return a.Series },
		func(dst, src *engine.NamedWindowAgg) { dst.Windows = mergeWindowLists(dst.Windows, src.Windows) })
}

// gatherShards folds every shard of a tenant into one per-series list: shard fetches a shard's
// entries (locally or from a remote owner), the coordinator drops series that fail the full matcher
// set — a remote peer applied only the equality subset — and merges any series that surfaces from
// more than one shard. Series are shard-partitioned, so the merge is rare, but it runs defensively.
// It is the shared body of the stepped and windowed fan-outs, which differ only in what one series'
// aggregates look like.
func gatherShards[T any](
	s *Storage, tid signal.TenantID, matchers []fetch.Matcher,
	shard func(shardKey signal.TenantID) ([]T, error),
	seriesOf func(*T) signal.Series,
	merge func(dst, src *T),
) ([]T, error) {
	tenant := s.normalizeTenant(tid)
	n := s.cluster.shardCount()

	var out []T

	index := make(map[signal.SeriesID]int, n) // id → position in out, to merge a series seen twice

	for idx := range n {
		named, err := shard(shardKeyOf(tenant, idx, n))
		if err != nil {
			return nil, err
		}

		for i := range named {
			na := &named[i]

			series := seriesOf(na)
			if !fetch.MatchesSeries(series, matchers) {
				continue
			}

			id := series.Hash()
			if j, ok := index[id]; ok {
				merge(&out[j], na)

				continue
			}

			index[id] = len(out)
			out = append(out, *na)
		}
	}

	return out, nil
}

// shardAggregate gets one metric shard's per-series step buckets.
func (s *Storage) shardAggregate(
	ctx context.Context, shardKey signal.TenantID, r fetch.Request, step int64,
) ([]engine.NamedAgg, error) {
	eq := equalityMatchers(r.Matchers)

	return shardAggregateWith(ctx, s, shardKey,
		func(eng *engine.Engine) ([]engine.NamedAgg, error) {
			return eng.AggregateStepNamed(ctx, fetch.Request{
				Tenant: shardKey, Start: r.Start, End: r.End, Matchers: r.Matchers,
			}, step)
		},
		func(addr string) ([]engine.NamedAgg, error) {
			return cluster.NewRemoteAggregator(addr, s.cluster.httpc, s.clusterOpts...).
				Aggregate(ctx, string(shardKey), r.Start, r.End, step, eq)
		})
}

// shardAggregateWindow gets one metric shard's per-series evaluation windows.
func (s *Storage) shardAggregateWindow(
	ctx context.Context, shardKey signal.TenantID, r fetch.Request, spec engine.WindowSpec,
) ([]engine.NamedWindowAgg, error) {
	eq := equalityMatchers(r.Matchers)

	return shardAggregateWith(ctx, s, shardKey,
		func(eng *engine.Engine) ([]engine.NamedWindowAgg, error) {
			return eng.AggregateWindowNamed(ctx, fetch.Request{
				Tenant: shardKey, Start: r.Start, End: r.End, Matchers: r.Matchers,
			}, spec)
		},
		func(addr string) ([]engine.NamedWindowAgg, error) {
			return cluster.NewRemoteAggregator(addr, s.cluster.httpc, s.clusterOpts...).
				AggregateWindow(ctx, string(shardKey), r.Start, r.End, spec, eq)
		})
}

// shardAggregateWith serves one shard's aggregates: locally (full matcher pushdown) if this node
// holds the shard, else from another owner with sequential failover (equality matchers pushed; the
// coordinator re-checks the full set on the returned identities).
func shardAggregateWith[T any](
	ctx context.Context, s *Storage, shardKey signal.TenantID,
	local func(*engine.Engine) ([]T, error),
	remote func(addr string) ([]T, error),
) ([]T, error) {
	isLocal, remotes := s.shardPlacement(ctx, rpcOpRead, signal.Metric, shardKey)
	if isLocal {
		eng, ok := s.lookupEngine(shardKey)
		if !ok {
			return nil, nil
		}

		return local(eng)
	}

	var (
		lastErr error
		absent  int
	)

	for _, addr := range remotes {
		got, err := remote(addr)
		if err == nil {
			return got, nil
		}

		if errors.Is(err, cluster.ErrShardAbsent) {
			absent++ // an owner that holds nothing is no answer at all, not an empty one
		}

		lastErr = err
	}

	if absent == len(remotes) { // every owner disclaims the shard: it has no data anywhere
		return nil, nil
	}

	return nil, lastErr // nil when there were no reachable owners (treated as no data)
}

// equalityMatchers extracts the serializable (equality) subset of a request's matchers.
func equalityMatchers(matchers []fetch.Matcher) []fetch.EqualMatcher {
	var eq []fetch.EqualMatcher
	for i := range matchers {
		if matchers[i].Spec != nil {
			eq = append(eq, *matchers[i].Spec)
		}
	}

	return eq
}

// unionNamed folds a shard's per-series aggregates into out, dropping series that fail the full
// matcher set (a remote peer applied only the equality subset) and merging buckets for any series
// id that already has an entry.
func unionNamed(out map[signal.SeriesID][]engine.BucketAgg, named []engine.NamedAgg, matchers []fetch.Matcher) {
	for i := range named {
		na := &named[i]
		if !fetch.MatchesSeries(na.Series, matchers) {
			continue
		}

		id := na.Series.Hash()
		if existing, ok := out[id]; ok {
			out[id] = mergeBucketLists(existing, na.Buckets)
		} else {
			out[id] = na.Buckets
		}
	}
}

// mergeBucketLists combines two per-series bucket lists by aligned start, summing counts/sums and
// taking min/max — used only on the rare path where a series surfaces from more than one shard. Each
// input bucket is non-empty (Count > 0), so its Min/Max are valid.
func mergeBucketLists(a, b []engine.BucketAgg) []engine.BucketAgg {
	byStart := make(map[int64]engine.SeriesAgg, len(a)+len(b))

	fold := func(x engine.BucketAgg) {
		e, ok := byStart[x.Start]
		if !ok {
			byStart[x.Start] = x.SeriesAgg

			return
		}

		byStart[x.Start] = engine.SeriesAgg{
			Count: e.Count + x.Count,
			Sum:   e.Sum + x.Sum,
			Min:   min(e.Min, x.Min),
			Max:   max(e.Max, x.Max),
		}
	}

	for _, x := range a {
		fold(x)
	}

	for _, x := range b {
		fold(x)
	}

	out := make([]engine.BucketAgg, 0, len(byStart))
	for start, agg := range byStart {
		out = append(out, engine.BucketAgg{Start: start, SeriesAgg: agg})
	}

	sort.Slice(out, func(i, j int) bool { return out[i].Start < out[j].Start })

	return out
}

// localRecordFetch serves a peer's fetch for one record signal (logs, traces, or profiles) from the
// local engine, pushing down the (equality) stream matchers it forwarded — the receiving side of
// [cluster.ReadHandler] for everything but metrics.
func (s *Storage) localRecordFetch(
	ctx context.Context, sig signal.Signal, tenant string, start, end int64, matchers []fetch.Matcher,
) ([]*fetch.Batch, error) {
	tid := s.normalizeTenant(signal.TenantID(tenant))

	// This path reaches the engine directly rather than through [Storage.recordFetcher], so it must
	// install the read budget itself — otherwise a peer serving a fan-out admits nothing and
	// materializes the whole result the caller has already said it has no room for.
	ctx = withReadBudget(ctx, s.maxQueryBytes)

	eng, ok := s.lookupRecordEngine(sig, tid)
	if !ok || !s.canAnswer(ctx, rpcOpRead, sig, tid, start, end) {
		return nil, cluster.ErrShardAbsent
	}

	it, err := eng.Fetch(ctx, fetch.Request{
		Signal: sig, Tenant: signal.TenantID(tenant), Start: start, End: end, Matchers: matchers,
	})
	if err != nil {
		return nil, err
	}

	return fetch.Drain(ctx, it)
}

// recordFetchFunc binds [Storage.localRecordFetch] to one signal, for the read handler's per-signal
// arguments.
func (s *Storage) recordFetchFunc(sig signal.Signal) cluster.FetchFunc {
	return func(ctx context.Context, tenant string, start, end int64, matchers []fetch.Matcher) ([]*fetch.Batch, error) {
		return s.localRecordFetch(ctx, sig, tenant, start, end, matchers)
	}
}

// clusterLogFetcherFor returns the log read seam for one tenant in cluster mode: local if this
// node owns the tenant, otherwise fanned out to an owner (a window+matcher superset re-filtered
// here), failing over between owners — the logs analog of [Storage.clusterFetcherFor].
func (s *Storage) clusterLogFetcherFor(tid signal.TenantID) fetch.Fetcher {
	return s.clusterRecordFetcherFor(signal.Log, tid, s.lookupLogEngine)
}

// clusterTraceFetcherFor is the traces analog of [Storage.clusterLogFetcherFor].
func (s *Storage) clusterTraceFetcherFor(tid signal.TenantID) fetch.Fetcher {
	return s.clusterRecordFetcherFor(signal.Trace, tid, s.lookupTraceEngine)
}

// clusterProfileFetcherFor is the profiles analog of [Storage.clusterLogFetcherFor].
func (s *Storage) clusterProfileFetcherFor(tid signal.TenantID) fetch.Fetcher {
	return s.clusterRecordFetcherFor(signal.Profile, tid, s.lookupProfileEngine)
}

// recordOwners reports whether this node owns the tenant and the addresses of its other owners.
// shardOwners reports whether this node owns shardKey (is among its ring owners) and the addresses
// of the remote owners. The key is used verbatim (already normalized, possibly a shard key).
func (s *Storage) shardOwners(shardKey signal.TenantID) (local bool, remotes []string) {
	cn := s.cluster
	for _, o := range s.ownerLookup(shardKey) {
		addr := cn.membership.AddrOf(o.ID)

		switch {
		case addr == cn.self:
			local = true
		case addr != "":
			remotes = append(remotes, addr)
		}
	}

	return local, remotes
}

// lookupRecordEngine resolves a tenant's engine for a record signal (log/trace/profile) without
// creating one. Metrics are not a record signal, so they return (nil, false).
func (s *Storage) lookupRecordEngine(sig signal.Signal, tid signal.TenantID) (*recordengine.Engine, bool) {
	switch sig {
	case signal.Log:
		return s.lookupLogEngine(tid)
	case signal.Trace:
		return s.lookupTraceEngine(tid)
	case signal.Profile:
		return s.lookupProfileEngine(tid)
	default:
		return nil, false
	}
}

// localSeries serves a peer's series listing from the local engine, dispatched by the request's
// signal (one enumeration RPC serves metrics and logs/traces/profiles alike).
func (s *Storage) localSeries(
	ctx context.Context, sig signal.Signal, tenant string, start, end int64, matchers []fetch.Matcher,
) ([]signal.Series, error) {
	tid := s.normalizeTenant(signal.TenantID(tenant))

	if sig == signal.Metric {
		eng, ok := s.lookupEngine(tid)
		if !ok || !s.canAnswer(ctx, rpcOpSeries, sig, tid, start, end) {
			return nil, cluster.ErrShardAbsent
		}

		return eng.Series(ctx, metricSeriesRequest(tid, matchers, start, end))
	}

	eng, ok := s.lookupRecordEngine(sig, tid)
	if !ok || !s.canAnswer(ctx, rpcOpSeries, sig, tid, start, end) {
		return nil, cluster.ErrShardAbsent
	}

	return eng.Series(matchers, start, end), nil
}

// metricSeriesRequest builds the enumeration request for the metrics engine, applying the
// record-engine convention the series seam shares: a zero start AND end disables the time filter.
func metricSeriesRequest(tid signal.TenantID, matchers []fetch.Matcher, start, end int64) fetch.Request {
	if start == 0 && end == 0 {
		start, end = math.MinInt64, math.MaxInt64
	}

	return fetch.Request{
		Signal:   signal.Metric,
		Tenant:   tid,
		Start:    start,
		End:      end,
		Matchers: matchers,
	}
}

// localKeys serves a peer's distinct attribute-key listing for a record signal from the local
// engine (the enumeration twin of localSeries, backing LogKeys' cluster fan-out).
func (s *Storage) localKeys(
	ctx context.Context, sig signal.Signal, tenant string, start, end int64,
) ([]cluster.KeyInfo, error) {
	tid := s.normalizeTenant(signal.TenantID(tenant))

	eng, ok := s.lookupRecordEngine(sig, tid)
	if !ok || !s.canAnswer(ctx, rpcOpSeries, sig, tid, start, end) {
		return nil, cluster.ErrShardAbsent
	}

	raw := eng.Keys(start, end)

	out := make([]cluster.KeyInfo, len(raw))
	for i := range raw {
		out[i] = cluster.KeyInfo{Key: raw[i].Key, Scope: uint8(raw[i].Scope)}
	}

	return out, nil
}

// localValues serves a peer's distinct column-value enumeration for a record signal from the local
// engine (the enumeration twin of localKeys, backing ColumnValues' cluster fan-out).
func (s *Storage) localValues(ctx context.Context, r cluster.ValuesRequest) ([][]byte, error) {
	tid := s.normalizeTenant(signal.TenantID(r.Tenant))

	eng, ok := s.lookupRecordEngine(r.Signal, tid)
	if !ok || !s.canAnswer(ctx, rpcOpValues, r.Signal, tid, r.Start, r.End) {
		return nil, cluster.ErrShardAbsent
	}

	return eng.ColumnValues(ctx, recordengine.ValuesRequest{
		Column:  r.Column,
		AttrKey: r.AttrKey,
		Start:   r.Start,
		End:     r.End,
		Limit:   r.Limit,
	})
}

// localProfileSymbols serves a peer's profile symbol store from the local engine.
func (s *Storage) localProfileSymbols(ctx context.Context, tenant string) (map[string][]byte, error) {
	eng, ok := s.lookupProfileEngine(s.normalizeTenant(signal.TenantID(tenant)))
	if !ok {
		return nil, cluster.ErrShardAbsent
	}

	return eng.SideSnapshot(ctx)
}

// clusterSeries lists a record signal's streams for a tenant in cluster mode: locally if this node
// owns the tenant, else from an owner (hedged failover), re-applying the non-equality matchers to
// the superset. Shared by the log/trace/profile series enumeration seams.
func (s *Storage) clusterSeries(
	ctx context.Context, sig signal.Signal, tid signal.TenantID, matchers []fetch.Matcher, start, end int64,
) ([]signal.Series, error) {
	tenant := s.normalizeTenant(tid)
	n := s.cluster.shardCount()
	eq := fetch.EqualitySpecs(matchers)

	// A tenant's streams are spread across N shards, so enumerate every shard and concatenate (a
	// stream lives in exactly one shard, so the sets are disjoint — no dedup needed).
	var all []signal.Series

	for idx := range n {
		ser, err := s.shardSeries(ctx, sig, shardKeyOf(tenant, idx, n), matchers, eq, start, end)
		if err != nil {
			return nil, err
		}

		all = append(all, ser...)
	}

	return all, nil
}

// shardSeries lists one record shard's stream identities: locally if this node owns the shard, else
// hedged across its remote owners (re-applying the non-equality matchers to the owner's superset).
func (s *Storage) shardSeries(
	ctx context.Context, sig signal.Signal, shardKey signal.TenantID, matchers []fetch.Matcher,
	eq []fetch.EqualMatcher, start, end int64,
) ([]signal.Series, error) {
	local, remotes := s.shardPlacement(ctx, rpcOpSeries, sig, shardKey)
	if local {
		ser, err := s.localSeries(ctx, sig, string(shardKey), start, end, matchers)
		if !disclaimedLocally(err) {
			return ser, err
		}
	}

	return hedgeOwners(ctx, s, rpcOpSeries, remotes, func(ctx context.Context, addr string) ([]signal.Series, error) {
		series, err := cluster.FetchSeries(ctx, s.cluster.httpc, addr, sig, string(shardKey), start, end, eq, s.clusterOpts...)
		if err != nil {
			return nil, err
		}

		return fetch.FilterSeries(series, matchers), nil
	})
}

// clusterProfileSeries lists a tenant's profile streams in cluster mode (a thin wrapper over the
// signal-generic clusterSeries).
func (s *Storage) clusterProfileSeries(
	ctx context.Context, tid signal.TenantID, matchers []fetch.Matcher, start, end int64,
) ([]signal.Series, error) {
	return s.clusterSeries(ctx, signal.Profile, tid, matchers, start, end)
}

// clusterKeys lists a record signal tenant's distinct attribute keys in cluster mode: locally if
// owned, else from an owner (hedged failover). Each owner is a complete replica, so the first
// successful response is authoritative — no cross-owner merge is needed.
func (s *Storage) clusterKeys(
	ctx context.Context, sig signal.Signal, tid signal.TenantID, start, end int64,
) ([]cluster.KeyInfo, error) {
	tenant := s.normalizeTenant(tid)
	n := s.cluster.shardCount()

	// A key can appear on streams in more than one shard, so union across shards, OR-ing the scope
	// bits per distinct key.
	scopes := make(map[string]uint8)

	for idx := range n {
		ks, err := s.shardKeys(ctx, sig, shardKeyOf(tenant, idx, n), start, end)
		if err != nil {
			return nil, err
		}

		for _, k := range ks {
			scopes[string(k.Key)] |= k.Scope
		}
	}

	keys := make([]string, 0, len(scopes))
	for k := range scopes {
		keys = append(keys, k)
	}

	sort.Strings(keys) // deterministic order

	out := make([]cluster.KeyInfo, len(keys))
	for i, k := range keys {
		out[i] = cluster.KeyInfo{Key: []byte(k), Scope: scopes[k]}
	}

	return out, nil
}

// shardKeys lists one record shard's distinct attribute keys: locally if owned, else hedged across
// its remote owners (each a complete replica).
func (s *Storage) shardKeys(
	ctx context.Context, sig signal.Signal, shardKey signal.TenantID, start, end int64,
) ([]cluster.KeyInfo, error) {
	local, remotes := s.shardPlacement(ctx, rpcOpKeys, sig, shardKey)
	if local {
		keys, err := s.localKeys(ctx, sig, string(shardKey), start, end)
		if !disclaimedLocally(err) {
			return keys, err
		}
	}

	return hedgeOwners(ctx, s, rpcOpKeys, remotes, func(ctx context.Context, addr string) ([]cluster.KeyInfo, error) {
		return cluster.FetchKeys(ctx, s.cluster.httpc, addr, sig, string(shardKey), start, end, s.clusterOpts...)
	})
}

// clusterValues enumerates a record signal's distinct column values for a tenant in cluster mode:
// locally if this node owns the shard, else from an owner (hedged failover). A value can occur on
// streams in more than one shard, so the shards' results are unioned and re-truncated to the limit.
func (s *Storage) clusterValues(ctx context.Context, r cluster.ValuesRequest) ([][]byte, error) {
	tenant := s.normalizeTenant(signal.TenantID(r.Tenant))
	n := s.cluster.shardCount()

	seen := make(map[string]struct{})

	for idx := range n {
		shard := r
		shard.Tenant = string(shardKeyOf(tenant, idx, n))

		vs, err := s.shardValues(ctx, shard)
		if err != nil {
			return nil, err
		}

		for _, v := range vs {
			seen[string(v)] = struct{}{}
		}
	}

	if len(seen) == 0 {
		return nil, nil
	}

	out := make([][]byte, 0, len(seen))
	for v := range seen {
		out = append(out, []byte(v))
	}

	slices.SortFunc(out, bytes.Compare)

	if r.Limit > 0 && len(out) > r.Limit {
		out = out[:r.Limit]
	}

	return out, nil
}

// shardValues enumerates one shard's distinct column values: locally if owned, else hedged across
// its remote owners (each a complete replica).
func (s *Storage) shardValues(ctx context.Context, r cluster.ValuesRequest) ([][]byte, error) {
	shardKey := signal.TenantID(r.Tenant)

	local, remotes := s.shardPlacement(ctx, rpcOpValues, r.Signal, shardKey)
	if local {
		values, err := s.localValues(ctx, r)
		if !disclaimedLocally(err) {
			return values, err
		}
	}

	return hedgeOwners(ctx, s, rpcOpValues, remotes, func(ctx context.Context, addr string) ([][]byte, error) {
		return cluster.FetchValues(ctx, s.cluster.httpc, addr, r, s.clusterOpts...)
	})
}

// clusterProfileSymbols returns a tenant's symbol-store tables in cluster mode: locally if owned,
// else from an owner (failover). Each owner is a complete replica (symbols ride the write path).
func (s *Storage) clusterProfileSymbols(ctx context.Context, tid signal.TenantID) (map[string][]byte, error) {
	tenant := s.normalizeTenant(tid)
	n := s.cluster.shardCount()

	// A stack's symbols live in whichever shard ingested it, so collect every shard's symbol tables
	// and union them — content-addressing makes the union a plain dedup, no id remap. A flamegraph
	// over samples from several shards then resolves every stack_id.
	parts := make([]map[string][]byte, 0, n)

	for idx := range n {
		tables, err := s.shardSymbols(ctx, shardKeyOf(tenant, idx, n))
		if err != nil {
			return nil, err
		}

		if len(tables) > 0 {
			parts = append(parts, tables)
		}
	}

	return profile.NewSymbolStore().Union(parts)
}

// shardSymbols returns one profile shard's unioned symbol tables: locally if owned, else hedged
// across its remote owners (each a complete replica — symbols ride the write path).
func (s *Storage) shardSymbols(ctx context.Context, shardKey signal.TenantID) (map[string][]byte, error) {
	local, remotes := s.shardPlacement(ctx, rpcOpSide, signal.Profile, shardKey)
	if local {
		tables, err := s.localProfileSymbols(ctx, string(shardKey))
		if !disclaimedLocally(err) {
			return tables, err
		}
	}

	return hedgeOwners(ctx, s, rpcOpSide, remotes, func(ctx context.Context, addr string) (map[string][]byte, error) {
		return cluster.FetchSide(ctx, s.cluster.httpc, addr, signal.Profile, string(shardKey), s.clusterOpts...)
	})
}

// clusterRecordFetcherFor returns a record signal's read seam for one tenant in cluster mode. A
// tenant's streams are spread across N shards (each a separately-placed ring unit, like metrics), so
// a query gathers across every shard and concatenates (records are append-only, not ts-deduped).
// With a single shard this is the unsharded owner-aware fetch. lookup resolves the local engine.
func (s *Storage) clusterRecordFetcherFor(
	sig signal.Signal, tid signal.TenantID, lookup func(signal.TenantID) (*recordengine.Engine, bool),
) fetch.Fetcher {
	cn := s.cluster
	tenant := s.normalizeTenant(tid)
	n := cn.shardCount()

	shardFetchers := make([]fetch.Fetcher, 0, n)
	for idx := range n {
		sk := shardKeyOf(tenant, idx, n)
		// Stamp the shard key as the request tenant so a remote peer serves the right shard engine
		// (and the local engine ignores it).
		shardFetchers = append(shardFetchers, scopedFetcher{inner: s.shardRecordFetcher(sig, sk, lookup), scope: sk})
	}

	if n == 1 {
		return shardFetchers[0]
	}

	return concatFetcher(shardFetchers)
}

// shardRecordFetcher returns the read seam for one record shard: the local engine if this node holds
// the shard, else a hedged fan-out across the shard's other owners (each owner's copy is complete).
func (s *Storage) shardRecordFetcher(
	sig signal.Signal, shardKey signal.TenantID, lookup func(signal.TenantID) (*recordengine.Engine, bool),
) fetch.Fetcher {
	local, remotes, absent := s.shardReadTargets(sig, shardKey)
	if local {
		if e, ok := lookup(shardKey); ok {
			return s.gapGuarded(sig, shardKey, e, remotes)
		}
	}

	if len(remotes) == 0 {
		return fetch.Merge() // no owner holds the shard: it has no data anywhere
	}

	return fetch.Filter(hedgedFetcher{
		store: s, op: rpcOpRead, remotes: remotes, absentShard: absentOf(absent, sig, shardKey),
	})
}

// recordEngineFor returns the local record engine (logs, traces, or profiles) for a signal+tenant,
// creating it (with a WAL when configured) on first use.
func (s *Storage) recordEngineFor(sig signal.Signal, tenant string) (*recordengine.Engine, error) {
	switch sig {
	case signal.Trace:
		return s.traceEngineFor(signal.TenantID(tenant))
	case signal.Profile:
		return s.profileEngineFor(signal.TenantID(tenant))
	default:
		return s.logEngineFor(signal.TenantID(tenant))
	}
}

// applyReplicated is the secondary receive path: it decodes a primary's accepted write and applies
// it verbatim to the local tenant engine for the addressed signal — no OOO re-check, the primary
// already decided.
func (s *Storage) applyReplicated(_ context.Context, payload []byte) error {
	sig, tenant, walBytes, err := cluster.DecodeWrite(payload)
	if err != nil {
		return err
	}

	if sig == signal.Metric {
		eng, err := s.engineFor(signal.TenantID(tenant))
		if err != nil {
			return err
		}

		if err := eng.ApplyReplicated(walBytes); err != nil {
			return errors.Wrapf(err, "apply replicated metrics for tenant %q", tenant)
		}

		return nil
	}

	eng, err := s.recordEngineFor(sig, tenant)
	if err != nil {
		return err
	}

	if err := eng.ApplyReplicated(walBytes); err != nil {
		return errors.Wrapf(err, "apply replicated %s for tenant %q", sig, tenant)
	}

	return nil
}

// close tears down the cluster runtime: deregister (revoke lease), stop the server, close the
// etcd client.
func (n *clusterNode) close(ctx context.Context) error {
	var firstErr error

	if err := n.membership.Close(ctx); err != nil {
		firstErr = err
	}

	if err := n.server.Shutdown(ctx); err != nil && firstErr == nil {
		firstErr = err
	}

	if err := n.client.Close(); err != nil && firstErr == nil {
		firstErr = err
	}

	return firstErr
}

// writeMetricsClustered is the cluster ingest path: it projects the batch, frames each
// tenant's series+samples as a WAL-encoded payload, and routes each to its ring **primary**.
// The primary is the single authority for the shard: it OOO-checks the write, reports the
// rejected count back here, and replicates the accepted set to the secondary owners — so the
// returned [Accepted] accounting matches the single-node path and every replica converges.
func (s *Storage) writeMetricsClustered(ctx context.Context, md metric.Metrics) (Accepted, error) {
	// The ingest-rate valve is applied at the origin (per real tenant, like the single-node path):
	// each node rate-limits its own ingest. The cardinality and in-flight-memory valves are
	// head-enforced, so they are applied by the shard primary in primaryWrite. Batches arrive in
	// projection order, so the last tenant's admission state is cached across a contiguous run.
	var (
		lastTenant signal.TenantID
		lastAdmit  *tenantAdmission
		lastLimits tenantpkg.Limits
		haveTenant bool
	)

	frames := cluster.FrameMetrics(md, s.cluster.shardCount(), s.opts.Tenant,
		func(tid signal.TenantID, b *metric.Batch) bool {
			if !haveTenant || tid != lastTenant {
				lastTenant, haveTenant = tid, true
				lastAdmit = s.admissionFor(tid)
				lastLimits = s.tenant.Resolve(tid).Limits
			}

			if lastAdmit.allowRate(lastLimits, int64(b.Len())*engine.SampleBytes, s.now()) {
				return true
			}

			lastAdmit.addRate(int64(b.Len()))

			return false // whole over-budget batch shed before framing
		})

	byShard, emitted, rateRejected := frames.Shards, frames.Emitted, int64(frames.Shed)

	// Each shard routes to its own ring primary independently; fan the routes out under a bound
	// rather than paying the sum of per-primary round-trips.
	type route struct {
		key     signal.TenantID
		payload []byte
	}

	routes := make([]route, 0, len(byShard))
	for sk, payload := range byShard {
		routes = append(routes, route{sk, payload})
	}

	rejects := make([]cluster.Reject, len(routes))
	errs := make([]error, len(routes))

	parallel.ForEach(len(routes), clusterWriteFanOut, func(i int) {
		rej, err := s.routeToPrimary(ctx, signal.Metric, string(routes[i].key), routes[i].payload)
		if err != nil {
			errs[i] = err

			return
		}

		rejects[i] = rej
	})

	// Combine the origin rate rejections with each primary's per-reason breakdown.
	rej := rejectTally{rate: rateRejected}
	for _, r := range rejects {
		rej.ooo += int64(r.OOO)
		rej.cardinality += int64(r.Cardinality)
		rej.inflight += int64(r.InFlight)
	}

	keys := make([]signal.TenantID, len(routes))
	for i, r := range routes {
		keys[i] = r.key
	}

	primaryRejected := rej.ooo + rej.cardinality + rej.inflight
	failed := routeFailures(frames.Counts, keys, errs)
	s.emitRouted(ctx, signal.Metric,
		int64(emitted-frames.Shed)-primaryRejected-failed, primaryRejected, failed)

	for _, err := range errs { // surface the first error deterministically (by route index)
		if err != nil {
			return Accepted{Accepted: int64(emitted) - rej.total(), Rejected: rej.total()}, err
		}
	}

	total := rej.total()
	accepted := int64(emitted) - total
	s.emitAdmission(ctx, signal.Metric, accepted, rej, 0, 0)

	return Accepted{Accepted: accepted, Rejected: total, RejectedReason: rej.reason()}, nil
}

// clusterWriteFanOut bounds how many shard/tenant primaries a clustered write routes to at once.
// Writes are RPC-bound, so this is set above the CPU count to overlap round-trips while capping
// in-flight requests on a wide fan-out.
const clusterWriteFanOut = 16

// routeToPrimary sends a signal's tenant write (WAL-framed records) to the tenant's ring primary
// and returns the primary's per-reason rejection breakdown. The primary — local or remote — is the
// single authority for the shard, so the admission decision and the accepted set are consistent
// across all replicas. The same path serves every signal, dispatched by sig.
func (s *Storage) routeToPrimary(ctx context.Context, sig signal.Signal, tenant string, walBytes []byte) (cluster.Reject, error) {
	primary, ok := s.cluster.membership.Ring().Primary([]byte(tenant))
	if !ok {
		return cluster.Reject{}, errors.New("cluster: no primary for tenant (empty ring)")
	}

	if s.cluster.membership.AddrOf(primary.ID) == s.cluster.self {
		return s.primaryWrite(ctx, sig, tenant, walBytes)
	}

	return s.sendPrimaryWrite(ctx, s.cluster.membership.AddrOf(primary.ID), cluster.EncodeWrite(sig, tenant, walBytes))
}

// primaryWrite applies a write as the tenant's primary — the shard's single authority, so it
// makes the admission decision (OOO + the cardinality/in-flight valves from the tenant's policy)
// and replicates the accepted set to the secondary owners at write quorum (the primary is one
// durable copy, so it needs RF/2 secondary acks). It returns the per-reason rejection breakdown.
// The applying engine is selected by sig (metrics vs the record signals).
func (s *Storage) primaryWrite(ctx context.Context, sig signal.Signal, tenant string, walBytes []byte) (cluster.Reject, error) {
	// A write is only acknowledged by a node that can still prove the shard is its own; see
	// cluster_fence.go. Checked before the engine is touched, so a fenced node neither applies
	// nor materializes anything for a shard that has moved on.
	if err := s.checkPrimaryClaim(ctx, sig, tenant); err != nil {
		return cluster.Reject{}, err
	}

	// Policy is per real tenant; in sharded-metric mode tenant is a shard key ({tenant}/_s{idx}).
	limits := s.tenant.Resolve(s.normalizeTenant(tenantOfShard(signal.TenantID(tenant)))).Limits

	var (
		accepted []byte
		rej      cluster.Reject
		err      error
	)

	if sig == signal.Metric {
		var eng *engine.Engine
		if eng, err = s.engineFor(signal.TenantID(tenant)); err == nil {
			var res engine.AppendResult
			accepted, res, err = eng.ApplyPrimary(walBytes, engine.AppendLimits{
				MaxSeries: limits.MaxSeries, MaxInFlightBytes: limits.MaxInFlightBytes,
			})
			rej = cluster.Reject{OOO: res.RejectedOOO, Cardinality: res.RejectedCardinality, InFlight: res.RejectedBytes}
			s.pokeFlush(eng)
		}
	} else {
		var eng *recordengine.Engine
		if eng, err = s.recordEngineFor(sig, tenant); err == nil {
			var res recordengine.AppendResult
			accepted, res, err = eng.ApplyPrimary(walBytes, recordengine.AppendLimits{
				MaxSeries: limits.MaxSeries, MaxInFlightBytes: limits.MaxInFlightBytes,
			})
			rej = cluster.Reject{OOO: res.RejectedOOO, Cardinality: res.RejectedCardinality, InFlight: res.RejectedBytes}
			s.pokeFlush(eng)
		}
	}

	if err != nil {
		return cluster.Reject{}, errors.Wrapf(err, "primary apply for tenant %q", tenant)
	}

	rf := s.rfFor(signal.TenantID(tenant))
	owners := s.ownerLookup(signal.TenantID(tenant))

	var targets []replica.Target
	for _, o := range owners {
		if addr := s.cluster.membership.AddrOf(o.ID); addr != s.cluster.self {
			targets = append(targets, replica.Target{Addr: addr})
		}
	}

	// The primary already holds one durable copy; it needs RF/2 more from secondaries, bounded
	// by how many are actually available (availability over strict durability when nodes are down).
	needAcks := min(rf/2, len(targets))
	if err := s.cluster.replicator.ReplicateQuorum(ctx, targets, cluster.EncodeWrite(sig, tenant, accepted), needAcks); err != nil {
		return rej, errors.Wrapf(err, "replicate tenant %q", tenant)
	}

	return rej, nil
}

// primaryWriteHandler serves the primary-write endpoint: a peer routes a tenant's write here
// when this node is the ring primary. The reject count is returned in the response body.
func (s *Storage) primaryWriteHandler() http.Handler {
	return cluster.PrimaryWriteHandler(func(
		ctx context.Context, sig signal.Signal, shardKey string, walBytes []byte,
	) (cluster.Reject, error) {
		ctx = s.obs.Base(ctx)
		s.obs.Logger(ctx).Debug("primary-write received",
			zap.Stringer("signal", sig), zap.String("tenant", shardKey), zap.Int("wal_bytes", len(walBytes)))

		rej, err := s.primaryWrite(ctx, sig, shardKey, walBytes)
		if err != nil {
			s.obs.Logger(ctx).Error("primary-write failed",
				zap.Stringer("signal", sig), zap.String("tenant", shardKey), zap.Error(err))

			return cluster.Reject{}, err
		}

		return rej, nil
	})
}

// sendPrimaryWrite forwards a tenant's write to the remote primary at addr and returns the reject
// count it reports. It is bounded by the write policy: each attempt has a per-try timeout (so a
// stuck primary is abandoned), but it retries only when the request provably never reached the
// server ([retry.ConnFailure]) — a write is never re-sent after the primary may have applied it.
func (s *Storage) sendPrimaryWrite(ctx context.Context, addr string, payload []byte) (cluster.Reject, error) {
	s.obs.Logger(ctx).Debug("primary-write send", zap.String("addr", addr), zap.Int("bytes", len(payload)))

	return retry.Do(ctx, s.writePolicy(ctx, rpcOpWrite), func(ctx context.Context) (cluster.Reject, error) {
		return cluster.SendPrimaryWrite(ctx, s.cluster.httpc, addr, payload)
	})
}

// termFor returns a function reporting this node's current ownership term for tid's shard — the
// etcd revision its compaction claim was created at — which the engine stamps into every bucket
// index it writes.
//
// It is read per index write rather than captured, because ownership outlives no engine: a shard
// handed to another node and back gives the same engine a new term, and the term is what says the
// index it then writes supersedes whatever the intervening owner left.
//
// nil in single-node mode, where the engine's generation is a plain local counter — there is no
// second writer for a term to order it against.
// writerID returns the identity this node's engines keep their WAL flush watermark under in the
// bucket index — the configured cluster node id, which is stable across restarts and distinct from
// every peer sharing the prefix.
//
// Empty in single-node mode: there is no second writer, so the engine keeps the anonymous slot the
// index has always had. It is read from the *configuration* rather than from the joined cluster so
// that an engine created before (or without) a successful join still writes under this node's own
// slot instead of silently sharing the anonymous one.
func (s *Storage) writerID() string {
	if s.opts.Cluster == nil {
		return ""
	}

	return s.opts.Cluster.Self.ID
}

func (s *Storage) termFor(tid signal.TenantID) func() uint64 {
	if s.cluster == nil {
		return nil
	}

	shard := string(s.normalizeTenant(tid))

	return func() uint64 {
		term, _ := s.cluster.ownership.Term(shard)

		// A shard this node does not hold a claim on reports 0, which keeps the generation on the
		// counter alone. It is the honest answer: an engine writing without a claim is not writing
		// as any tenure of the shard, and must not be able to supersede one that is.
		return term
	}
}

// Routing outcomes reported by [Storage.emitRouted] on the coordinator side of a clustered write.
const (
	routedAccepted = "accepted"
	routedRejected = "rejected"
	routedFailed   = "failed"
)

// emitRouted records what a clustered write routed to shard primaries, split by outcome, on the
// routing node. It counts only what was actually framed and sent: the origin rate valve sheds
// before routing and is already covered by storage.ingest.rejected on this same node.
//
// It is called once per write (bulk), so it never touches the per-point hot path.
func (s *Storage) emitRouted(ctx context.Context, sig signal.Signal, accepted, rejected, failed int64) {
	name := sig.String()
	c := s.obs.Cluster
	c.Routed(ctx, accepted, name, routedAccepted)
	c.Routed(ctx, rejected, name, routedRejected)
	c.Routed(ctx, failed, name, routedFailed)
}

// routeFailures sums the points the errored routes carried, so a failed route is attributed the
// number of points it actually took rather than being counted as accepted.
func routeFailures(counts map[signal.TenantID]int, keys []signal.TenantID, errs []error) int64 {
	var failed int64

	for i, err := range errs {
		if err != nil {
			failed += int64(counts[keys[i]])
		}
	}

	return failed
}

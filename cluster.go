package storage

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/go-faster/errors"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"

	"github.com/oteldb/storage/cluster"
	"github.com/oteldb/storage/cluster/etcd"
	"github.com/oteldb/storage/cluster/partsync"
	"github.com/oteldb/storage/cluster/replica"
	"github.com/oteldb/storage/engine"
	"github.com/oteldb/storage/internal/obs"
	"github.com/oteldb/storage/internal/parallel"
	"github.com/oteldb/storage/internal/retry"
	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/recordengine"
	"github.com/oteldb/storage/reliability"
	"github.com/oteldb/storage/signal"
	"github.com/oteldb/storage/signal/metric"
	"github.com/oteldb/storage/signal/profile"
	tenantpkg "github.com/oteldb/storage/tenant"
	"github.com/oteldb/storage/wal"
)

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
}

// rfFor resolves the replication factor for one shard key: the tenant's
// [tenant.Durability] RF when set, else the cluster-wide default (cluster.Config.RF).
// Policy is per real tenant — a shard key ({tenant}/_s{idx}) collapses via tenantOfShard —
// so every shard of a tenant shares one RF. The ring clamps the result to the membership
// size at lookup time.
func (s *Storage) rfFor(shardKey signal.TenantID) int {
	if rf := s.tenant.Resolve(s.normalizeTenant(tenantOfShard(shardKey))).Durability.RF; rf > 0 {
		return rf
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

	st, err := s.cluster.psync.Sync(ctx, enginePrefix, remotes, strict)
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
func (n *clusterNode) shardCount() int {
	if n.shards < 1 {
		return 1
	}

	return n.shards
}

// shardSep separates a tenant from its shard index in a shard key. It is chosen so a shard key is
// a valid backend path segment and never collides with a real tenant id (which the embedder keeps
// free of this marker).
const shardSep = "/_s"

// shardKeyOf returns the routing/storage key for tenant's shard idx. With a single shard it is the
// bare (already-normalized) tenant, so ring placement and on-disk prefixes are byte-identical to
// the unsharded path; with N>1 it suffixes the shard index.
func shardKeyOf(tenant signal.TenantID, idx, n int) signal.TenantID {
	if n <= 1 {
		return tenant
	}

	return tenant + signal.TenantID(shardSep+strconv.Itoa(idx))
}

// tenantOfShard recovers the tenant id from a shard key (the inverse of [shardKeyOf]), for policy
// resolution. A key without the shard marker (the single-shard case) is returned unchanged.
func tenantOfShard(shardKey signal.TenantID) signal.TenantID {
	if i := strings.LastIndex(string(shardKey), shardSep); i >= 0 {
		return shardKey[:i]
	}

	return shardKey
}

// shardOf maps a series id to a shard index in [0, n). The series id is already a uniform content
// hash, so the low word modulo n distributes evenly.
func shardOf(id signal.SeriesID, n int) int {
	if n <= 1 {
		return 0
	}

	return int(id.Lo % uint64(n))
}

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

	// The replicator applies an inbound (or local) write to the addressed tenant's engine. Its
	// transport shares the tuned client (connection timeouts) so replication tolerates a slow peer.
	rp := replica.New(cfg.Self.Addr, replica.NewHTTPTransport(httpc), s.applyReplicated)

	var lc net.ListenConfig

	ln, err := lc.Listen(ctx, "tcp", cfg.Self.Addr)
	if err != nil {
		_ = client.Close()

		return errors.Wrapf(err, "listen on %q", cfg.Self.Addr)
	}

	mux := http.NewServeMux()
	mux.Handle(replica.ReplicatePath, rp.Handler())       // secondary: trusting apply
	mux.Handle(primaryWritePath, s.primaryWriteHandler()) // primary: OOO apply + replicate
	// read fan-out across metric/log/trace/profile signals.
	mux.Handle(cluster.ReadPath, cluster.ReadHandler(s.localFetch, s.localLogFetch, s.localTraceFetch, s.localProfileFetch))
	mux.Handle(cluster.AggregatePath, cluster.AggregateHandler(s.localAggregate)) // metric aggregate pushdown
	mux.Handle(cluster.SeriesPath, cluster.SeriesHandler(s.localSeries))          // record-signal series enumeration (log/trace/profile)
	mux.Handle(cluster.KeysPath, cluster.KeysHandler(s.localKeys))                // record-signal attribute-key enumeration
	mux.Handle(cluster.SidePath, cluster.SideHandler(s.localProfileSymbols))      // profile symbol store
	// Part mirroring for per-node private backends: peers list and fetch this node's backend
	// objects. Mounted unconditionally (read-only; useful for operator inspection), used by the
	// maintenance loop only when Config.PrivateBackend is set.
	mux.Handle(partsync.ListPath, partsync.ListHandler(s.backend))
	mux.Handle(partsync.ObjectPath, partsync.ObjectHandler(s.backend))
	mux.Handle(partsync.NotifyPath, s.partsNotifyHandler())
	srv := &http.Server{Handler: mux, ReadHeaderTimeout: 5 * time.Second}

	go func() { _ = srv.Serve(ln) }()

	mship, err := etcd.Join(ctx, client, root, cfg.Self, 0)
	if err != nil {
		_ = srv.Close()
		_ = client.Close()

		return errors.Wrap(err, "join cluster")
	}

	mship.SetLogger(s.obs.Log)
	s.obs.Logger(ctx).Info("joined cluster",
		zap.String("id", cfg.Self.ID), zap.String("zone", cfg.Self.Zone), zap.String("addr", cfg.Self.Addr))

	s.cluster = &clusterNode{
		client:     client,
		membership: mship,
		ownership:  etcd.NewOwnership(client, root, cfg.Self.ID, mship.LeaseID()),
		replicator: rp,
		httpc:      httpc,
		server:     srv,
		listener:   ln,
		self:       cfg.Self.Addr,
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
	eng, ok := s.lookupEngine(s.normalizeTenant(signal.TenantID(tenant)))
	if !ok {
		return nil, nil
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

	return fetch.Merge(shardFetchers...)
}

// shardFetcher returns the read seam for one metric shard: the local engine if this node is an
// owner (full matcher pushdown), else a fail-over across the shard's remote owners (each owner's
// copy is complete; matchers are re-applied to the returned superset).
func (s *Storage) shardFetcher(shardKey signal.TenantID) fetch.Fetcher {
	cn := s.cluster
	owners := cn.membership.Ring().Lookup([]byte(shardKey), s.rfFor(shardKey))

	var remotes []fetch.Fetcher
	for _, o := range owners {
		addr := cn.membership.AddrOf(o.ID)
		if addr == cn.self { // this node is an owner: serve locally
			if e, ok := s.lookupEngine(shardKey); ok {
				return e
			}

			return fetch.Merge() // owner but no data yet
		}

		if addr != "" {
			remotes = append(remotes, cluster.NewRemoteFetcher(signal.Metric, addr, cn.httpc))
		}
	}

	return &filteringFetcher{inner: hedgedFetcher{store: s, op: rpcOpRead, remotes: remotes}}
}

// localAggregate serves a peer's metric aggregate from the local shard engine, pushing down the
// (equality) matchers it forwarded — the receiving side of [cluster.AggregateHandler].
func (s *Storage) localAggregate(
	ctx context.Context, tenant string, start, end, step int64, matchers []fetch.Matcher,
) ([]engine.NamedAgg, error) {
	eng, ok := s.lookupEngine(s.normalizeTenant(signal.TenantID(tenant)))
	if !ok {
		return nil, nil
	}

	return eng.AggregateStepNamed(ctx, fetch.Request{
		Tenant: signal.TenantID(tenant), Start: start, End: end, Matchers: matchers,
	}, step)
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

// clusterAggregateNamedFor is the labeled variant of [clusterAggregateFor]: it computes a tenant's
// step-bucketed aggregate across all its shards but keeps each series' identity (so the coordinator
// can render labels), re-checking the full matcher set against each shard's returned identities and
// merging buckets for any series that surfaces from more than one shard. It backs the labeled
// [Storage.AggregateMetricsNamed] pushdown path in cluster mode.
func (s *Storage) clusterAggregateNamedFor(
	ctx context.Context, tid signal.TenantID, r fetch.Request, step int64,
) ([]engine.NamedAgg, error) {
	cn := s.cluster
	tenant := s.normalizeTenant(tid)
	n := cn.shardCount()

	var out []engine.NamedAgg
	index := make(map[signal.SeriesID]int, n) // id → position in out, to merge a series seen twice

	for idx := range n {
		sk := shardKeyOf(tenant, idx, n)

		named, err := s.shardAggregate(ctx, sk, r, step)
		if err != nil {
			return nil, err
		}

		for i := range named {
			na := &named[i]
			if !matchesAllSeries(na.Series, r.Matchers) {
				continue
			}

			id := na.Series.Hash()
			if j, ok := index[id]; ok {
				out[j].Buckets = mergeBucketLists(out[j].Buckets, na.Buckets)
			} else {
				index[id] = len(out)
				out = append(out, engine.NamedAgg{Series: na.Series, Buckets: na.Buckets})
			}
		}
	}

	return out, nil
}

// shardAggregate gets one metric shard's per-series aggregates: locally (full matcher pushdown) if
// this node owns it, else from a remote owner with sequential failover (equality matchers pushed;
// the coordinator re-checks the full set on the returned identities).
func (s *Storage) shardAggregate(
	ctx context.Context, shardKey signal.TenantID, r fetch.Request, step int64,
) ([]engine.NamedAgg, error) {
	cn := s.cluster
	owners := cn.membership.Ring().Lookup([]byte(shardKey), s.rfFor(shardKey))

	for _, o := range owners {
		if cn.membership.AddrOf(o.ID) == cn.self { // owner: serve locally
			eng, ok := s.lookupEngine(shardKey)
			if !ok {
				return nil, nil // owner, no data yet
			}

			return eng.AggregateStepNamed(ctx, fetch.Request{
				Tenant: shardKey, Start: r.Start, End: r.End, Matchers: r.Matchers,
			}, step)
		}
	}

	eq := equalityMatchers(r.Matchers)

	var lastErr error
	for _, o := range owners {
		addr := cn.membership.AddrOf(o.ID)
		if addr == "" || addr == cn.self {
			continue
		}

		named, err := cluster.NewRemoteAggregator(addr, cn.httpc).Aggregate(ctx, string(shardKey), r.Start, r.End, step, eq)
		if err == nil {
			return named, nil
		}

		lastErr = err
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
		if !matchesAllSeries(na.Series, matchers) {
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

// localLogFetch serves a peer's log fetch from the local log engine, pushing down the (equality)
// stream matchers it forwarded.
func (s *Storage) localLogFetch(ctx context.Context, tenant string, start, end int64, matchers []fetch.Matcher) ([]*fetch.Batch, error) {
	eng, ok := s.lookupLogEngine(s.normalizeTenant(signal.TenantID(tenant)))
	if !ok {
		return nil, nil
	}

	it, err := eng.Fetch(ctx, fetch.Request{Signal: signal.Log, Tenant: signal.TenantID(tenant), Start: start, End: end, Matchers: matchers})
	if err != nil {
		return nil, err
	}

	return fetch.Drain(ctx, it)
}

// clusterLogFetcherFor returns the log read seam for one tenant in cluster mode: local if this
// node owns the tenant, otherwise fanned out to an owner (a window+matcher superset re-filtered
// here), failing over between owners — the logs analog of [Storage.clusterFetcherFor].
func (s *Storage) clusterLogFetcherFor(tid signal.TenantID) fetch.Fetcher {
	return s.clusterRecordFetcherFor(signal.Log, tid, s.lookupLogEngine)
}

// localTraceFetch serves a peer's span fetch from the local traces engine.
func (s *Storage) localTraceFetch(ctx context.Context, tenant string, start, end int64, matchers []fetch.Matcher) ([]*fetch.Batch, error) {
	eng, ok := s.lookupTraceEngine(s.normalizeTenant(signal.TenantID(tenant)))
	if !ok {
		return nil, nil
	}

	it, err := eng.Fetch(ctx, fetch.Request{Signal: signal.Trace, Tenant: signal.TenantID(tenant), Start: start, End: end, Matchers: matchers})
	if err != nil {
		return nil, err
	}

	return fetch.Drain(ctx, it)
}

// clusterTraceFetcherFor is the traces analog of [Storage.clusterLogFetcherFor].
func (s *Storage) clusterTraceFetcherFor(tid signal.TenantID) fetch.Fetcher {
	return s.clusterRecordFetcherFor(signal.Trace, tid, s.lookupTraceEngine)
}

// localProfileFetch serves a peer's sample fetch from the local profiles engine.
func (s *Storage) localProfileFetch(
	ctx context.Context, tenant string, start, end int64, matchers []fetch.Matcher,
) ([]*fetch.Batch, error) {
	eng, ok := s.lookupProfileEngine(s.normalizeTenant(signal.TenantID(tenant)))
	if !ok {
		return nil, nil
	}

	it, err := eng.Fetch(ctx, fetch.Request{
		Signal: signal.Profile, Tenant: signal.TenantID(tenant), Start: start, End: end, Matchers: matchers,
	})
	if err != nil {
		return nil, err
	}

	return fetch.Drain(ctx, it)
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
	for _, o := range cn.membership.Ring().Lookup([]byte(shardKey), s.rfFor(shardKey)) {
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

// equalitySpecs extracts the serializable (equality) matchers to push down to a peer.
func equalitySpecs(matchers []fetch.Matcher) []fetch.EqualMatcher {
	var eq []fetch.EqualMatcher
	for i := range matchers {
		if matchers[i].Spec != nil {
			eq = append(eq, *matchers[i].Spec)
		}
	}

	return eq
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

// localSeries serves a peer's series listing for any record signal from the local engine,
// dispatched by the request's signal (one enumeration RPC serves logs/traces/profiles).
func (s *Storage) localSeries(
	_ context.Context, sig signal.Signal, tenant string, start, end int64, matchers []fetch.Matcher,
) ([]signal.Series, error) {
	eng, ok := s.lookupRecordEngine(sig, s.normalizeTenant(signal.TenantID(tenant)))
	if !ok {
		return nil, nil
	}

	return eng.Series(matchers, start, end), nil
}

// localKeys serves a peer's distinct attribute-key listing for a record signal from the local
// engine (the enumeration twin of localSeries, backing LogKeys' cluster fan-out).
func (s *Storage) localKeys(
	_ context.Context, sig signal.Signal, tenant string, start, end int64,
) ([]cluster.KeyInfo, error) {
	eng, ok := s.lookupRecordEngine(sig, s.normalizeTenant(signal.TenantID(tenant)))
	if !ok {
		return nil, nil
	}

	raw := eng.Keys(start, end)

	out := make([]cluster.KeyInfo, len(raw))
	for i := range raw {
		out[i] = cluster.KeyInfo{Key: raw[i].Key, Scope: uint8(raw[i].Scope)}
	}

	return out, nil
}

// localProfileSymbols serves a peer's profile symbol store from the local engine.
func (s *Storage) localProfileSymbols(ctx context.Context, tenant string) (map[string][]byte, error) {
	eng, ok := s.lookupProfileEngine(s.normalizeTenant(signal.TenantID(tenant)))
	if !ok {
		return map[string][]byte{}, nil
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
	eq := equalitySpecs(matchers)

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
	local, remotes := s.shardOwners(shardKey)
	if local {
		return s.localSeries(ctx, sig, string(shardKey), start, end, matchers)
	}

	if len(remotes) == 0 {
		return nil, nil
	}

	thunks := make([]func(context.Context) ([]signal.Series, error), len(remotes))
	for i := range remotes {
		addr := remotes[i]
		thunks[i] = func(ctx context.Context) ([]signal.Series, error) {
			series, err := cluster.FetchSeries(ctx, s.cluster.httpc, addr, sig, string(shardKey), start, end, eq)
			if err != nil {
				return nil, err
			}

			kept := series[:0]
			for i := range series {
				if matchesAllSeries(series[i], matchers) {
					kept = append(kept, series[i])
				}
			}

			return kept, nil
		}
	}

	return retry.Hedge(ctx, s.readPolicy(ctx, rpcOpSeries), thunks)
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
	local, remotes := s.shardOwners(shardKey)
	if local {
		return s.localKeys(ctx, sig, string(shardKey), start, end)
	}

	if len(remotes) == 0 {
		return nil, nil
	}

	thunks := make([]func(context.Context) ([]cluster.KeyInfo, error), len(remotes))
	for i := range remotes {
		addr := remotes[i]
		thunks[i] = func(ctx context.Context) ([]cluster.KeyInfo, error) {
			return cluster.FetchKeys(ctx, s.cluster.httpc, addr, sig, string(shardKey), start, end)
		}
	}

	return retry.Hedge(ctx, s.readPolicy(ctx, rpcOpKeys), thunks)
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
	local, remotes := s.shardOwners(shardKey)
	if local {
		return s.localProfileSymbols(ctx, string(shardKey))
	}

	if len(remotes) == 0 {
		return map[string][]byte{}, nil
	}

	thunks := make([]func(context.Context) (map[string][]byte, error), len(remotes))
	for i := range remotes {
		addr := remotes[i]
		thunks[i] = func(ctx context.Context) (map[string][]byte, error) {
			return cluster.FetchSide(ctx, s.cluster.httpc, addr, signal.Profile, string(shardKey))
		}
	}

	return retry.Hedge(ctx, s.readPolicy(ctx, rpcOpSide), thunks)
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

// shardRecordFetcher returns the read seam for one record shard: the local engine if this node is an
// owner, else a hedged fan-out across the shard's remote owners (each owner's copy is complete).
func (s *Storage) shardRecordFetcher(
	sig signal.Signal, shardKey signal.TenantID, lookup func(signal.TenantID) (*recordengine.Engine, bool),
) fetch.Fetcher {
	cn := s.cluster
	owners := cn.membership.Ring().Lookup([]byte(shardKey), s.rfFor(shardKey))

	var remotes []fetch.Fetcher
	for _, o := range owners {
		addr := cn.membership.AddrOf(o.ID)
		if addr == cn.self { // owner: serve locally
			if e, ok := lookup(shardKey); ok {
				return e
			}

			return fetch.Merge() // owner but no data yet
		}

		if addr != "" {
			remotes = append(remotes, cluster.NewRemoteFetcher(sig, addr, cn.httpc))
		}
	}

	return &filteringFetcher{inner: hedgedFetcher{store: s, op: rpcOpRead, remotes: remotes}}
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

// filteringFetcher re-applies a request's matchers to the inner result, which may be a superset
// (a remote owner returns its whole window since matchers are not serializable). A series is
// kept iff every matcher matches a present attribute of that name — the same positive semantics
// the engine's postings resolution applies; absent/negated handling stays in the language layer.
type filteringFetcher struct {
	inner fetch.Fetcher
}

func (f *filteringFetcher) Fetch(ctx context.Context, r fetch.Request) (fetch.Iterator, error) {
	it, err := f.inner.Fetch(ctx, r)
	if err != nil {
		return nil, err
	}

	batches, err := fetch.Drain(ctx, it)
	if err != nil {
		return nil, err
	}

	if len(r.Matchers) == 0 {
		return fetch.NewSliceIterator(batches), nil
	}

	kept := batches[:0]
	for _, b := range batches {
		if matchesAllSeries(b.Series, r.Matchers) {
			kept = append(kept, b)
		}
	}

	return fetch.NewSliceIterator(kept), nil
}

// matchesAllSeries reports whether s satisfies every matcher (each over the present value of
// its named label).
func matchesAllSeries(s signal.Series, matchers []fetch.Matcher) bool {
	for i := range matchers {
		v, ok := lookupSeriesLabel(s, matchers[i].Name)
		if !ok || !matchers[i].Match(v) {
			return false
		}
	}

	return true
}

// lookupSeriesLabel resolves a label value from a series the way the engine indexes it for matching
// (recordengine's indexLabels / metric's series labels): the series' own attributes, then the
// resource and scope attributes, then the reserved scope name/version labels. This is what lets a
// fan-out matcher on a resource label (e.g. service.name) re-filter correctly for the record signals.
func lookupSeriesLabel(s signal.Series, name []byte) (signal.Value, bool) {
	if v, ok := s.Attributes.Get(name); ok {
		return v, true
	}

	if v, ok := s.Resource.Attributes.Get(name); ok {
		return v, true
	}

	if v, ok := s.Scope.Attributes.Get(name); ok {
		return v, true
	}

	switch string(name) {
	case "otel.scope.name":
		if len(s.Scope.Name) > 0 {
			return signal.StringValue(s.Scope.Name), true
		}
	case "otel.scope.version":
		if len(s.Scope.Version) > 0 {
			return signal.StringValue(s.Scope.Version), true
		}
	}

	return signal.Value{}, false
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
	type shardWAL struct {
		buf  bytes.Buffer
		w    *wal.Writer
		seen map[signal.SeriesID]struct{}
	}

	// Group each point by its shard key — the (tenant, hash(seriesID) % N) routing/storage unit —
	// so a tenant's series spread across the ring instead of pinning to one owner set. With a
	// single shard the key is the tenant, identical to the unsharded path.
	n := s.cluster.shardCount()
	byShard := make(map[signal.TenantID]*shardWAL)

	// The ingest-rate valve is applied at the origin (per real tenant, like the single-node path):
	// each node rate-limits its own ingest. The cardinality and in-flight-memory valves are
	// head-enforced, so they are applied by the shard primary in primaryWrite. Cache the last
	// tenant's admission state across a tenant-contiguous run of batches.
	var (
		rateRejected int64
		lastTenant   signal.TenantID
		lastAdmit    *tenantAdmission
		lastLimits   tenantpkg.Limits
		haveTenant   bool
	)

	emitted := metric.Project(md, func(b *metric.Batch) {
		tid := s.normalizeTenant(s.tenantFor(b.Resource(), b.Scope()))
		if !haveTenant || tid != lastTenant {
			lastTenant, haveTenant = tid, true
			lastAdmit = s.admissionFor(tid)
			lastLimits = s.tenant.Resolve(tid).Limits
		}

		if !lastAdmit.allowRate(lastLimits, int64(b.Len())*engine.SampleBytes, s.now()) {
			rateRejected += int64(b.Len())
			lastAdmit.addRate(int64(b.Len()))

			return // whole over-budget batch shed before framing
		}

		for i := range b.Len() {
			id := b.IDs[i]
			sk := shardKeyOf(tid, shardOf(id, n), n)

			sw := byShard[sk]
			if sw == nil {
				sw = &shardWAL{seen: make(map[signal.SeriesID]struct{})}
				sw.w = wal.NewWriter(&sw.buf)
				byShard[sk] = sw
			}

			if _, ok := sw.seen[id]; !ok { // register each series once per shard
				sw.seen[id] = struct{}{}
				_ = sw.w.WriteSeries(id, b.Series(i))
			}

			_ = sw.w.WriteSamples(id, b.Ts[i:i+1], b.Values[i:i+1])
		}
	})

	// Each shard routes to its own ring primary independently; fan the routes out under a bound
	// rather than paying the sum of per-primary round-trips.
	type route struct {
		key     signal.TenantID
		payload []byte
	}

	routes := make([]route, 0, len(byShard))
	for sk, sw := range byShard {
		routes = append(routes, route{sk, sw.buf.Bytes()})
	}

	rejects := make([]primaryReject, len(routes))
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
		rej.ooo += int64(r.ooo)
		rej.cardinality += int64(r.cardinality)
		rej.inflight += int64(r.inflight)
	}

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

const primaryWritePath = "/internal/primary-write"

// clusterWriteFanOut bounds how many shard/tenant primaries a clustered write routes to at once.
// Writes are RPC-bound, so this is set above the CPU count to overlap round-trips while capping
// in-flight requests on a wide fan-out.
const clusterWriteFanOut = 16

// routeToPrimary sends a signal's tenant write (WAL-framed records) to the tenant's ring primary
// and returns the primary's per-reason rejection breakdown. The primary — local or remote — is the
// single authority for the shard, so the admission decision and the accepted set are consistent
// across all replicas. The same path serves every signal, dispatched by sig.
func (s *Storage) routeToPrimary(ctx context.Context, sig signal.Signal, tenant string, walBytes []byte) (primaryReject, error) {
	primary, ok := s.cluster.membership.Ring().Primary([]byte(tenant))
	if !ok {
		return primaryReject{}, errors.New("cluster: no primary for tenant (empty ring)")
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
func (s *Storage) primaryWrite(ctx context.Context, sig signal.Signal, tenant string, walBytes []byte) (primaryReject, error) {
	// Policy is per real tenant; in sharded-metric mode tenant is a shard key ({tenant}/_s{idx}).
	limits := s.tenant.Resolve(s.normalizeTenant(tenantOfShard(signal.TenantID(tenant)))).Limits

	var (
		accepted []byte
		rej      primaryReject
		err      error
	)

	if sig == signal.Metric {
		var eng *engine.Engine
		if eng, err = s.engineFor(signal.TenantID(tenant)); err == nil {
			var res engine.AppendResult
			accepted, res, err = eng.ApplyPrimary(walBytes, engine.AppendLimits{
				MaxSeries: limits.MaxSeries, MaxInFlightBytes: limits.MaxInFlightBytes,
			})
			rej = primaryReject{ooo: res.RejectedOOO, cardinality: res.RejectedCardinality, inflight: res.RejectedBytes}
		}
	} else {
		var eng *recordengine.Engine
		if eng, err = s.recordEngineFor(sig, tenant); err == nil {
			var res recordengine.AppendResult
			accepted, res, err = eng.ApplyPrimary(walBytes, recordengine.AppendLimits{
				MaxSeries: limits.MaxSeries, MaxInFlightBytes: limits.MaxInFlightBytes,
			})
			rej = primaryReject{ooo: res.RejectedOOO, cardinality: res.RejectedCardinality, inflight: res.RejectedBytes}
		}
	}

	if err != nil {
		return primaryReject{}, errors.Wrapf(err, "primary apply for tenant %q", tenant)
	}

	rf := s.rfFor(signal.TenantID(tenant))
	owners := s.cluster.membership.Ring().Lookup([]byte(tenant), rf)

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

// primaryReject is the per-reason rejection breakdown the shard primary reports back to the origin
// (over the primary-write RPC) so clustered ingest attributes OTLP partial-success exactly like the
// single-node path. The rate valve is applied at the origin, so it is not carried here.
type primaryReject struct{ ooo, cardinality, inflight int }

// primaryWriteHandler serves the primary-write endpoint: a peer routes a tenant's write here
// when this node is the ring primary. The reject count is returned in the response body.
func (s *Storage) primaryWriteHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if req.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)

			return
		}

		payload, err := io.ReadAll(req.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)

			return
		}

		sig, tenant, walBytes, err := cluster.DecodeWrite(payload)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)

			return
		}

		ctx := s.obs.Base(obs.ExtractHTTP(req.Context(), req.Header)) // join the caller's trace
		s.obs.Logger(ctx).Debug("primary-write received",
			zap.Stringer("signal", sig), zap.String("tenant", tenant), zap.Int("wal_bytes", len(walBytes)))

		rej, err := s.primaryWrite(ctx, sig, tenant, walBytes)
		if err != nil {
			s.obs.Logger(ctx).Error("primary-write failed", zap.Stringer("signal", sig), zap.String("tenant", tenant), zap.Error(err))
			http.Error(w, err.Error(), http.StatusInternalServerError)

			return
		}

		// Response body is the per-reason reject breakdown: "ooo cardinality inflight".
		_, _ = fmt.Fprintf(w, "%d %d %d", rej.ooo, rej.cardinality, rej.inflight)
	})
}

// sendPrimaryWrite forwards a tenant's write to the remote primary at addr and returns the reject
// count it reports. It is bounded by the write policy: each attempt has a per-try timeout (so a
// stuck primary is abandoned), but it retries only when the request provably never reached the
// server ([retry.ConnFailure]) — a write is never re-sent after the primary may have applied it.
func (s *Storage) sendPrimaryWrite(ctx context.Context, addr string, payload []byte) (primaryReject, error) {
	s.obs.Logger(ctx).Debug("primary-write send", zap.String("addr", addr), zap.Int("bytes", len(payload)))

	return retry.Do(ctx, s.writePolicy(ctx, rpcOpWrite), func(ctx context.Context) (primaryReject, error) {
		u := (&url.URL{Scheme: "http", Host: addr}).JoinPath(primaryWritePath)
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, u.String(), bytes.NewReader(payload))
		if err != nil {
			return primaryReject{}, errors.Wrap(err, "build primary-write request")
		}

		obs.InjectHTTP(ctx, req.Header) // carry the trace into the primary-write RPC

		resp, err := s.cluster.httpc.Do(req)
		if err != nil {
			return primaryReject{}, errors.Wrapf(err, "primary-write to %q", addr)
		}
		defer func() { _ = resp.Body.Close() }()

		body, err := io.ReadAll(resp.Body)
		if err != nil {
			return primaryReject{}, errors.Wrap(err, "read primary-write response")
		}

		if resp.StatusCode != http.StatusOK {
			return primaryReject{}, errors.Errorf("cluster: primary %q returned %d: %s", addr, resp.StatusCode, bytes.TrimSpace(body))
		}

		var rej primaryReject
		if _, err := fmt.Sscanf(string(bytes.TrimSpace(body)), "%d %d %d", &rej.ooo, &rej.cardinality, &rej.inflight); err != nil {
			return primaryReject{}, errors.Wrap(err, "parse reject breakdown")
		}

		return rej, nil
	})
}

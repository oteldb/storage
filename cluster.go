package storage

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/go-faster/errors"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"

	"github.com/oteldb/storage/cluster"
	"github.com/oteldb/storage/cluster/etcd"
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
	mux.Handle(cluster.SeriesPath, cluster.SeriesHandler(s.localProfileSeries))   // profile series enumeration
	mux.Handle(cluster.SidePath, cluster.SideHandler(s.localProfileSymbols))      // profile symbol store
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
	}

	return nil
}

// localFetch serves a peer's fetch from the local engine, pushing down the (equality) matchers
// it forwarded — the read-fan-out server's view of this node's data.
func (s *Storage) localFetch(ctx context.Context, tenant string, start, end int64, matchers []fetch.Matcher) ([]*fetch.Batch, error) {
	eng, ok := s.lookupEngine(s.normalizeTenant(signal.TenantID(tenant)))
	if !ok {
		return nil, nil
	}

	it, err := eng.Fetch(ctx, fetch.Request{Tenant: signal.TenantID(tenant), Start: start, End: end, Matchers: matchers})
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
	owners := cn.membership.Ring().Lookup([]byte(shardKey), cn.rf)

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
	owners := cn.membership.Ring().Lookup([]byte(shardKey), cn.rf)

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
func (s *Storage) recordOwners(tid signal.TenantID) (local bool, remotes []string) {
	cn := s.cluster
	for _, o := range cn.membership.Ring().Lookup([]byte(s.normalizeTenant(tid)), cn.rf) {
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

// localProfileSeries serves a peer's profile series listing from the local engine.
func (s *Storage) localProfileSeries(
	_ context.Context, tenant string, start, end int64, matchers []fetch.Matcher,
) ([]signal.Series, error) {
	eng, ok := s.lookupProfileEngine(s.normalizeTenant(signal.TenantID(tenant)))
	if !ok {
		return nil, nil
	}

	return eng.Series(matchers, start, end), nil
}

// localProfileSymbols serves a peer's profile symbol store from the local engine.
func (s *Storage) localProfileSymbols(ctx context.Context, tenant string) (map[string][]byte, error) {
	eng, ok := s.lookupProfileEngine(s.normalizeTenant(signal.TenantID(tenant)))
	if !ok {
		return map[string][]byte{}, nil
	}

	return eng.SideSnapshot(ctx)
}

// clusterProfileSeries lists a tenant's profile streams in cluster mode: locally if this node owns
// the tenant, else from an owner (failover), re-applying the non-equality matchers to the superset.
func (s *Storage) clusterProfileSeries(
	ctx context.Context, tid signal.TenantID, matchers []fetch.Matcher, start, end int64,
) ([]signal.Series, error) {
	local, remotes := s.recordOwners(tid)
	if local {
		return s.localProfileSeries(ctx, string(tid), start, end, matchers)
	}

	if len(remotes) == 0 {
		return nil, nil
	}

	eq := equalitySpecs(matchers)

	// Hedge the enumeration across the owners (each is a complete replica): a slow/down owner is
	// raced or failed over, and the requester re-applies the non-equality matchers to the superset.
	thunks := make([]func(context.Context) ([]signal.Series, error), len(remotes))
	for i := range remotes {
		addr := remotes[i]
		thunks[i] = func(ctx context.Context) ([]signal.Series, error) {
			series, err := cluster.FetchSeries(ctx, s.cluster.httpc, addr, signal.Profile,
				string(s.normalizeTenant(tid)), start, end, eq)
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

// clusterProfileSymbols returns a tenant's symbol-store tables in cluster mode: locally if owned,
// else from an owner (failover). Each owner is a complete replica (symbols ride the write path).
func (s *Storage) clusterProfileSymbols(ctx context.Context, tid signal.TenantID) (map[string][]byte, error) {
	local, remotes := s.recordOwners(tid)
	if local {
		return s.localProfileSymbols(ctx, string(tid))
	}

	if len(remotes) == 0 {
		return map[string][]byte{}, nil
	}

	// Hedge across owners (each carries the full symbol store; it rides the write path).
	thunks := make([]func(context.Context) (map[string][]byte, error), len(remotes))
	for i := range remotes {
		addr := remotes[i]
		thunks[i] = func(ctx context.Context) (map[string][]byte, error) {
			return cluster.FetchSide(ctx, s.cluster.httpc, addr, signal.Profile, string(s.normalizeTenant(tid)))
		}
	}

	return retry.Hedge(ctx, s.readPolicy(ctx, rpcOpSide), thunks)
}

// clusterRecordFetcherFor returns a record signal's read seam for one tenant in cluster mode:
// local if this node owns the tenant (the head is replicated here), otherwise fanned out to an
// owner over HTTP (a window+matcher superset the requester re-filters), failing over between
// owners. lookup resolves the local engine for the signal.
func (s *Storage) clusterRecordFetcherFor(
	sig signal.Signal, tid signal.TenantID, lookup func(signal.TenantID) (*recordengine.Engine, bool),
) fetch.Fetcher {
	cn := s.cluster
	owners := cn.membership.Ring().Lookup([]byte(s.normalizeTenant(tid)), cn.rf)

	var remotes []fetch.Fetcher
	for _, o := range owners {
		addr := cn.membership.AddrOf(o.ID)
		if addr == cn.self { // owner: serve locally
			if e, ok := lookup(s.normalizeTenant(tid)); ok {
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

	emitted := metric.Project(md, func(b *metric.Batch) {
		tenant := s.normalizeTenant(s.tenantFor(b.Resource(), b.Scope()))

		for i := range b.Len() {
			id := b.IDs[i]
			sk := shardKeyOf(tenant, shardOf(id, n), n)

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

	var (
		rejected atomic.Int64
		errs     = make([]error, len(routes))
	)

	parallel.ForEach(len(routes), clusterWriteFanOut, func(i int) {
		rej, err := s.routeToPrimary(ctx, signal.Metric, string(routes[i].key), routes[i].payload)
		if err != nil {
			errs[i] = err

			return
		}

		rejected.Add(int64(rej))
	})

	for _, err := range errs { // surface the first error deterministically (by route index)
		if err != nil {
			return Accepted{Accepted: int64(emitted) - rejected.Load(), Rejected: rejected.Load()}, err
		}
	}

	// The primary's rejections are out-of-order drops (admission is applied at the origin today).
	rej := rejected.Load()
	accepted := int64(emitted) - rej
	s.emitAdmission(ctx, signal.Metric, accepted, rejectTally{ooo: rej}, 0)

	return Accepted{Accepted: accepted, Rejected: rej}, nil
}

const primaryWritePath = "/internal/primary-write"

// clusterWriteFanOut bounds how many shard/tenant primaries a clustered write routes to at once.
// Writes are RPC-bound, so this is set above the CPU count to overlap round-trips while capping
// in-flight requests on a wide fan-out.
const clusterWriteFanOut = 16

// routeToPrimary sends a signal's tenant write (WAL-framed records) to the tenant's ring primary
// and returns how many records the primary rejected as out-of-order. The primary — local or
// remote — is the single authority for the shard, so the OOO decision and the accepted set are
// consistent across all replicas. The same path serves metrics and logs, dispatched by sig.
func (s *Storage) routeToPrimary(ctx context.Context, sig signal.Signal, tenant string, walBytes []byte) (rejected int, err error) {
	primary, ok := s.cluster.membership.Ring().Primary([]byte(tenant))
	if !ok {
		return 0, errors.New("cluster: no primary for tenant (empty ring)")
	}

	if s.cluster.membership.AddrOf(primary.ID) == s.cluster.self {
		return s.primaryWrite(ctx, sig, tenant, walBytes)
	}

	return s.sendPrimaryWrite(ctx, s.cluster.membership.AddrOf(primary.ID), cluster.EncodeWrite(sig, tenant, walBytes))
}

// primaryWrite applies a write as the tenant's primary (OOO-checked, the authoritative decision)
// and replicates the accepted set to the secondary owners at write quorum (the primary is one
// durable copy, so it needs RF/2 secondary acks). It returns the rejected count. The applying
// engine is selected by sig (metrics vs logs).
func (s *Storage) primaryWrite(ctx context.Context, sig signal.Signal, tenant string, walBytes []byte) (int, error) {
	var (
		accepted []byte
		rejected int
		err      error
	)

	if sig == signal.Metric {
		var eng *engine.Engine
		if eng, err = s.engineFor(signal.TenantID(tenant)); err == nil {
			accepted, rejected, err = eng.ApplyPrimary(walBytes)
		}
	} else {
		var eng *recordengine.Engine
		if eng, err = s.recordEngineFor(sig, tenant); err == nil {
			accepted, rejected, err = eng.ApplyPrimary(walBytes)
		}
	}

	if err != nil {
		return 0, errors.Wrapf(err, "primary apply for tenant %q", tenant)
	}

	owners := s.cluster.membership.Ring().Lookup([]byte(tenant), s.cluster.rf)

	var targets []replica.Target
	for _, o := range owners {
		if addr := s.cluster.membership.AddrOf(o.ID); addr != s.cluster.self {
			targets = append(targets, replica.Target{Addr: addr})
		}
	}

	// The primary already holds one durable copy; it needs RF/2 more from secondaries, bounded
	// by how many are actually available (availability over strict durability when nodes are down).
	needAcks := min(s.cluster.rf/2, len(targets))
	if err := s.cluster.replicator.ReplicateQuorum(ctx, targets, cluster.EncodeWrite(sig, tenant, accepted), needAcks); err != nil {
		return rejected, errors.Wrapf(err, "replicate tenant %q", tenant)
	}

	return rejected, nil
}

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

		rejected, err := s.primaryWrite(ctx, sig, tenant, walBytes)
		if err != nil {
			s.obs.Logger(ctx).Error("primary-write failed", zap.Stringer("signal", sig), zap.String("tenant", tenant), zap.Error(err))
			http.Error(w, err.Error(), http.StatusInternalServerError)

			return
		}

		_, _ = fmt.Fprintf(w, "%d", rejected)
	})
}

// sendPrimaryWrite forwards a tenant's write to the remote primary at addr and returns the reject
// count it reports. It is bounded by the write policy: each attempt has a per-try timeout (so a
// stuck primary is abandoned), but it retries only when the request provably never reached the
// server ([retry.ConnFailure]) — a write is never re-sent after the primary may have applied it.
func (s *Storage) sendPrimaryWrite(ctx context.Context, addr string, payload []byte) (int, error) {
	s.obs.Logger(ctx).Debug("primary-write send", zap.String("addr", addr), zap.Int("bytes", len(payload)))

	return retry.Do(ctx, s.writePolicy(ctx, rpcOpWrite), func(ctx context.Context) (int, error) {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, "http://"+addr+primaryWritePath, bytes.NewReader(payload))
		if err != nil {
			return 0, errors.Wrap(err, "build primary-write request")
		}

		obs.InjectHTTP(ctx, req.Header) // carry the trace into the primary-write RPC

		resp, err := s.cluster.httpc.Do(req)
		if err != nil {
			return 0, errors.Wrapf(err, "primary-write to %q", addr)
		}
		defer func() { _ = resp.Body.Close() }()

		body, err := io.ReadAll(resp.Body)
		if err != nil {
			return 0, errors.Wrap(err, "read primary-write response")
		}

		if resp.StatusCode != http.StatusOK {
			return 0, errors.Errorf("cluster: primary %q returned %d: %s", addr, resp.StatusCode, bytes.TrimSpace(body))
		}

		rejected, err := strconv.Atoi(string(bytes.TrimSpace(body)))
		if err != nil {
			return 0, errors.Wrap(err, "parse reject count")
		}

		return rejected, nil
	})
}

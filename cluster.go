package storage

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"strconv"
	"time"

	"github.com/go-faster/errors"
	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/oteldb/storage/cluster"
	"github.com/oteldb/storage/cluster/etcd"
	"github.com/oteldb/storage/cluster/replica"
	"github.com/oteldb/storage/query/fetch"
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

	// The replicator applies an inbound (or local) write to the addressed tenant's engine.
	rp := replica.New(cfg.Self.Addr, replica.NewHTTPTransport(nil), s.applyReplicated)

	var lc net.ListenConfig

	ln, err := lc.Listen(ctx, "tcp", cfg.Self.Addr)
	if err != nil {
		_ = client.Close()

		return errors.Wrapf(err, "listen on %q", cfg.Self.Addr)
	}

	mux := http.NewServeMux()
	mux.Handle(replica.ReplicatePath, rp.Handler())                                  // secondary: trusting apply
	mux.Handle(primaryWritePath, s.primaryWriteHandler())                            // primary: OOO apply + replicate
	mux.Handle(cluster.ReadPath, cluster.ReadHandler(s.localFetch, s.localLogFetch)) // read fan-out
	srv := &http.Server{Handler: mux, ReadHeaderTimeout: 5 * time.Second}

	go func() { _ = srv.Serve(ln) }()

	mship, err := etcd.Join(ctx, client, root, cfg.Self, 0)
	if err != nil {
		_ = srv.Close()
		_ = client.Close()

		return errors.Wrap(err, "join cluster")
	}

	s.cluster = &clusterNode{
		client:     client,
		membership: mship,
		ownership:  etcd.NewOwnership(client, root, cfg.Self.ID, mship.LeaseID()),
		replicator: rp,
		httpc:      http.DefaultClient,
		server:     srv,
		listener:   ln,
		self:       cfg.Self.Addr,
		rf:         rf,
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

// clusterFetcherFor returns the read seam for one tenant in cluster mode: if this node owns the
// tenant it serves locally (the head is replicated here, with full matcher pushdown); otherwise
// it fans out to an owner over HTTP (a window superset) and re-applies the request's matchers,
// failing over between owners.
//
//nolint:dupl // the log analog clusterLogFetcherFor deliberately mirrors this shape
func (s *Storage) clusterFetcherFor(tid signal.TenantID) fetch.Fetcher {
	cn := s.cluster
	owners := cn.membership.Ring().Lookup([]byte(s.normalizeTenant(tid)), cn.rf)

	var remotes []fetch.Fetcher
	for _, o := range owners {
		addr := cn.membership.AddrOf(o.ID)
		if addr == cn.self { // this node is an owner: serve locally
			if e, ok := s.lookupEngine(s.normalizeTenant(tid)); ok {
				return e
			}

			return fetch.Merge() // owner but no data yet
		}

		if addr != "" {
			remotes = append(remotes, cluster.NewRemoteFetcher(signal.Metric, addr, nil))
		}
	}

	return &filteringFetcher{inner: failoverFetcher(remotes)}
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
//
//nolint:dupl // deliberately mirrors clusterFetcherFor over the log engine + log remote fetcher
func (s *Storage) clusterLogFetcherFor(tid signal.TenantID) fetch.Fetcher {
	cn := s.cluster
	owners := cn.membership.Ring().Lookup([]byte(s.normalizeTenant(tid)), cn.rf)

	var remotes []fetch.Fetcher
	for _, o := range owners {
		addr := cn.membership.AddrOf(o.ID)
		if addr == cn.self { // owner: serve locally (head replicated here)
			if e, ok := s.lookupLogEngine(s.normalizeTenant(tid)); ok {
				return e
			}

			return fetch.Merge() // owner but no data yet
		}

		if addr != "" {
			remotes = append(remotes, cluster.NewRemoteFetcher(signal.Log, addr, nil))
		}
	}

	return &filteringFetcher{inner: failoverFetcher(remotes)}
}

// applyReplicated is the secondary receive path: it decodes a primary's accepted write and
// applies it verbatim to the local tenant engine for the addressed signal (metrics or logs) —
// no OOO re-check, the primary already decided.
func (s *Storage) applyReplicated(_ context.Context, payload []byte) error {
	sig, tenant, walBytes, err := cluster.DecodeWrite(payload)
	if err != nil {
		return err
	}

	if sig == signal.Log {
		if err := s.logEngineFor(signal.TenantID(tenant)).ApplyReplicated(walBytes); err != nil {
			return errors.Wrapf(err, "apply replicated logs for tenant %q", tenant)
		}

		return nil
	}

	if err := s.engineFor(signal.TenantID(tenant)).ApplyReplicated(walBytes); err != nil {
		return errors.Wrapf(err, "apply replicated write for tenant %q", tenant)
	}

	return nil
}

// failoverFetcher tries its children in order and returns the first that succeeds — a read
// from any single owner is complete (owners are replicas), so this tolerates a down owner.
type failoverFetcher []fetch.Fetcher

func (fs failoverFetcher) Fetch(ctx context.Context, r fetch.Request) (fetch.Iterator, error) {
	if len(fs) == 0 {
		return nil, errors.New("cluster: no reachable owners for tenant")
	}

	var lastErr error
	for _, f := range fs {
		it, err := f.Fetch(ctx, r)
		if err == nil {
			return it, nil
		}

		lastErr = err
	}

	return nil, errors.Wrap(lastErr, "cluster: all owners failed")
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
// its named attribute).
func matchesAllSeries(s signal.Series, matchers []fetch.Matcher) bool {
	for i := range matchers {
		v, ok := s.Attributes.Get(matchers[i].Name)
		if !ok || !matchers[i].Match(v) {
			return false
		}
	}

	return true
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
	type tenantWAL struct {
		buf  bytes.Buffer
		w    *wal.Writer
		seen map[signal.SeriesID]struct{}
	}

	byTenant := make(map[signal.TenantID]*tenantWAL)

	emitted := metric.Project(md, func(b *metric.Batch) {
		tid := s.tenantFor(b.Resource(), b.Scope())

		tw := byTenant[tid]
		if tw == nil {
			tw = &tenantWAL{seen: make(map[signal.SeriesID]struct{})}
			tw.w = wal.NewWriter(&tw.buf)
			byTenant[tid] = tw
		}

		for i := range b.Len() {
			id := b.IDs[i]
			if _, ok := tw.seen[id]; !ok { // register each series once
				tw.seen[id] = struct{}{}
				_ = tw.w.WriteSeries(id, b.Series(i))
			}

			_ = tw.w.WriteSamples(id, b.Ts[i:i+1], b.Values[i:i+1])
		}
	})

	var rejected int64

	for tid, tw := range byTenant {
		tenant := string(s.normalizeTenant(tid))

		rej, err := s.routeToPrimary(ctx, signal.Metric, tenant, tw.buf.Bytes())
		if err != nil {
			return Accepted{Accepted: int64(emitted) - rejected, Rejected: rejected}, err
		}

		rejected += int64(rej)
	}

	return Accepted{Accepted: int64(emitted) - rejected, Rejected: rejected}, nil
}

const primaryWritePath = "/internal/primary-write"

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

	if sig == signal.Log {
		accepted, rejected, err = s.logEngineFor(signal.TenantID(tenant)).ApplyPrimary(walBytes)
	} else {
		accepted, rejected, err = s.engineFor(signal.TenantID(tenant)).ApplyPrimary(walBytes)
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

		rejected, err := s.primaryWrite(req.Context(), sig, tenant, walBytes)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)

			return
		}

		_, _ = fmt.Fprintf(w, "%d", rejected)
	})
}

// sendPrimaryWrite forwards a tenant's write to the remote primary at addr and returns the
// reject count it reports.
func (s *Storage) sendPrimaryWrite(ctx context.Context, addr string, payload []byte) (int, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, "http://"+addr+primaryWritePath, bytes.NewReader(payload))
	if err != nil {
		return 0, errors.Wrap(err, "build primary-write request")
	}

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
}

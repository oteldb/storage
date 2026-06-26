package storage

import (
	"bytes"
	"context"
	"net"
	"net/http"
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
	writer     *cluster.Writer
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
	mux.Handle(replica.ReplicatePath, rp.Handler())
	mux.Handle(cluster.ReadPath, cluster.ReadHandler(s.localFetch)) // read fan-out endpoint
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
		writer:     cluster.NewWriter(rf, mship, mship.AddrOf, rp),
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
			remotes = append(remotes, cluster.NewRemoteFetcher(addr, nil))
		}
	}

	return &filteringFetcher{inner: failoverFetcher(remotes)}
}

// applyReplicated decodes a replicated write and applies it to the local tenant engine. It is
// the receive side of replication (called for both local and remote owners).
func (s *Storage) applyReplicated(_ context.Context, payload []byte) error {
	tenant, walBytes, err := cluster.DecodeWrite(payload)
	if err != nil {
		return err
	}

	// The OOO-rejected count is not propagated back to the origin's partial-success; cluster
	// ingest reports accepted == emitted (see writeMetricsClustered).
	if _, err := s.engineFor(signal.TenantID(tenant)).ApplyReplicated(walBytes); err != nil {
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
// tenant's series+samples as a WAL-encoded payload, and routes each to its ring-owners at
// write quorum (the local owner applies in process). Out-of-order rejection and per-point
// accounting of the single-node path are not applied here.
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

	for tid, tw := range byTenant {
		payload := cluster.EncodeWrite(string(s.normalizeTenant(tid)), tw.buf.Bytes())
		if err := s.cluster.writer.Write(ctx, string(s.normalizeTenant(tid)), payload); err != nil {
			return Accepted{Accepted: int64(emitted)}, err
		}
	}

	return Accepted{Accepted: int64(emitted)}, nil
}

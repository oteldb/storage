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
	"github.com/oteldb/storage/signal"
	"github.com/oteldb/storage/signal/metric"
	"github.com/oteldb/storage/wal"
)

// clusterNode is the cluster runtime a [Storage] owns in cluster mode: the etcd client and
// membership, the replica server, and the routed write path.
type clusterNode struct {
	client     *clientv3.Client
	membership *etcd.Membership
	writer     *cluster.Writer
	server     *http.Server
	listener   net.Listener
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
		writer:     cluster.NewWriter(rf, mship, mship.AddrOf, rp),
		server:     srv,
		listener:   ln,
	}

	return nil
}

// applyReplicated decodes a replicated write and applies it to the local tenant engine. It is
// the receive side of replication (called for both local and remote owners).
func (s *Storage) applyReplicated(_ context.Context, payload []byte) error {
	tenant, walBytes, err := cluster.DecodeWrite(payload)
	if err != nil {
		return err
	}

	if err := s.engineFor(signal.TenantID(tenant)).ApplyReplicated(walBytes); err != nil {
		return errors.Wrapf(err, "apply replicated write for tenant %q", tenant)
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

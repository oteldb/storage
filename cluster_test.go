package storage

import (
	"context"
	"net"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	fsserver "github.com/go-faster/fs/server"
	"github.com/go-faster/fs/storagemem"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/server/v3/embed"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/backend/s3"
	"github.com/oteldb/storage/cluster"
	"github.com/oteldb/storage/cluster/etcd"
	"github.com/oteldb/storage/query/fetch"
)

func freeAddr(t *testing.T) string {
	t.Helper()
	var lc net.ListenConfig
	l, err := lc.Listen(context.Background(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := l.Addr().String()
	require.NoError(t, l.Close())

	return addr
}

// startEtcd boots an in-process single-node etcd and returns its client endpoint URL.
func startEtcd(t *testing.T) string {
	t.Helper()

	clientURL := "http://" + freeAddr(t)
	peerURL := "http://" + freeAddr(t)
	lc, _ := url.Parse(clientURL)
	lp, _ := url.Parse(peerURL)

	cfg := embed.NewConfig()
	cfg.Dir = t.TempDir()
	cfg.LogLevel = "error"
	cfg.ListenClientUrls = []url.URL{*lc}
	cfg.AdvertiseClientUrls = []url.URL{*lc}
	cfg.ListenPeerUrls = []url.URL{*lp}
	cfg.AdvertisePeerUrls = []url.URL{*lp}
	cfg.InitialCluster = cfg.Name + "=" + peerURL

	e, err := embed.StartEtcd(cfg)
	require.NoError(t, err)
	t.Cleanup(e.Close)

	select {
	case <-e.Server.ReadyNotify():
	case <-time.After(30 * time.Second):
		t.Fatal("embedded etcd did not become ready")
	}

	return clientURL
}

func openClusterNode(t *testing.T, endpoint, id string) *Storage {
	t.Helper()

	return openClusterNodeWith(t, endpoint, id, backend.Memory())
}

func openClusterNodeWith(t *testing.T, endpoint, id string, be backend.Backend, opts ...Option) *Storage {
	t.Helper()

	all := append([]Option{WithBackend(be), WithCluster(&cluster.Config{
		Etcd: []string{endpoint},
		Self: etcd.Member{ID: id, Addr: freeAddr(t)},
		RF:   2,
	})}, opts...)

	s, err := Open(context.Background(), Options{}, all...)
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close(context.Background()) })

	return s
}

// sharedS3 starts one in-process S3 server and returns a factory of backends over the same
// bucket — so multiple cluster nodes share an object store (the object-store-native model).
func sharedS3(t *testing.T) func() backend.Backend {
	t.Helper()

	store := storagemem.New()
	require.NoError(t, store.CreateBucket(context.Background(), "oteldb"))
	srv := httptest.NewServer(fsserver.NewHandler(store))
	t.Cleanup(srv.Close)

	client := awss3.New(awss3.Options{
		Region:       "us-east-1",
		BaseEndpoint: aws.String(srv.URL),
		UsePathStyle: true,
		Credentials:  credentials.NewStaticCredentialsProvider("test", "test", ""),
	})

	return func() backend.Backend { return s3.New(s3.NewAWS(client, "oteldb"), "") }
}

// TestClusteredStorageReplicatesAcrossNodes is the M6 facade capstone: two clustered Storage
// nodes share an etcd; a write to one is routed by the ring and replicated to both, so each
// node serves it from its own engine.
//
//nolint:paralleltest // owns an embedded etcd; runs serially
func TestClusteredStorageReplicatesAcrossNodes(t *testing.T) {
	endpoint := startEtcd(t)
	ctx := context.Background()

	a := openClusterNode(t, endpoint, "node-a")
	b := openClusterNode(t, endpoint, "node-b")

	// Wait for both nodes to see the full 2-node membership before writing.
	require.Eventually(t, func() bool {
		return len(a.cluster.membership.Members()) == 2 && len(b.cluster.membership.Members()) == 2
	}, 10*time.Second, 50*time.Millisecond, "membership converges to two nodes")

	// Write to node A; with RF=2 the tenant's owners are both nodes (quorum 2 ⇒ both apply).
	_, err := a.WriteMetrics(ctx, gaugeBatch("api", "http.requests", []int64{100, 200}, []float64{1, 2}))
	require.NoError(t, err)

	// Both nodes' engines independently hold the replicated series.
	for name, s := range map[string]*Storage{"node-a": a, "node-b": b} {
		it, err := s.Fetcher("default").Fetch(ctx, fetch.Request{
			Start: 0, End: 1 << 60, Matchers: []fetch.Matcher{nameMatcher("http.requests")},
		})
		require.NoError(t, err)
		got, err := fetch.Drain(ctx, it)
		require.NoError(t, err)
		require.Lenf(t, got, 1, "%s serves the replicated series", name)
		assert.Equalf(t, []int64{100, 200}, got[0].Timestamps, "%s timestamps", name)
		assert.Equalf(t, []float64{1, 2}, got[0].Values, "%s values", name)
	}
}

// TestClusterOnlyPrimaryCompacts proves the rebalance executor at work: with the tenant
// replicated to both nodes, only the ring-primary acquires the compaction claim and flushes to
// the object store; the replica skips, so a tenant's parts are written by exactly one node.
//
//nolint:paralleltest // owns an embedded etcd; runs serially
func TestClusterOnlyPrimaryCompacts(t *testing.T) {
	endpoint := startEtcd(t)
	ctx := context.Background()

	nodes := map[string]*Storage{
		"node-a": openClusterNode(t, endpoint, "node-a"),
		"node-b": openClusterNode(t, endpoint, "node-b"),
	}
	a := nodes["node-a"]

	require.Eventually(t, func() bool {
		return len(a.cluster.membership.Members()) == 2
	}, 10*time.Second, 50*time.Millisecond)

	_, err := a.WriteMetrics(ctx, gaugeBatch("api", "http.requests", []int64{100, 200}, []float64{1, 2}))
	require.NoError(t, err)

	// Both nodes hold the replicated head; identify the ring-primary and the replica.
	p, ok := a.cluster.membership.Ring().Primary([]byte("default"))
	require.True(t, ok)
	primary := nodes[p.ID]

	var replica *Storage
	for id, s := range nodes {
		if id != p.ID {
			replica = s
		}
	}

	// Run a maintenance tick on both nodes.
	primary.maintain(ctx)
	replica.maintain(ctx)

	pe, ok := primary.lookupEngine("default")
	require.True(t, ok)
	assert.Equal(t, 1, pe.PartCount(), "the primary flushed the tenant's part")

	re, ok := replica.lookupEngine("default")
	require.True(t, ok)
	assert.Equal(t, 0, re.PartCount(), "the replica did not compact (it holds no claim)")
}

// TestClusterReplicaTrimsHeadAfterOwnerFlush closes the replica-memory parity gap: over a
// SHARED object store, after the primary flushes a tenant, the replica's maintenance pass pulls
// the flushed part and trims its head to the unflushed window — bounding replica memory while
// still serving the full series (head ∪ parts).
//
//nolint:paralleltest // owns an embedded etcd; runs serially
func TestClusterReplicaTrimsHeadAfterOwnerFlush(t *testing.T) {
	endpoint := startEtcd(t)
	newBackend := sharedS3(t)
	ctx := context.Background()

	nodes := map[string]*Storage{
		"node-a": openClusterNodeWith(t, endpoint, "node-a", newBackend()),
		"node-b": openClusterNodeWith(t, endpoint, "node-b", newBackend()),
	}
	a := nodes["node-a"]

	require.Eventually(t, func() bool {
		return len(a.cluster.membership.Members()) == 2
	}, 10*time.Second, 50*time.Millisecond)

	_, err := a.WriteMetrics(ctx, gaugeBatch("api", "http.requests", []int64{100, 200}, []float64{1, 2}))
	require.NoError(t, err)

	p, ok := a.cluster.membership.Ring().Primary([]byte("default"))
	require.True(t, ok)
	primary := nodes[p.ID]

	var replica *Storage
	for id, s := range nodes {
		if id != p.ID {
			replica = s
		}
	}

	pe, _ := primary.lookupEngine("default")
	re, _ := replica.lookupEngine("default")
	require.Equal(t, 2, pe.HeadSampleCount(), "primary head has the write")
	require.Equal(t, 2, re.HeadSampleCount(), "replica head has the replicated write")

	primary.maintain(ctx) // primary flushes to the shared store
	assert.Equal(t, 1, pe.PartCount(), "primary flushed a part")
	assert.Equal(t, 0, pe.HeadSampleCount(), "primary head drained by flush")

	replica.maintain(ctx) // replica pulls the part and trims its head
	assert.Equal(t, 1, re.PartCount(), "replica loaded the part from the shared store")
	assert.Equal(t, 0, re.HeadSampleCount(), "replica head trimmed — flushed samples dropped")

	// The replica still serves the full series, now from the part.
	it, err := replica.Fetcher("default").Fetch(ctx, fetch.Request{
		Start: 0, End: 1 << 60, Matchers: []fetch.Matcher{nameMatcher("http.requests")},
	})
	require.NoError(t, err)
	got, err := fetch.Drain(ctx, it)
	require.NoError(t, err)
	require.Len(t, got, 1)
	assert.Equal(t, []int64{100, 200}, got[0].Timestamps)
}

// TestClusteredLogsReplicateAndRead is the logs analog of the metric capstone: a log write to one
// node is routed by the ring to the tenant's primary and replicated to both owners, so each node
// serves it. The third node (a non-owner) reads it via the log read fan-out.
//
//nolint:paralleltest // owns an embedded etcd; runs serially
func TestClusteredLogsReplicateAndRead(t *testing.T) {
	endpoint := startEtcd(t)
	ctx := context.Background()

	nodes := map[string]*Storage{
		"node-a": openClusterNode(t, endpoint, "node-a"),
		"node-b": openClusterNode(t, endpoint, "node-b"),
		"node-c": openClusterNode(t, endpoint, "node-c"),
	}
	a := nodes["node-a"]

	require.Eventually(t, func() bool {
		return len(a.cluster.membership.Members()) == 3
	}, 10*time.Second, 50*time.Millisecond, "membership converges to three nodes")

	_, err := a.WriteLogs(ctx, logBatch("api", [3]any{100, 9, "first"}, [3]any{200, 17, "second"}))
	require.NoError(t, err)

	// Every node serves the records — owners from their replica, the non-owner via fan-out.
	for name, s := range nodes {
		got := logBodies(t, s.LogFetcher("default"), fetch.Request{Start: 0, End: 1 << 60})
		assert.Equalf(t, []string{"first", "second"}, got, "%s serves the clustered logs", name)
	}
}

// TestClusteredTracesReplicateAndRead is the traces analog of the clustered-logs capstone: spans
// written to one node are routed to the tenant's primary and replicated to both owners, and a
// trace-by-id lookup on the third (non-owner) node fans out to an owner and returns the trace.
//
//nolint:paralleltest // owns an embedded etcd; runs serially
func TestClusteredTracesReplicateAndRead(t *testing.T) {
	endpoint := startEtcd(t)
	ctx := context.Background()

	nodes := map[string]*Storage{
		"node-a": openClusterNode(t, endpoint, "node-a"),
		"node-b": openClusterNode(t, endpoint, "node-b"),
		"node-c": openClusterNode(t, endpoint, "node-c"),
	}
	a := nodes["node-a"]

	require.Eventually(t, func() bool {
		return len(a.cluster.membership.Members()) == 3
	}, 10*time.Second, 50*time.Millisecond, "membership converges to three nodes")

	_, err := a.WriteTraces(ctx, traceBatch("api",
		spanSpec{traceID: "T", spanID: "root", name: "GET /", start: 100, end: 900},
		spanSpec{traceID: "T", spanID: "child", parent: "root", name: "db", start: 200, end: 400},
	))
	require.NoError(t, err)

	// Every node serves the trace by id — owners locally, the non-owner via fan-out.
	for name, s := range nodes {
		got, err := s.Trace(ctx, "default", []byte("T"))
		require.NoErrorf(t, err, "%s trace-by-id", name)

		names := make([]string, 0, 2)
		for _, b := range got {
			names = append(names, spanNames(b)...)
		}

		assert.ElementsMatchf(t, []string{"GET /", "db"}, names, "%s returns the trace's spans", name)
	}
}

// TestClusteredLogsAccountForRejected proves the primary-authoritative log path reports accurate
// partial-success accounting: an out-of-order record surfaces in Accepted.Rejected.
//
//nolint:paralleltest // owns an embedded etcd; runs serially
func TestClusteredLogsAccountForRejected(t *testing.T) {
	endpoint := startEtcd(t)
	ctx := context.Background()

	s := openClusterNodeWith(t, endpoint, "node-a", backend.Memory(), WithOOOWindow(50))

	require.Eventually(t, func() bool {
		return len(s.cluster.membership.Members()) == 1
	}, 10*time.Second, 50*time.Millisecond)

	acc, err := s.WriteLogs(ctx, logBatch("api", [3]any{2000, 9, "a"}))
	require.NoError(t, err)
	assert.Equal(t, Accepted{Accepted: 1}, acc)

	// 3000 advances newest; 900 is far below (newest-OOOWindow) so the primary rejects it.
	acc, err = s.WriteLogs(ctx, logBatch("api", [3]any{3000, 9, "b"}, [3]any{900, 9, "old"}))
	require.NoError(t, err)
	assert.Equal(t, Accepted{Accepted: 1, Rejected: 1}, acc, "the out-of-order record is accounted as rejected")
}

// TestClusterPrimaryAccountsForRejectedSamples proves the primary-authoritative write path
// reports accurate partial-success accounting: the ring-primary OOO-checks the write (the single
// authority for the shard) and the rejected count surfaces all the way back through the clustered
// ingest call's [Accepted], matching the single-node path.
//
//nolint:paralleltest // owns an embedded etcd; runs serially
func TestClusterPrimaryAccountsForRejectedSamples(t *testing.T) {
	endpoint := startEtcd(t)
	ctx := context.Background()

	s := openClusterNodeWith(t, endpoint, "node-a", backend.Memory(), WithOOOWindow(50))

	require.Eventually(t, func() bool {
		return len(s.cluster.membership.Members()) == 1
	}, 10*time.Second, 50*time.Millisecond)

	// First write establishes the head's newest timestamp at 2000.
	acc, err := s.WriteMetrics(ctx, gaugeBatch("api", "http.requests", []int64{2000}, []float64{1}))
	require.NoError(t, err)
	assert.Equal(t, Accepted{Accepted: 1, Rejected: 0}, acc)

	// Second write: 3000 advances newest; 900 is far below (newest-OOOWindow) so the primary
	// rejects it. The reject count must reach the caller via Accepted.
	acc, err = s.WriteMetrics(ctx, gaugeBatch("api", "http.requests", []int64{3000, 900}, []float64{5, 9}))
	require.NoError(t, err)
	assert.Equal(t, Accepted{Accepted: 1, Rejected: 1}, acc, "the out-of-order sample is accounted as rejected")
}

// TestClusteredReadFansOutToOwners is the read-fan-out capstone: with three nodes and RF=2,
// a tenant is owned by two of them; a query on the third (a non-owner, which holds none of the
// tenant's data) fans out to an owner over HTTP and returns the result.
//
//nolint:paralleltest // owns an embedded etcd; runs serially
func TestClusteredReadFansOutToOwners(t *testing.T) {
	endpoint := startEtcd(t)
	ctx := context.Background()

	nodes := map[string]*Storage{
		"node-a": openClusterNode(t, endpoint, "node-a"),
		"node-b": openClusterNode(t, endpoint, "node-b"),
		"node-c": openClusterNode(t, endpoint, "node-c"),
	}
	a := nodes["node-a"]

	require.Eventually(t, func() bool {
		return len(a.cluster.membership.Members()) == 3
	}, 10*time.Second, 50*time.Millisecond, "membership converges to three nodes")

	// Write via node A; it routes to the tenant's two owners.
	_, err := a.WriteMetrics(ctx, gaugeBatch("api", "http.requests", []int64{100, 200}, []float64{1, 2}))
	require.NoError(t, err)

	// The tenant's owners are two of the three nodes; find the third (the non-owner).
	owners := a.cluster.membership.Ring().Lookup([]byte("default"), 2)
	require.Len(t, owners, 2)
	ownerID := map[string]bool{owners[0].ID: true, owners[1].ID: true}

	var nonOwner *Storage
	var nonOwnerName string
	for name, s := range nodes {
		if !ownerID[name] {
			nonOwner, nonOwnerName = s, name
		}
	}
	require.NotNil(t, nonOwner, "one node is not an owner")

	// The non-owner holds no local data for the tenant...
	_, hasLocal := nonOwner.lookupEngine("default")
	assert.Falsef(t, hasLocal, "%s (non-owner) has no local engine for the tenant", nonOwnerName)

	// ...yet its Fetcher fans out to an owner and returns the data.
	it, err := nonOwner.Fetcher("default").Fetch(ctx, fetch.Request{
		Start: 0, End: 1 << 60, Matchers: []fetch.Matcher{nameMatcher("http.requests")},
	})
	require.NoError(t, err)
	got, err := fetch.Drain(ctx, it)
	require.NoError(t, err)
	require.Lenf(t, got, 1, "%s served the series via read fan-out", nonOwnerName)
	assert.Equal(t, []int64{100, 200}, got[0].Timestamps)
	assert.Equal(t, []float64{1, 2}, got[0].Values)
}

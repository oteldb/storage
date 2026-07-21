package storage

import (
	"context"
	"net"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
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
	qprofile "github.com/oteldb/storage/query/profile"
	"github.com/oteldb/storage/signal"
	"github.com/oteldb/storage/signal/profile"
	"github.com/oteldb/storage/tenant"
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

// openClusterNodeSharded opens a clustered node with ShardsPerTenant set, for per-series sharding.
func openClusterNodeSharded(t *testing.T, endpoint, id string, shards int) *Storage {
	t.Helper()

	s, err := Open(context.Background(), Options{}, WithBackend(backend.Memory()), WithCluster(&cluster.Config{
		Etcd:            []string{endpoint},
		Self:            etcd.Member{ID: id, Addr: freeAddr(t)},
		RF:              2,
		ShardsPerTenant: shards,
	}))
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close(context.Background()) })

	return s
}

// TestClusterPerSeriesShardingSpreadsAndGathers verifies sharded-tenant placement: one tenant's
// series spread across the ring by shard, every series is readable from any node (the read gathers
// across shards), and the shards are genuinely distributed (no single node owns them all).
//
//nolint:paralleltest // owns an embedded etcd; runs serially
func TestClusterPerSeriesShardingSpreadsAndGathers(t *testing.T) {
	endpoint := startEtcd(t)
	ctx := context.Background()

	const shards = 4

	nodes := map[string]*Storage{
		"node-a": openClusterNodeSharded(t, endpoint, "node-a", shards),
		"node-b": openClusterNodeSharded(t, endpoint, "node-b", shards),
		"node-c": openClusterNodeSharded(t, endpoint, "node-c", shards),
	}
	a := nodes["node-a"]

	require.Eventually(t, func() bool {
		return len(a.cluster.membership.Members()) == 3
	}, 10*time.Second, 50*time.Millisecond, "membership converges to three nodes")

	// Write many distinct series through node A; each routes to its shard's primary.
	const nSeries = 24

	names := make([]string, nSeries)
	for i := range names {
		names[i] = "metric_" + strconv.Itoa(i)
		_, err := a.WriteMetrics(ctx, gaugeBatch("svc", names[i], []int64{100}, []float64{float64(i)}))
		require.NoError(t, err)
	}

	// Every series is readable from every node — the read gathers across all shards.
	for name, s := range nodes {
		for i, m := range names {
			it, err := s.Fetcher("default").Fetch(ctx, fetch.Request{
				Start: 0, End: 1 << 60, Matchers: []fetch.Matcher{nameMatcher(m)},
			})
			require.NoError(t, err)
			got, err := fetch.Drain(ctx, it)
			require.NoError(t, err)
			require.Lenf(t, got, 1, "%s reads %s across shards", name, m)
			assert.Equal(t, []float64{float64(i)}, got[0].Values)
		}
	}

	// The shards are genuinely spread: their primaries span more than one node.
	primaries := map[string]bool{}
	for idx := range shards {
		sk := shardKeyOf("default", idx, shards)
		p, ok := a.cluster.membership.Ring().Primary([]byte(sk))
		require.True(t, ok)
		primaries[p.ID] = true
	}
	assert.Greaterf(t, len(primaries), 1, "the %d shards are owned by more than one node", shards)

	// Cross-node fan-out is genuinely exercised: with RF=2 < 3 nodes, every shard has a non-owner,
	// and the "readable from every node" loop above had those non-owners gather from owners. Confirm
	// the data is distributed (not all co-located on one node): the per-node local engine counts sum
	// to more than the shard count, i.e. shards are replicated across distinct nodes.
	total := 0
	for _, s := range nodes {
		s.tmu.Lock()
		total += len(s.tenants)
		s.tmu.Unlock()
	}
	assert.Greaterf(t, total, shards, "shards are replicated across nodes (%d engine instances over %d shards)", total, shards)
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

// TestClusteredProfilesReplicateAndRead is the profiles analog of the clustered capstone: samples
// AND the symbol store written to one node are routed to the tenant's primary and replicated to
// both owners, so every node — including the third (non-owner), via fan-out — serves a sample
// search, the series enumeration, and the symbol resolution.
//
//nolint:paralleltest // owns an embedded etcd; runs serially
func TestClusteredProfilesReplicateAndRead(t *testing.T) {
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

	// profileBatch stacks main→work; this lets us check resolution end to end.
	_, err := a.WriteProfiles(ctx, profileBatch("api", 1000,
		sampleSpec{"cpu", "nanoseconds", 50},
		sampleSpec{"cpu", "nanoseconds", 70},
	))
	require.NoError(t, err)

	// Every node serves the samples, the enumeration, and the resolution — owners locally, the
	// non-owner via fan-out.
	for name, s := range nodes {
		got, err := fetch.Drain(ctx, must(s.ProfileFetcher("default").Fetch(ctx, fetch.Request{
			Signal: signal.Profile, Start: 0, End: 1 << 60,
			Matchers: []fetch.Matcher{nameMatcherSvc("api")},
		})))
		require.NoErrorf(t, err, "%s profile search", name)
		require.Lenf(t, got, 1, "%s serves the replicated stream", name)
		assert.ElementsMatchf(t, []int64{50, 70}, profValues(got[0]), "%s returns the samples", name)

		// Enumeration fan-out: the matching stream is listed with its type label.
		series, err := s.ProfileSeries(ctx, "default", []fetch.Matcher{nameMatcherSvc("api")}, 0, 0)
		require.NoErrorf(t, err, "%s profile series", name)
		require.Lenf(t, series, 1, "%s enumerates the stream", name)
		st, _ := series[0].Resource.Attributes.Get(profile.LabelSampleType)
		assert.Equalf(t, "cpu", string(st.Str()), "%s type label", name)

		// Resolution fan-out: the replicated symbol store resolves the sample's stack to its frames.
		resolver, err := s.ProfileResolver(ctx, "default")
		require.NoErrorf(t, err, "%s profile resolver", name)
		stacks, _ := got[0].Column(profile.ColStackID)
		frames := resolver.Resolve(stacks.Bytes[0])
		names := make([]string, 0, len(frames))
		for _, f := range frames {
			names = append(names, f.Function)
		}
		assert.ElementsMatchf(t, []string{"work", "main"}, names, "%s resolves the stack", name)
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
	assert.Equal(t, Accepted{Accepted: 1, Rejected: 1, RejectedReason: "out_of_order"}, acc,
		"the out-of-order record is accounted as rejected, with its reason")
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
	assert.Equal(t, Accepted{Accepted: 1, Rejected: 1, RejectedReason: "out_of_order"}, acc,
		"the out-of-order sample is accounted as rejected, with its reason")
}

// TestClusterPrimaryAppliesCardinalityLimit proves the shard primary now enforces the per-tenant
// cardinality valve on the clustered write path (not just the OOO check): a write that would mint a
// series beyond MaxSeries is shed by the primary and surfaces as a max_series rejection.
//
//nolint:paralleltest // owns an embedded etcd; runs serially
func TestClusterPrimaryAppliesCardinalityLimit(t *testing.T) {
	endpoint := startEtcd(t)
	ctx := context.Background()

	s := openClusterNodeWith(t, endpoint, "node-a", backend.Memory(),
		WithTenancy(tenant.ResolverFunc(func(signal.TenantID) tenant.Policy {
			return tenant.Policy{Limits: tenant.Limits{MaxSeries: 1}}
		})))

	require.Eventually(t, func() bool {
		return len(s.cluster.membership.Members()) == 1
	}, 10*time.Second, 50*time.Millisecond)

	// First series fills the cardinality budget.
	acc, err := s.WriteMetrics(ctx, gaugeBatch("api", "m1", []int64{1}, []float64{1}))
	require.NoError(t, err)
	assert.Equal(t, Accepted{Accepted: 1}, acc)

	// A second, distinct series would exceed MaxSeries=1: the primary sheds it as cardinality.
	acc, err = s.WriteMetrics(ctx, gaugeBatch("api", "m2", []int64{2}, []float64{2}))
	require.NoError(t, err)
	assert.Equal(t, Accepted{Accepted: 0, Rejected: 1, RejectedReason: "max_series"}, acc,
		"the over-cardinality series is shed by the primary, accounted with its reason")
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

// TestClusteredLogEnumerationFansOut covers the LogSeries/LogKeys cluster fan-out: a non-owner,
// holding none of a tenant's log data, serves both enumerations from an owner over HTTP.
//
//nolint:paralleltest // owns an embedded etcd; runs serially
func TestClusteredLogEnumerationFansOut(t *testing.T) {
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

	// Write logs (with a record attribute) via node A; it routes to the tenant's two owners.
	_, err := a.WriteLogs(ctx, logBatchWithAttrs("api",
		[4]any{100, 9, "first", "http.method"},
		[4]any{200, 17, "second", "http.status_code"},
	))
	require.NoError(t, err)

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

	_, hasLocal := nonOwner.lookupLogEngine("default")
	require.Falsef(t, hasLocal, "%s (non-owner) has no local log engine", nonOwnerName)

	// LogSeries fans out: the non-owner returns the tenant's one stream.
	series, err := nonOwner.LogSeries(ctx, "default", []fetch.Matcher{logSvcMatcher("api")}, 0, 0)
	require.NoError(t, err)
	require.Lenf(t, series, 1, "%s served LogSeries via fan-out", nonOwnerName)

	// LogKeys fans out: including the per-record attribute keys LogSeries cannot see.
	keys, err := nonOwner.LogKeys(ctx, "default", 0, 0)
	require.NoError(t, err)

	got := logKeyScopes(keys)
	assert.Equal(t, KeyScopeResource, got["service.name"], "resource attribute (a stream label)")
	assert.Equal(t, KeyScopeRecord, got["http.method"], "record attribute served via fan-out")
	assert.Equal(t, KeyScopeRecord, got["http.status_code"])
}

// TestInspectClusterSection verifies Inspect populates the cluster view: this node's address, the
// live membership, and the shards it holds a compaction claim on.
//
//nolint:paralleltest // owns an embedded etcd; runs serially
func TestInspectClusterSection(t *testing.T) {
	endpoint := startEtcd(t)
	ctx := context.Background()

	s := openClusterNodeWith(t, endpoint, "node-a", backend.Memory())

	require.Eventually(t, func() bool {
		return len(s.cluster.membership.Members()) == 1
	}, 10*time.Second, 50*time.Millisecond)

	_, err := s.WriteMetrics(ctx, gaugeBatch("api", "m1", []int64{1}, []float64{1}))
	require.NoError(t, err)

	// A maintenance cycle reconciles ownership so this node claims the tenant's shard.
	s.maintain(ctx)

	ss := s.Inspect()
	require.NotNil(t, ss.Cluster, "cluster mode ⇒ cluster section present")
	assert.NotEmpty(t, ss.Cluster.Self, "node address")
	require.Len(t, ss.Cluster.Members, 1)
	assert.Equal(t, "node-a", ss.Cluster.Members[0].ID)
	assert.Contains(t, ss.Cluster.Owned, "default", "the sole node owns the tenant's shard")
}

// TestAdminCompactOwnershipGate verifies the Admin flush/compact ownership gate: a node that is not
// the ring-primary of a tenant returns ErrNotOwner (so a shard's parts stay single-writer), while
// the owner succeeds.
//
//nolint:paralleltest // owns an embedded etcd; runs serially
func TestAdminCompactOwnershipGate(t *testing.T) {
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
	}, 10*time.Second, 50*time.Millisecond)

	_, err := a.WriteMetrics(ctx, gaugeBatch("api", "m1", []int64{1}, []float64{1}))
	require.NoError(t, err)

	// The tenant's primary is one specific node; the others must refuse to compact it.
	primary, ok := a.cluster.membership.Ring().Primary([]byte("default"))
	require.True(t, ok)

	for name, s := range nodes {
		err := s.Admin().Compact(ctx, "default", signal.Metric)
		if name == primary.ID {
			require.NoErrorf(t, err, "%s is the primary and may compact", name)
		} else {
			require.ErrorIsf(t, err, ErrNotOwner, "%s is not the primary and must refuse", name)
		}
	}
}

// TestClusteredRecordShardingGathersAllStreams proves per-signal sharding for records: a tenant's
// log streams scatter across shards, and a query on any node gathers across every shard to return
// the full set (not just the streams whose shard that node happens to own).
//
//nolint:paralleltest // owns an embedded etcd; runs serially
func TestClusteredRecordShardingGathersAllStreams(t *testing.T) {
	endpoint := startEtcd(t)
	ctx := context.Background()

	const shards = 4

	nodes := map[string]*Storage{
		"node-a": openClusterNodeSharded(t, endpoint, "node-a", shards),
		"node-b": openClusterNodeSharded(t, endpoint, "node-b", shards),
		"node-c": openClusterNodeSharded(t, endpoint, "node-c", shards),
	}
	a := nodes["node-a"]

	require.Eventually(t, func() bool {
		return len(a.cluster.membership.Members()) == 3
	}, 10*time.Second, 50*time.Millisecond, "membership converges to three nodes")

	// Write 8 distinct log streams (distinct service.name ⇒ distinct stream ids ⇒ scattered shards).
	const streams = 8
	for i := range streams {
		_, err := a.WriteLogs(ctx, logBatch("svc"+strconv.Itoa(i), [3]any{i + 1, 9, "m"}))
		require.NoError(t, err)
	}

	// Every node must return all streams via cross-shard gather.
	for name, s := range nodes {
		it, err := s.LogFetcher("default").Fetch(ctx, fetch.Request{Signal: signal.Log, Start: 0, End: 1 << 62})
		require.NoErrorf(t, err, "%s fetch", name)
		got, err := fetch.Drain(ctx, it)
		require.NoErrorf(t, err, "%s drain", name)
		assert.Lenf(t, got, streams, "%s gathered all streams across shards", name)
	}
}

// TestClusteredRecordShardingEnumeratesAcrossShards proves LogSeries/LogKeys gather across shards:
// streams (and their distinct attribute keys) scattered over shards are all enumerated from any node.
//
//nolint:paralleltest // owns an embedded etcd; runs serially
func TestClusteredRecordShardingEnumeratesAcrossShards(t *testing.T) {
	endpoint := startEtcd(t)
	ctx := context.Background()

	const shards = 4

	nodes := map[string]*Storage{
		"node-a": openClusterNodeSharded(t, endpoint, "node-a", shards),
		"node-b": openClusterNodeSharded(t, endpoint, "node-b", shards),
		"node-c": openClusterNodeSharded(t, endpoint, "node-c", shards),
	}
	a := nodes["node-a"]

	require.Eventually(t, func() bool {
		return len(a.cluster.membership.Members()) == 3
	}, 10*time.Second, 50*time.Millisecond)

	// Each service is a distinct stream (scattered across shards) carrying a distinct attribute key.
	const streams = 6

	wantKeys := map[string]struct{}{"service.name": {}, "otel.scope.name": {}}

	for i := range streams {
		key := "attr." + strconv.Itoa(i)
		wantKeys[key] = struct{}{}
		_, err := a.WriteLogs(ctx, logBatchWithAttrs("svc"+strconv.Itoa(i), [4]any{i + 1, 9, "m", key}))
		require.NoError(t, err)
	}

	for name, s := range nodes {
		// The cross-shard gather is complete once the reading node's ring has caught up to the full
		// membership; the writes above are synchronous, but a freshly-joined node's ring view can still
		// be converging when the reads start, so poll rather than assume the first read sees every shard.
		require.EventuallyWithTf(t, func(c *assert.CollectT) {
			series, err := s.LogSeries(ctx, "default", nil, 0, 0)
			assert.NoErrorf(c, err, "%s LogSeries", name)
			assert.Lenf(c, series, streams, "%s enumerates all streams across shards", name)

			keys, err := s.LogKeys(ctx, "default", 0, 0)
			assert.NoErrorf(c, err, "%s LogKeys", name)

			got := logKeyScopes(keys)
			for k := range wantKeys {
				assert.Containsf(c, got, k, "%s LogKeys union includes %q", name, k)
			}
		}, 10*time.Second, 50*time.Millisecond, "%s enumerates all streams and keys across shards", name)
	}
}

// TestClusteredShardedTraceByID proves trace-by-id reassembles across shards: a trace whose spans
// belong to different services (distinct streams, scattered shards) returns all spans from any node.
//
//nolint:paralleltest // owns an embedded etcd; runs serially
func TestClusteredShardedTraceByID(t *testing.T) {
	endpoint := startEtcd(t)
	ctx := context.Background()

	const shards = 4

	nodes := map[string]*Storage{
		"node-a": openClusterNodeSharded(t, endpoint, "node-a", shards),
		"node-b": openClusterNodeSharded(t, endpoint, "node-b", shards),
		"node-c": openClusterNodeSharded(t, endpoint, "node-c", shards),
	}
	a := nodes["node-a"]

	require.Eventually(t, func() bool {
		return len(a.cluster.membership.Members()) == 3
	}, 10*time.Second, 50*time.Millisecond)

	// One trace "t" with spans in three services (three streams that scatter across shards).
	for i, svc := range []string{"frontend", "backend", "db"} {
		_, err := a.WriteTraces(ctx, traceBatch(svc, spanSpec{
			traceID: "t", spanID: "s" + strconv.Itoa(i), name: "op" + strconv.Itoa(i), start: 1, end: 2,
		}))
		require.NoError(t, err)
	}

	for name, s := range nodes {
		spans, err := s.Trace(ctx, "default", []byte("t"))
		require.NoErrorf(t, err, "%s trace-by-id", name)

		total := 0
		for _, b := range spans {
			total += len(b.Timestamps)
		}

		assert.Equalf(t, 3, total, "%s reassembles all spans of the trace across shards", name)
	}
}

// TestClusteredShardedProfileResolver proves profile flamegraph resolution unions symbol stores
// across shards: samples from streams on different shards all resolve from any node.
//
//nolint:paralleltest // owns an embedded etcd; runs serially
func TestClusteredShardedProfileResolver(t *testing.T) {
	endpoint := startEtcd(t)
	ctx := context.Background()

	const shards = 4

	nodes := map[string]*Storage{
		"node-a": openClusterNodeSharded(t, endpoint, "node-a", shards),
		"node-b": openClusterNodeSharded(t, endpoint, "node-b", shards),
		"node-c": openClusterNodeSharded(t, endpoint, "node-c", shards),
	}
	a := nodes["node-a"]

	require.Eventually(t, func() bool {
		return len(a.cluster.membership.Members()) == 3
	}, 10*time.Second, 50*time.Millisecond)

	// Profiles for three services (distinct streams ⇒ scattered shards), each with a stack.
	for _, svc := range []string{"svc-a", "svc-b", "svc-c"} {
		_, err := a.WriteProfiles(ctx, profileBatch(svc, 1000, sampleSpec{"cpu", "nanoseconds", 50}))
		require.NoError(t, err)
	}

	for name, s := range nodes {
		// Gather samples across shards.
		got, err := fetch.Drain(ctx, must(s.ProfileFetcher("default").Fetch(ctx, fetch.Request{
			Signal: signal.Profile, Start: 0, End: 1 << 60,
		})))
		require.NoErrorf(t, err, "%s profile search", name)
		require.GreaterOrEqualf(t, len(got), 3, "%s gathers streams across shards", name)

		// The unioned-across-shards symbol store resolves a stack from any stream.
		resolver, err := s.ProfileResolver(ctx, "default")
		require.NoErrorf(t, err, "%s profile resolver", name)
		stacks, _ := got[0].Column(profile.ColStackID)
		frames := resolver.Resolve(stacks.Bytes[0])
		assert.NotEmptyf(t, frames, "%s resolves a stack via the cross-shard symbol union", name)
	}
}

func TestShardHelpers(t *testing.T) {
	t.Parallel()

	// A single shard collapses to the bare tenant (byte-identical to the unsharded layout).
	assert.Equal(t, signal.TenantID("acme"), shardKeyOf("acme", 0, 1))
	assert.Equal(t, signal.TenantID("acme"), shardKeyOf("acme", 0, 0))

	// With N>1 the shard index is suffixed, and tenantOfShard inverts it.
	for _, n := range []int{2, 4, 8} {
		for idx := range n {
			sk := shardKeyOf("acme", idx, n)
			assert.Equal(t, signal.TenantID("acme"), tenantOfShard(sk), "round-trips to the tenant")
			assert.NotEqual(t, signal.TenantID("acme"), sk, "sharded key differs from the tenant")
		}
	}

	// tenantOfShard is a no-op on an unsharded (bare tenant) key.
	assert.Equal(t, signal.TenantID("acme"), tenantOfShard("acme"))

	// shardOf is in range and stable.
	id := signal.SeriesID{Hi: 7, Lo: 123456789}
	assert.Equal(t, 0, shardOf(id, 1))
	for _, n := range []int{2, 4, 16} {
		s := shardOf(id, n)
		assert.GreaterOrEqual(t, s, 0)
		assert.Less(t, s, n)
		assert.Equal(t, s, shardOf(id, n), "deterministic")
	}
}

// TestClusterExplainAnalyzeRemote verifies the distributed EXPLAIN ANALYZE: a profiled query on a
// non-owner grafts the owner's fetch subtree (labeled by peer address) into the requester's tree.
//
//nolint:paralleltest // owns an embedded etcd; runs serially
func TestClusterExplainAnalyzeRemote(t *testing.T) {
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
	}, 10*time.Second, 50*time.Millisecond)

	_, err := a.WriteMetrics(ctx, gaugeBatch("api", "http.requests", []int64{100, 200}, []float64{1, 2}))
	require.NoError(t, err)

	owners := a.cluster.membership.Ring().Lookup([]byte("default"), 2)
	ownerID := map[string]bool{owners[0].ID: true, owners[1].ID: true}

	var nonOwner *Storage
	for name, s := range nodes {
		if !ownerID[name] {
			nonOwner = s
		}
	}
	require.NotNil(t, nonOwner)

	pctx, coll := qprofile.WithCollector(ctx)
	it, err := nonOwner.Fetcher("default").Fetch(pctx, fetch.Request{
		Start: 0, End: 1 << 60, Matchers: []fetch.Matcher{nameMatcher("http.requests")},
	})
	require.NoError(t, err)
	_, err = fetch.Drain(pctx, it)
	require.NoError(t, err)

	root := coll.Root()
	t.Logf("DISTRIBUTED EXPLAIN ANALYZE:\n%s", root.Render())

	// The non-owner's tree contains a "remote …" node holding the owner's engine.fetch subtree.
	var remote *qprofile.Node
	var walk func(*qprofile.Node)
	walk = func(n *qprofile.Node) {
		if strings.HasPrefix(n.Name, "remote ") {
			remote = n
		}
		for _, c := range n.Children {
			walk(c)
		}
	}
	walk(root)

	require.NotNil(t, remote, "the peer's subtree was grafted")
	require.NotNil(t, findNode(remote, "engine.fetch"), "owner's engine.fetch nests under the remote node")
}

// TestClusterPerTenantRF verifies the per-tenant durability policy: a tenant whose
// [tenant.Durability] sets RF=3 is replicated to every node of a 3-node cluster even though the
// cluster-wide default is RF=2, and rfFor falls back to the cluster default for a tenant with a
// zero policy.
//
//nolint:paralleltest // owns an embedded etcd; runs serially
func TestClusterPerTenantRF(t *testing.T) {
	endpoint := startEtcd(t)
	ctx := context.Background()

	// Every tenant resolves to RF=3 (the "gold" durability tier); the cluster default stays 2.
	gold := WithTenancy(tenant.ResolverFunc(func(signal.TenantID) tenant.Policy {
		return tenant.Policy{Durability: tenant.Durability{RF: 3}}
	}))

	nodes := map[string]*Storage{
		"node-a": openClusterNodeWith(t, endpoint, "node-a", backend.Memory(), gold),
		"node-b": openClusterNodeWith(t, endpoint, "node-b", backend.Memory(), gold),
		"node-c": openClusterNodeWith(t, endpoint, "node-c", backend.Memory(), gold),
	}

	require.Eventually(t, func() bool {
		for _, s := range nodes {
			if len(s.cluster.membership.Members()) != 3 {
				return false
			}
		}

		return true
	}, 10*time.Second, 50*time.Millisecond, "membership converges to three nodes")

	a := nodes["node-a"]
	require.Equal(t, 3, a.rfFor("default"), "policy RF overrides the cluster default")
	require.Equal(t, 3, a.rfFor("default/_s1"), "a shard key resolves its tenant's RF")

	_, err := a.WriteMetrics(ctx, gaugeBatch("api", "http.requests", []int64{100, 200}, []float64{1, 2}))
	require.NoError(t, err)

	// With RF=3 every node is an owner: each node's local engine independently holds the series
	// (localFetch never fans out, so this proves replication, not read fan-out).
	for name, s := range nodes {
		require.Eventuallyf(t, func() bool {
			got, err := s.localFetch(ctx, "default", 0, 1<<60, []fetch.Matcher{nameMatcher("http.requests")})

			return err == nil && len(got) == 1
		}, 10*time.Second, 50*time.Millisecond, "%s holds the replicated series locally", name)
	}
}

// TestClusterRFDefaultsWithoutPolicy pins the fallback: with no tenancy resolver the cluster-wide
// RF applies unchanged.
//
//nolint:paralleltest // owns an embedded etcd; runs serially
func TestClusterRFDefaultsWithoutPolicy(t *testing.T) {
	endpoint := startEtcd(t)

	s := openClusterNode(t, endpoint, "node-a") // cluster.Config.RF = 2, no Tenancy
	require.Equal(t, 2, s.rfFor("default"), "zero durability policy falls back to cluster RF")
}

// openClusterNodePrivate opens a clustered node with its own private in-memory backend and
// PrivateBackend set: the shared-nothing mode, where flushed parts replicate node-to-node.
func openClusterNodePrivate(t *testing.T, endpoint, id string, rf int) *Storage {
	t.Helper()

	s, err := Open(context.Background(), Options{}, WithBackend(backend.Memory()), WithCluster(&cluster.Config{
		Etcd:           []string{endpoint},
		Self:           etcd.Member{ID: id, Addr: freeAddr(t)},
		RF:             rf,
		PrivateBackend: true,
	}))
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close(context.Background()) })

	return s
}

// TestClusterSharedNothingReplicatesParts is the shared-nothing analog of
// TestClusterReplicaTrimsHeadAfterOwnerFlush: with per-node PRIVATE backends (no shared store),
// after the primary flushes, the replica's maintenance pass mirrors the flushed part objects
// over the partsync endpoints into its own backend, loads them, and trims its head — same
// convergence, no shared store involved.
//
//nolint:paralleltest // owns an embedded etcd; runs serially
func TestClusterSharedNothingReplicatesParts(t *testing.T) {
	endpoint := startEtcd(t)
	ctx := context.Background()

	nodes := map[string]*Storage{
		"node-a": openClusterNodePrivate(t, endpoint, "node-a", 2),
		"node-b": openClusterNodePrivate(t, endpoint, "node-b", 2),
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

	primary.maintain(ctx) // primary flushes to its own private backend
	pe, _ := primary.lookupEngine("default")
	require.Equal(t, 1, pe.PartCount(), "primary flushed a part")

	replica.maintain(ctx) // replica mirrors the part objects and trims its head
	re, _ := replica.lookupEngine("default")
	assert.Equal(t, 1, re.PartCount(), "replica mirrored the part into its private backend")
	assert.Equal(t, 0, re.HeadSampleCount(), "replica head trimmed to the unflushed window")

	// The replica serves the full series from its own backend — no shared store anywhere.
	it, err := replica.Fetcher("default").Fetch(ctx, fetch.Request{
		Start: 0, End: 1 << 60, Matchers: []fetch.Matcher{nameMatcher("http.requests")},
	})
	require.NoError(t, err)
	got, err := fetch.Drain(ctx, it)
	require.NoError(t, err)
	require.Len(t, got, 1)
	assert.Equal(t, []int64{100, 200}, got[0].Timestamps)
	assert.Equal(t, []float64{1, 2}, got[0].Values)
}

// TestClusterSharedNothingSurvivesNodeLoss is the durability capstone for shared-nothing mode:
// RF=3 over three private backends, the primary flushes and is then lost for good — every
// flushed sample stays queryable from each survivor's own local disk.
//
//nolint:paralleltest // owns an embedded etcd; runs serially
func TestClusterSharedNothingSurvivesNodeLoss(t *testing.T) {
	endpoint := startEtcd(t)
	ctx := context.Background()

	nodes := map[string]*Storage{
		"node-a": openClusterNodePrivate(t, endpoint, "node-a", 3),
		"node-b": openClusterNodePrivate(t, endpoint, "node-b", 3),
		"node-c": openClusterNodePrivate(t, endpoint, "node-c", 3),
	}
	a := nodes["node-a"]

	require.Eventually(t, func() bool {
		for _, s := range nodes {
			if len(s.cluster.membership.Members()) != 3 {
				return false
			}
		}

		return true
	}, 10*time.Second, 50*time.Millisecond)

	_, err := a.WriteMetrics(ctx, gaugeBatch("api", "http.requests", []int64{100, 200, 300}, []float64{1, 2, 3}))
	require.NoError(t, err)

	p, ok := a.cluster.membership.Ring().Primary([]byte("default"))
	require.True(t, ok)
	primary := nodes[p.ID]

	primary.maintain(ctx) // flush on the primary's private disk
	pe, _ := primary.lookupEngine("default")
	require.Equal(t, 1, pe.PartCount())

	// Both replicas mirror the flushed part before the primary dies.
	for id, s := range nodes {
		if id != p.ID {
			s.maintain(ctx)
			re, _ := s.lookupEngine("default")
			require.Equalf(t, 1, re.PartCount(), "%s mirrored the part", id)
		}
	}

	// The primary is permanently lost (its private backend goes with it).
	require.NoError(t, primary.Close(ctx))
	delete(nodes, p.ID)

	// Every survivor still serves all flushed samples from its own backend.
	for id, s := range nodes {
		it, err := s.Fetcher("default").Fetch(ctx, fetch.Request{
			Start: 0, End: 1 << 60, Matchers: []fetch.Matcher{nameMatcher("http.requests")},
		})
		require.NoError(t, err)
		got, err := fetch.Drain(ctx, it)
		require.NoError(t, err)
		require.Lenf(t, got, 1, "%s serves the series after primary loss", id)
		assert.Equalf(t, []int64{100, 200, 300}, got[0].Timestamps, "%s: no flushed sample lost", id)
		assert.Equalf(t, []float64{1, 2, 3}, got[0].Values, "%s values", id)
	}
}

// TestClusterSharedNothingReplicatesLogParts proves the record-signal path of shared-nothing
// part replication: the partsync mirror is signal-agnostic (it moves backend objects under the
// engine prefix), so a flushed log part — columns, blooms, keys.bin, streams.bin — mirrors to
// the replica's private backend the same way a metric part does.
//
//nolint:paralleltest // owns an embedded etcd; runs serially
func TestClusterSharedNothingReplicatesLogParts(t *testing.T) {
	endpoint := startEtcd(t)
	ctx := context.Background()

	nodes := map[string]*Storage{
		"node-a": openClusterNodePrivate(t, endpoint, "node-a", 2),
		"node-b": openClusterNodePrivate(t, endpoint, "node-b", 2),
	}
	a := nodes["node-a"]

	require.Eventually(t, func() bool {
		return len(a.cluster.membership.Members()) == 2
	}, 10*time.Second, 50*time.Millisecond)

	_, err := a.WriteLogs(ctx, logBatch("api", [3]any{100, 9, "first"}, [3]any{200, 17, "second"}))
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

	primary.maintain(ctx)
	pe, ok := primary.lookupLogEngine("default")
	require.True(t, ok)
	require.Equal(t, 1, pe.PartCount(), "primary flushed the log part")

	replica.maintain(ctx)
	re, ok := replica.lookupLogEngine("default")
	require.True(t, ok)
	assert.Equal(t, 1, re.PartCount(), "replica mirrored the log part into its private backend")

	got := logBodies(t, replica.LogFetcher("default"), fetch.Request{Start: 0, End: 1 << 60})
	assert.Equal(t, []string{"first", "second"}, got, "replica serves the logs from its own backend")
}

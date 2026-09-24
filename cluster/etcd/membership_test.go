package etcd

import (
	"context"
	"errors"
	"net"
	"net/url"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/server/v3/embed"
)

const httpScheme = "http"

func TestMemberEncodeDecodeRoundTrip(t *testing.T) {
	t.Parallel()

	in := Member{ID: "node-7", Zone: "eu-1", Addr: "10.0.0.7:9000"}
	out, err := decodeMember(in.encode())
	require.NoError(t, err)
	assert.Equal(t, in, out)

	// The hierarchical failure-domain path round-trips too.
	hier := Member{ID: "node-7", Zone: "rack1", Addr: "10.0.0.7:9000", Domains: []string{"rack1", "srv3"}}
	out, err = decodeMember(hier.encode())
	require.NoError(t, err)
	assert.Equal(t, hier, out)

	// A member without domains decodes with a nil Domains (the old wire form stays valid).
	noDomains, err := decodeMember(in.encode())
	require.NoError(t, err)
	assert.Nil(t, noDomains.Domains)
}

func TestDecodeMemberRejectsGarbage(t *testing.T) {
	t.Parallel()

	_, err := decodeMember([]byte("not json"))
	require.Error(t, err)
}

// etcdStartAttempts bounds the re-pick below. The race is rare and independent
// per attempt, so a handful is plenty; a larger number would only make a real
// bind failure — a firewall, an exhausted ephemeral range — take longer to
// report, and it would report it as a race, which it is not.
const etcdStartAttempts = 5

// freeAddrs returns n distinct free localhost host:port values.
//
// Every listener is held open until all n addresses have been chosen. Calling a
// one-at-a-time helper twice does not guarantee two ports: the first listener is
// closed before the second opens, so the OS is free to hand the same port back,
// and etcd would then be asked to bind its client and peer to one address.
//
// The addresses are still only reservations. Between these closes and etcd's
// bind, anything else on the machine can take one — including another test in
// this package, since each boots its own etcd. embed.StartEtcd takes URLs and
// gives no way to hand it an already-bound listener, so the window cannot be
// closed here; startEtcdOnFreshPorts absorbs it instead.
func freeAddrs(t *testing.T, n int) []string {
	t.Helper()

	var (
		lc        net.ListenConfig
		listeners = make([]net.Listener, 0, n)
		addrs     = make([]string, 0, n)
	)
	for range n {
		l, err := lc.Listen(context.Background(), "tcp", "127.0.0.1:0")
		require.NoError(t, err)
		listeners = append(listeners, l)
		addrs = append(addrs, l.Addr().String())
	}
	for _, l := range listeners {
		require.NoError(t, l.Close())
	}

	return addrs
}

// isAddrInUse reports whether err is a lost-port race rather than a real failure.
//
// The string check backs up errors.Is because embed.StartEtcd reports a bind
// failure through its own error text on some paths rather than wrapping the
// syscall error, and a missed match here would turn an absorbed race back into
// the flake this exists to remove.
func isAddrInUse(err error) bool {
	return err != nil &&
		(errors.Is(err, syscall.EADDRINUSE) ||
			strings.Contains(err.Error(), "address already in use") ||
			strings.Contains(err.Error(), "Only one usage of each socket address"))
}

// bootEtcd starts an embedded etcd on the given addresses. The error is returned
// rather than asserted so a caller can tell a lost port from a real failure.
func bootEtcd(dir string, client, peer url.URL) (*embed.Etcd, error) {
	cfg := embed.NewConfig()
	cfg.Dir = dir
	cfg.LogLevel = "error"
	cfg.ListenClientUrls = []url.URL{client}
	cfg.AdvertiseClientUrls = []url.URL{client}
	cfg.ListenPeerUrls = []url.URL{peer}
	cfg.AdvertisePeerUrls = []url.URL{peer}
	cfg.InitialCluster = cfg.Name + "=" + peer.String()

	return embed.StartEtcd(cfg)
}

// startEtcdOnFreshPorts boots an embedded etcd, re-picking its ports if another
// process took one between the reservation and the bind. It returns the data
// directory it settled on as well, so a caller that restarts the server can
// bring it back on the same state.
//
// Retrying rather than widening the reservation is the fix available: the window
// belongs to the listen-then-close idiom, which reserves nothing, and etcd will
// not take a bound listener. Each attempt gets a new data directory too, so a
// half-started server cannot leave state behind for the next one.
func startEtcdOnFreshPorts(t *testing.T) (e *embed.Etcd, client, peer url.URL, dir string) {
	t.Helper()

	var lastErr error
	for attempt := range etcdStartAttempts {
		addrs := freeAddrs(t, 2)
		client = url.URL{Scheme: httpScheme, Host: addrs[0]}
		peer = url.URL{Scheme: httpScheme, Host: addrs[1]}
		dir = t.TempDir()

		e, err := bootEtcd(dir, client, peer)
		if err == nil {
			return e, client, peer, dir
		}
		if !isAddrInUse(err) {
			require.NoErrorf(t, err, "embedded etcd failed to start on attempt %d", attempt+1)
		}
		lastErr = err
	}

	require.FailNowf(t, "embedded etcd could not obtain a free port",
		"%d attempts, last error: %v", etcdStartAttempts, lastErr)

	return nil, url.URL{}, url.URL{}, ""
}

// awaitReady blocks until the server is serving, or fails the test.
func awaitReady(t *testing.T, e *embed.Etcd) {
	t.Helper()

	select {
	case <-e.Server.ReadyNotify():
	case <-time.After(30 * time.Second):
		e.Close()
		t.Fatal("embedded etcd did not become ready")
	}
}

// startEtcd boots an in-process single-node etcd and returns a client for it.
func startEtcd(t *testing.T) *clientv3.Client {
	t.Helper()

	e, lc, _, _ := startEtcdOnFreshPorts(t)
	t.Cleanup(e.Close)
	awaitReady(t, e)

	client, err := clientv3.New(clientv3.Config{Endpoints: []string{lc.String()}, DialTimeout: 5 * time.Second})
	require.NoError(t, err)
	t.Cleanup(func() { _ = client.Close() })

	return client
}

func memberIDs(ms []Member) []string {
	out := make([]string, len(ms))
	for i, m := range ms {
		out[i] = m.ID
	}

	return out
}

//nolint:paralleltest // owns an embedded etcd; runs serially
func TestMembershipJoinWatchLeave(t *testing.T) {
	client := startEtcd(t)
	ctx := context.Background()

	// Node A joins.
	a, err := Join(ctx, client, "/oteldb", Member{ID: "node-a", Zone: "z1", Addr: "a:1"}, 5*time.Second)
	require.NoError(t, err)
	assert.Equal(t, []string{"node-a"}, memberIDs(a.Members()))
	assert.Equal(t, 1, a.Ring().Len())

	// Node B joins; A's watch must see it and rebuild the ring.
	b, err := Join(ctx, client, "/oteldb", Member{ID: "node-b", Zone: "z2", Addr: "b:1"}, 5*time.Second)
	require.NoError(t, err)

	require.Eventually(t, func() bool { return a.Ring().Len() == 2 }, 5*time.Second, 25*time.Millisecond,
		"node A's ring picks up node B")
	assert.Equal(t, []string{"node-a", "node-b"}, memberIDs(a.Members()))

	// Both nodes place a key on the same owners (deterministic, shared membership).
	assert.Equal(t,
		a.Ring().Lookup([]byte("series-42"), 2),
		b.Ring().Lookup([]byte("series-42"), 2),
		"placement agrees across nodes")

	// Node B leaves (lease revoked on Close); A's ring shrinks back.
	require.NoError(t, b.Close(ctx))
	require.Eventually(t, func() bool { return a.Ring().Len() == 1 }, 5*time.Second, 25*time.Millisecond,
		"node A drops node B after it leaves")
	assert.Equal(t, []string{"node-a"}, memberIDs(a.Members()))

	require.NoError(t, a.Close(ctx))
}

func TestJoinRequiresID(t *testing.T) {
	t.Parallel()

	_, err := Join(context.Background(), nil, "/oteldb", Member{}, time.Second)
	require.Error(t, err)
}

// Issue #509: freeAddr closed each listener before opening the next, so two
// calls could be handed the same port back and etcd would be asked to bind its
// client and peer to one address.
func TestFreeAddrsAreDistinctAndBindable(t *testing.T) {
	t.Parallel()

	const n = 16
	addrs := freeAddrs(t, n)
	require.Len(t, addrs, n)

	seen := make(map[string]bool, n)
	for _, addr := range addrs {
		require.False(t, seen[addr], "freeAddrs handed out %s twice", addr)
		seen[addr] = true

		host, port, err := net.SplitHostPort(addr)
		require.NoError(t, err)
		assert.Equal(t, "127.0.0.1", host, "the harness must not bind a routable address")
		assert.NotEqual(t, "0", port, "an address with port 0 reserves nothing")

		// Released by the time it is returned: the caller is expected to bind it.
		var lc net.ListenConfig
		l, err := lc.Listen(context.Background(), "tcp", addr)
		require.NoErrorf(t, err, "freeAddrs returned %s but it cannot be bound", addr)
		require.NoError(t, l.Close())
	}
}

// isAddrInUse decides whether the retry absorbs a failure or reports it, so it
// has to be right in both directions: a missed race is the flake this change
// exists to remove, and a false positive spends every attempt on an error that
// would never have gone away and then reports it as a lost port.
func TestIsAddrInUseClassifiesRealBindErrors(t *testing.T) {
	t.Parallel()

	var lc net.ListenConfig
	held, err := lc.Listen(context.Background(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer func() { _ = held.Close() }()

	// A genuine collision, produced rather than constructed.
	_, err = lc.Listen(context.Background(), "tcp", held.Addr().String())
	require.Error(t, err)
	assert.True(t, isAddrInUse(err), "a real bind collision must be recognised: %v", err)

	assert.False(t, isAddrInUse(nil), "no error is not a lost port")
	assert.False(t, isAddrInUse(errors.New("permission denied")),
		"an unrelated failure must be reported, not retried away")
	assert.False(t, isAddrInUse(context.DeadlineExceeded),
		"a timeout is not a lost port")

	// The Windows wording is matched by string because the syscall number
	// differs there; asserted directly so the branch is not carried untested on
	// a machine that cannot produce it.
	assert.True(t, isAddrInUse(errors.New(
		"listen tcp 127.0.0.1:2379: bind: Only one usage of each socket address is normally permitted")),
		"the Windows spelling must classify too — CI runs there")
}

// The whole point of the retry is that a boot still succeeds while the machine
// is busy taking ports, so it is exercised with real contention rather than a
// stub: a decoy occupies addresses continuously while etcd starts.
func TestStartEtcdOnFreshPortsSucceedsUnderPortContention(t *testing.T) {
	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		var lc net.ListenConfig
		for {
			select {
			case <-stop:
				return
			default:
			}
			l, err := lc.Listen(context.Background(), "tcp", "127.0.0.1:0")
			if err != nil {
				return
			}
			_ = l.Close()
		}
	}()
	t.Cleanup(func() {
		close(stop)
		<-done
	})

	e, client, peer, dir := startEtcdOnFreshPorts(t)
	t.Cleanup(e.Close)
	awaitReady(t, e)

	assert.NotEqual(t, client.Host, peer.Host, "client and peer never share an address")
	assert.NotEmpty(t, dir, "the data directory is returned so a restart can reuse it")
}

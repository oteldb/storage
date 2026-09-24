// Package etcdtest runs an in-process single-node etcd for tests.
package etcdtest

import (
	"net/url"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/server/v3/embed"
)

const readyTimeout = 30 * time.Second

// Start boots an etcd that stops with tb and returns its client endpoint URL.
func Start(tb testing.TB) string {
	tb.Helper()

	return NewServer(tb).Endpoint()
}

// Server is an embedded etcd that can be stopped and restarted on the same client URL and data
// directory, which is what an etcd rollout looks like to a node that keeps running across it.
type Server struct {
	tb     testing.TB
	dir    string
	client url.URL
	e      *embed.Etcd
}

// NewServer boots an etcd that stops with tb, on a client port the kernel picks.
func NewServer(tb testing.TB) *Server {
	tb.Helper()

	s := &Server{
		tb:     tb,
		dir:    tb.TempDir(),
		client: url.URL{Scheme: "http", Host: "127.0.0.1:0"},
	}
	s.start()
	tb.Cleanup(s.Stop)

	return s
}

// Endpoint returns the client URL, stable across [Server.Restart].
func (s *Server) Endpoint() string {
	return s.client.String()
}

// Stop shuts etcd down. It is a no-op on a stopped server.
func (s *Server) Stop() {
	if s.e == nil {
		return
	}

	s.e.Close()
	s.e = nil
}

// Restart starts a stopped server again on its client URL and data directory. Rebinding a fixed
// port can collide with another socket; nothing reserves it while the server is down.
func (s *Server) Restart() {
	s.tb.Helper()

	require.Nil(s.tb, s.e, "etcd is still running")
	s.start()
}

// Dial returns a client for this server, closed with the test.
func (s *Server) Dial() *clientv3.Client {
	s.tb.Helper()

	c, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{s.Endpoint()},
		DialTimeout: 5 * time.Second,
	})
	require.NoError(s.tb, err)
	s.tb.Cleanup(func() { _ = c.Close() })

	return c
}

func (s *Server) start() {
	s.tb.Helper()

	// The peer URL is never dialed by a single-node cluster, so it is advertised as the unbound
	// port-0 address rather than the one the listener gets.
	peer := url.URL{Scheme: "http", Host: "127.0.0.1:0"}

	cfg := embed.NewConfig()
	cfg.Dir = s.dir
	cfg.LogLevel = "error"
	cfg.ListenClientUrls = []url.URL{s.client}
	cfg.AdvertiseClientUrls = []url.URL{s.client}
	cfg.ListenPeerUrls = []url.URL{peer}
	cfg.AdvertisePeerUrls = []url.URL{peer}
	cfg.InitialCluster = cfg.Name + "=" + peer.String()
	// The gateway dials the configured listen address, which is port 0 on the first start.
	cfg.EnableGRPCGateway = false

	e, err := embed.StartEtcd(cfg)
	require.NoError(s.tb, err)

	select {
	case <-e.Server.ReadyNotify():
	case <-time.After(readyTimeout):
		e.Close()
		s.tb.Fatal("embedded etcd did not become ready")
	}

	s.client.Host = e.Clients[0].Addr().String()
	s.e = e
}

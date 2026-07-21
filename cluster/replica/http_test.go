package replica_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-faster/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/cluster/replica"
)

// node is a test peer: a replicator whose apply records payloads, served over HTTP.
type node struct {
	mu       sync.Mutex
	received [][]byte
	failNext bool
}

func (n *node) apply(_ context.Context, payload []byte) error {
	n.mu.Lock()
	defer n.mu.Unlock()

	if n.failNext {
		return errors.New("apply rejected")
	}

	n.received = append(n.received, payload)

	return nil
}

func (n *node) count() int {
	n.mu.Lock()
	defer n.mu.Unlock()

	return len(n.received)
}

// serve mounts a replicator's handler on an httptest server and returns its host:port.
func serve(t *testing.T, apply replica.ApplyFunc) string {
	t.Helper()

	rp := replica.New("", nil, apply)
	mux := http.NewServeMux()
	mux.Handle(replica.ReplicatePath, rp.Handler())

	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	return strings.TrimPrefix(srv.URL, "http://")
}

func TestHTTPReplicationRoundTrip(t *testing.T) {
	t.Parallel()

	b, c := &node{}, &node{}
	addrB := serve(t, b.apply)
	addrC := serve(t, c.apply)

	// Node A replicates to itself + B + C over the real HTTP transport.
	a := &node{}
	rp := replica.New("local", replica.NewHTTPTransport(nil), a.apply)

	err := rp.Replicate(context.Background(), targets("local", addrB, addrC), []byte("payload"))
	require.NoError(t, err)

	// Quorum is 2 of 3; A always applies locally, so at least one remote also applied. Give
	// the best-effort third a moment, then assert every replica eventually has the write.
	assert.Equal(t, 1, a.count(), "local applied")
	assert.Eventually(t, func() bool { return b.count() == 1 && c.count() == 1 }, time.Second, 20*time.Millisecond,
		"both remotes received the replicated write over HTTP")
}

func TestHTTPRemoteApplyErrorFailsSend(t *testing.T) {
	t.Parallel()

	b := &node{failNext: true}
	addrB := serve(t, b.apply)

	// rf=2 (self + B); B rejects ⇒ quorum 2 unreachable ⇒ error surfaced from the transport.
	rp := replica.New("local", replica.NewHTTPTransport(http.DefaultClient), (&node{}).apply)
	err := rp.Replicate(context.Background(), targets("local", addrB), []byte("p"))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "500")
}

func TestHTTPHandlerRejectsGet(t *testing.T) {
	t.Parallel()

	addr := serve(t, (&node{}).apply)
	u := (&url.URL{Scheme: "http", Host: addr}).JoinPath(replica.ReplicatePath)
	resp, err := http.Get(u.String()) //nolint:noctx // simple test request
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()
	assert.Equal(t, http.StatusMethodNotAllowed, resp.StatusCode)
}

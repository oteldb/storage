package replica_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/go-faster/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/cluster/replica"
)

// fakeTransport records sends and can fail or delay selected addresses.
type fakeTransport struct {
	mu       sync.Mutex
	fail     map[string]bool
	delay    map[string]time.Duration
	received map[string][]byte
}

func newFakeTransport() *fakeTransport {
	return &fakeTransport{fail: map[string]bool{}, delay: map[string]time.Duration{}, received: map[string][]byte{}}
}

func (f *fakeTransport) Send(_ context.Context, addr string, payload []byte) error {
	if d := f.delay[addr]; d > 0 {
		time.Sleep(d)
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	if f.fail[addr] {
		return errors.New("send failed")
	}

	f.received[addr] = payload

	return nil
}

func (f *fakeTransport) got(addr string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	_, ok := f.received[addr]

	return ok
}

// localApplier records payloads applied to the local node.
type localApplier struct {
	mu      sync.Mutex
	applied [][]byte
}

func (l *localApplier) apply(_ context.Context, payload []byte) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.applied = append(l.applied, payload)

	return nil
}

func (l *localApplier) count() int {
	l.mu.Lock()
	defer l.mu.Unlock()

	return len(l.applied)
}

func targets(addrs ...string) []replica.Target {
	out := make([]replica.Target, len(addrs))
	for i, a := range addrs {
		out[i] = replica.Target{Addr: a}
	}

	return out
}

func TestReplicateQuorumReached(t *testing.T) {
	t.Parallel()

	tr := newFakeTransport()
	local := &localApplier{}
	rp := replica.New("self", tr, local.apply)

	// 3 replicas, one remote fails — quorum (2) still met via self + the other remote.
	tr.fail["n3"] = true
	err := rp.Replicate(context.Background(), targets("self", "n2", "n3"), []byte("write"))
	require.NoError(t, err)
	assert.Equal(t, 1, local.count(), "self applied locally")
}

func TestReplicateQuorumNotMet(t *testing.T) {
	t.Parallel()

	tr := newFakeTransport()
	local := &localApplier{}
	rp := replica.New("self", tr, local.apply)

	// Two of three remotes fail; only self acks ⇒ 1 < quorum 2.
	tr.fail["n2"] = true
	tr.fail["n3"] = true
	err := rp.Replicate(context.Background(), targets("self", "n2", "n3"), []byte("w"))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "quorum")
}

func TestReplicateTwoReplicasNeedBoth(t *testing.T) {
	t.Parallel()

	tr := newFakeTransport()
	rp := replica.New("self", tr, (&localApplier{}).apply)

	// rf=2 ⇒ quorum 2: a single failure cannot be tolerated.
	tr.fail["n2"] = true
	require.Error(t, rp.Replicate(context.Background(), targets("self", "n2"), []byte("w")))
}

func TestReplicateReturnsAtQuorumNotAllReplicas(t *testing.T) {
	t.Parallel()

	tr := newFakeTransport()
	rp := replica.New("self", tr, (&localApplier{}).apply)

	// One remote is very slow; quorum (self + fast remote) must return without waiting for it.
	tr.delay["slow"] = 2 * time.Second

	start := time.Now()
	require.NoError(t, rp.Replicate(context.Background(), targets("self", "fast", "slow"), []byte("w")))
	assert.Less(t, time.Since(start), time.Second, "returned at quorum, did not wait for the slow replica")

	// The slow replica still receives the write (best-effort convergence).
	assert.Eventually(t, func() bool { return tr.got("slow") }, 3*time.Second, 20*time.Millisecond)
}

func TestReplicateNoTargets(t *testing.T) {
	t.Parallel()

	rp := replica.New("self", newFakeTransport(), (&localApplier{}).apply)
	require.ErrorIs(t, rp.Replicate(context.Background(), nil, []byte("w")), replica.ErrNoTargets)
}

func TestApplyAppliesLocally(t *testing.T) {
	t.Parallel()

	local := &localApplier{}
	rp := replica.New("self", nil, local.apply) // transport unused by Apply
	require.NoError(t, rp.Apply(context.Background(), []byte("x")))
	assert.Equal(t, 1, local.count())
}

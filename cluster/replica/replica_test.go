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

// gatedTransport holds the send to gate open until released, the way an in-flight request to a
// slow peer is still on the wire when the caller has already moved on.
type gatedTransport struct {
	*fakeTransport

	gate    string
	release chan struct{}
	sendErr chan error
}

func newGatedTransport(gate string) *gatedTransport {
	return &gatedTransport{
		fakeTransport: newFakeTransport(),
		gate:          gate,
		release:       make(chan struct{}),
		sendErr:       make(chan error, 1),
	}
}

func (g *gatedTransport) Send(ctx context.Context, addr string, payload []byte) error {
	if addr != g.gate {
		return g.fakeTransport.Send(ctx, addr, payload)
	}

	select {
	case <-ctx.Done():
		g.sendErr <- ctx.Err()

		return ctx.Err()
	case <-g.release:
	}

	if err := ctx.Err(); err != nil {
		g.sendErr <- err

		return err
	}

	err := g.fakeTransport.Send(ctx, addr, payload)
	g.sendErr <- err

	return err
}

func TestReplicateStragglerSurvivesRequestCancel(t *testing.T) {
	t.Parallel()

	tr := newGatedTransport("slow")
	rp := replica.New("self", tr, (&localApplier{}).apply)

	// The request-scoped context: canceled the moment the primary answers, as an HTTP handler's is.
	reqCtx, cancel := context.WithCancel(t.Context())
	require.NoError(t, rp.ReplicateQuorum(reqCtx, targets("fast", "slow"), []byte("w"), 1))
	cancel()

	close(tr.release)

	require.NoError(t, <-tr.sendErr, "the straggler send must outlive the request context")
	assert.True(t, tr.got("slow"), "the slow secondary must still receive the acknowledged write")
}

func TestCloseCancelsStragglers(t *testing.T) {
	t.Parallel()

	tr := newGatedTransport("slow")
	rp := replica.New("self", tr, (&localApplier{}).apply)

	reqCtx, cancel := context.WithCancel(t.Context())
	require.NoError(t, rp.ReplicateQuorum(reqCtx, targets("fast", "slow"), []byte("w"), 1))
	cancel()

	rp.Close()

	require.ErrorIs(t, <-tr.sendErr, context.Canceled, "Close ends the straggler send")
	assert.False(t, tr.got("slow"))
}

func TestReplicateCallerCancelBoundsOnlyTheWait(t *testing.T) {
	t.Parallel()

	tr := newGatedTransport("slow")
	rp := replica.New("self", tr, (&localApplier{}).apply)

	reqCtx, cancel := context.WithCancel(t.Context())
	cancel()

	err := rp.ReplicateQuorum(reqCtx, targets("slow"), []byte("w"), 1)
	require.ErrorIs(t, err, context.Canceled)

	close(tr.release)

	require.NoError(t, <-tr.sendErr, "the send itself is not cut by the caller's cancellation")
	assert.True(t, tr.got("slow"))
}

func TestSendTimeoutBoundsTheDetachedSend(t *testing.T) {
	t.Parallel()

	tr := newGatedTransport("slow")
	rp := replica.New("self", tr, (&localApplier{}).apply, replica.WithSendTimeout(time.Millisecond))
	t.Cleanup(rp.Close)

	// Never released: with the send detached from the caller, this timeout is the only thing that
	// ends it — and, since it also bounds the quorum sends, what the quorum wait fails on.
	start := time.Now()
	err := rp.ReplicateQuorum(t.Context(), targets("slow"), []byte("w"), 1)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.ErrorIs(t, <-tr.sendErr, context.DeadlineExceeded)
	assert.Less(t, time.Since(start), time.Second, "the configured timeout, not %v, bounds the wait", replica.DefaultSendTimeout)
	assert.False(t, tr.got("slow"))
}

func TestSendTimeoutDefaultsWhenUnset(t *testing.T) {
	t.Parallel()

	for _, d := range []time.Duration{0, -time.Second} {
		tr := newGatedTransport("slow")
		rp := replica.New("self", tr, (&localApplier{}).apply, replica.WithSendTimeout(d))
		t.Cleanup(rp.Close)

		require.NoError(t, rp.ReplicateQuorum(t.Context(), targets("self", "slow"), []byte("w"), 1))

		close(tr.release)
		require.NoError(t, <-tr.sendErr, "a non-positive timeout leaves %v in force", replica.DefaultSendTimeout)
	}
}

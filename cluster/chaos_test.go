package cluster_test

// Fault-injection ("chaos") harness for the L0 distributed write/read path. It wires N real
// in-memory engines together with the real quorum replicator ([replica.Replicator]), the real HRW
// ring for owner resolution, and a fault-injecting in-process transport, then asserts the
// distributed safety properties under node death, network partition, and slow replicas:
//
//   - quorum write succeeds iff a quorum of owners is reachable (no false acks);
//   - every quorum-acked write survives any later minority failure (no data loss);
//   - reads converge by gathering+merging across the reachable replicas (read failover).
//
// It deliberately uses the production replicator/ring/merge code (not reimplementations), so a
// regression in the quorum or merge logic trips these tests. Coordination (etcd) and the HTTP
// transport are out of scope here — they are wire/membership details; this harness targets the
// correctness core (quorum + convergence) that determines whether we lose data under fault.

import (
	"bytes"
	"context"
	"encoding/binary"
	"math"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/go-faster/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/cluster/replica"
	"github.com/oteldb/storage/cluster/ring"
	"github.com/oteldb/storage/engine"
	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/signal"
)

var errUnreachable = errors.New("chaos: peer unreachable")

// chaosCluster is N engines behind a fault-injecting transport, sharing one HRW ring. It models a
// single logical writer whose reachability to each owner is controlled at runtime (kill/revive/
// slow), so it can reproduce node death, partitions (a set of nodes unreachable), and stragglers.
type chaosCluster struct {
	t       *testing.T
	rf      int
	key     []byte
	series  signal.Series
	ring    *ring.Ring
	engines map[string]*engine.Engine

	mu   sync.Mutex
	down map[string]bool
	slow map[string]time.Duration
}

func newChaosCluster(t *testing.T, ids []string, rf int) *chaosCluster {
	t.Helper()

	c := &chaosCluster{
		t: t, rf: rf, key: []byte("chaos-key"),
		series:  signal.Series{Attributes: signal.NewAttributes(signal.KeyValue{Key: []byte("job"), Value: signal.StringValue([]byte("chaos"))})},
		engines: make(map[string]*engine.Engine, len(ids)),
		down:    map[string]bool{}, slow: map[string]time.Duration{},
	}

	nodes := make([]ring.Node, len(ids))
	for i, id := range ids {
		c.engines[id] = engine.New(engine.Config{})
		nodes[i] = ring.Node{ID: id}
	}
	c.ring = ring.New(nodes...)

	return c
}

// Send implements [replica.Transport]: it delivers a replicated write to addr's engine unless the
// peer is unreachable (down/partitioned), after an optional straggler delay.
func (c *chaosCluster) Send(ctx context.Context, addr string, payload []byte) error {
	c.mu.Lock()
	down, slow, eng := c.down[addr], c.slow[addr], c.engines[addr]
	c.mu.Unlock()

	if slow > 0 {
		select {
		case <-time.After(slow):
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	if down || eng == nil {
		return errUnreachable
	}

	ts, val := decodeSample(payload)
	_, err := eng.Append(c.series, ts, val)

	return err
}

// owners returns the rf replica owners of the (single) chaos key, primary first.
func (c *chaosCluster) owners() []ring.Node { return c.ring.Lookup(c.key, c.rf) }

func (c *chaosCluster) kill(id string)   { c.mu.Lock(); c.down[id] = true; c.mu.Unlock() }
func (c *chaosCluster) revive(id string) { c.mu.Lock(); c.down[id] = false; c.mu.Unlock() }
func (c *chaosCluster) setSlow(id string) {
	c.mu.Lock()
	c.slow[id] = 2 * time.Millisecond
	c.mu.Unlock()
}

func (c *chaosCluster) healAll() {
	c.mu.Lock()
	c.down = map[string]bool{}
	c.slow = map[string]time.Duration{}
	c.mu.Unlock()
}

// write replicates one (ts, val) sample to the key's owners with quorum (rf/2+1), returning
// whether it was acked (a quorum durably applied it). Uses the real [replica.Replicator] — the
// "__client__" self never matches an owner, so every owner apply goes through the fault transport.
func (c *chaosCluster) write(ctx context.Context, ts int64, val float64) bool {
	owners := c.owners()
	tgts := make([]replica.Target, len(owners))
	for i, o := range owners {
		tgts[i] = replica.Target{Addr: o.ID}
	}

	rp := replica.New("__client__", c, nil)

	return rp.ReplicateQuorum(ctx, tgts, encodeSample(ts, val), c.rf/2+1) == nil
}

// read gathers the key's samples from every currently-reachable owner and merges them (the real
// [fetch.Merge]) — the read-failover / convergence path. Returns the set of timestamps seen.
func (c *chaosCluster) read(ctx context.Context) map[int64]bool {
	c.mu.Lock()
	var fetchers []fetch.Fetcher
	for _, o := range c.owners() {
		if !c.down[o.ID] {
			fetchers = append(fetchers, c.engines[o.ID])
		}
	}
	c.mu.Unlock()

	seen := map[int64]bool{}
	if len(fetchers) == 0 {
		return seen
	}

	it, err := fetch.Merge(fetchers...).Fetch(ctx, fetch.Request{
		Start: math.MinInt64, End: math.MaxInt64, Matchers: []fetch.Matcher{chaosMatcher()},
	})
	require.NoError(c.t, err)

	batches, err := fetch.Drain(ctx, it)
	require.NoError(c.t, err)

	for _, b := range batches {
		for _, ts := range b.Timestamps {
			seen[ts] = true
		}
	}

	return seen
}

func chaosMatcher() fetch.Matcher {
	want := []byte("chaos")

	return fetch.Matcher{Name: []byte("job"), Match: func(v signal.Value) bool { return bytes.Equal(v.Str(), want) }}
}

func encodeSample(ts int64, val float64) []byte {
	b := make([]byte, 16)
	binary.LittleEndian.PutUint64(b[:8], uint64(ts))
	binary.LittleEndian.PutUint64(b[8:], math.Float64bits(val))

	return b
}

func decodeSample(b []byte) (int64, float64) {
	return int64(binary.LittleEndian.Uint64(b[:8])), math.Float64frombits(binary.LittleEndian.Uint64(b[8:]))
}

// TestChaosNoFaultReplicatesToEveryOwner is the baseline: with no faults, every owner receives
// every write and a gather read sees all of them.
func TestChaosNoFaultReplicatesToEveryOwner(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	c := newChaosCluster(t, []string{"a", "b", "c", "d", "e"}, 3)
	for ts := int64(1); ts <= 100; ts++ {
		require.True(t, c.write(ctx, ts, float64(ts)), "write %d should ack with no faults", ts)
	}

	assert.Len(t, c.read(ctx), 100)
}

// TestChaosMinorityDownStillDurable kills one owner (a minority for rf=3, quorum 2): writes still
// ack via the two reachable owners, and the data is readable both while the owner is down and
// after it returns (still behind, but the quorum holds it).
func TestChaosMinorityDownStillDurable(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	c := newChaosCluster(t, []string{"a", "b", "c", "d", "e"}, 3)
	owners := c.owners()
	c.kill(owners[2].ID)

	for ts := int64(1); ts <= 100; ts++ {
		require.True(t, c.write(ctx, ts, float64(ts)), "write %d should ack with a minority down", ts)
	}
	assert.Len(t, c.read(ctx), 100, "all writes readable from the up quorum")

	c.revive(owners[2].ID)
	assert.Len(t, c.read(ctx), 100, "still complete after the lagging owner returns")
}

// TestChaosMajorityDownWriteFails kills a majority of owners: quorum is unreachable, so writes must
// fail (a write that cannot reach a quorum must never report success).
func TestChaosMajorityDownWriteFails(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	c := newChaosCluster(t, []string{"a", "b", "c", "d", "e"}, 3)
	owners := c.owners()
	c.kill(owners[1].ID)
	c.kill(owners[2].ID)

	assert.False(t, c.write(ctx, 1, 1), "write must fail when a quorum of owners is unreachable")
}

// TestChaosRollingFailureNoLoss rotates a single-node failure across the owners while writing: every
// write still meets quorum, and after healing, every acked write is readable — no data loss across
// a rolling minority outage.
func TestChaosRollingFailureNoLoss(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	c := newChaosCluster(t, []string{"a", "b", "c", "d", "e"}, 3)
	owners := c.owners()
	acked := map[int64]bool{}

	for ts := int64(1); ts <= 300; ts++ {
		c.healAll()
		c.kill(owners[int(ts)%len(owners)].ID) // exactly one owner down each round (minority)

		if c.write(ctx, ts, float64(ts)) {
			acked[ts] = true
		}

		// Mid-flight: every already-acked write is still on a reachable owner, so a gather read sees it.
		if ts%50 == 0 {
			seen := c.read(ctx)
			for a := range acked {
				assert.Truef(t, seen[a], "acked write %d not readable during rolling failure", a)
			}
		}
	}

	assert.Len(t, acked, 300, "every write should ack — only a minority was ever down")

	c.healAll()
	seen := c.read(ctx)
	for a := range acked {
		assert.Truef(t, seen[a], "acked write %d lost after heal", a)
	}
}

// TestChaosRandomized is the soak test: deterministic random faults (kill a minority, slow a
// straggler, or none) between writes, with periodic gather reads. The core invariant — no acked
// write is ever lost — must hold throughout and after a full heal.
func TestChaosRandomized(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	c := newChaosCluster(t, []string{"a", "b", "c", "d", "e", "f", "g"}, 3)
	owners := c.owners()
	rng := rand.New(rand.NewSource(1))
	acked := map[int64]bool{}

	const writes = 1000
	for ts := int64(1); ts <= writes; ts++ {
		c.healAll()
		switch rng.Intn(3) {
		case 0:
			c.kill(owners[rng.Intn(len(owners))].ID) // one owner down (minority)
		case 1:
			c.setSlow(owners[rng.Intn(len(owners))].ID) // a straggler — quorum must not wait for it
		case 2:
			// no fault
		}

		if c.write(ctx, ts, float64(ts)) {
			acked[ts] = true
		}

		if ts%100 == 0 {
			seen := c.read(ctx)
			for a := range acked {
				assert.Truef(t, seen[a], "acked write %d not readable at ts=%d", a, ts)
			}
		}
	}

	require.Len(t, acked, writes, "with only a minority faulted, every write should ack")

	c.healAll()
	seen := c.read(ctx)
	require.Len(t, seen, writes, "no acked write lost after the cluster heals")
}

package storage

import (
	"context"
	"path"
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend/bucketindex"
	"github.com/oteldb/storage/backend/file"
	"github.com/oteldb/storage/cluster"
	"github.com/oteldb/storage/engine"
	"github.com/oteldb/storage/signal"
)

// TestEngineSeamsDecidedBeforeClusterStart pins the creation-time half: recovery builds engines
// while s.cluster is still nil, so the seams must come from the options and tolerate being called
// before the cluster layer exists.
func TestEngineSeamsDecidedBeforeClusterStart(t *testing.T) {
	t.Parallel()

	s := &Storage{opts: Options{Cluster: &cluster.Config{PrivateBackend: true}}}

	r := s.repairerFor("t", "t/metrics")
	require.NotNil(t, r, "a private-backend cluster engine gets a repair seam even before the cluster starts")

	got := r.FetchWants(context.Background(), []bucketindex.Want{{Prefix: "t/metrics/p1"}})
	require.Len(t, got, 1)
	assert.Equal(t, bucketindex.WantIncomplete, got[0].Outcome, "no owner set yet, so nothing is concluded")
	require.NoError(t, got[0].Err)

	term := s.termFor("t")
	require.NotNil(t, term, "a cluster engine carries a term source from creation")
	assert.Zero(t, term(), "and reports no tenure until the cluster layer holds a claim")
}

// recoveredPair is two private-backend nodes over file backends, RF 2, so node-a can be closed and
// reopened over what it left on disk: every engine the reopen creates comes out of recovery, before
// the cluster layer starts.
type recoveredPair struct {
	endpoint string
	dirs     map[string]string
	nodes    map[string]*Storage
}

func newRecoveredPair(t *testing.T) *recoveredPair {
	t.Helper()

	p := &recoveredPair{
		endpoint: startEtcd(t),
		dirs:     map[string]string{"node-a": t.TempDir(), "node-b": t.TempDir()},
		nodes:    make(map[string]*Storage, 2),
	}

	for id := range p.dirs {
		p.nodes[id] = p.open(t, id)
	}

	awaitMembership(t, p.nodes)

	return p
}

func (p *recoveredPair) open(t *testing.T, id string, opts ...Option) *Storage {
	t.Helper()

	be, err := file.New(p.dirs[id])
	require.NoError(t, err)

	return openClusterNodeWith(t, p.endpoint, id, be, opts...)
}

// sharedPart writes one batch through node-a and waits until both nodes' engines for sig name the
// same part, returning that part's index entry: a part a peer holds under the prefix node-a will
// lose.
func (p *recoveredPair) sharedPart(t *testing.T, sig signal.Signal) bucketindex.Entry {
	t.Helper()
	ctx := context.Background()

	a := p.nodes["node-a"]
	shard := shardKeyOf("default", 0, a.cluster.shardCount())

	switch sig {
	case signal.Metric:
		_, err := a.WriteMetrics(ctx, gaugeBatch("api", "http.requests", []int64{100, 200}, []float64{1, 2}))
		require.NoError(t, err)
	default:
		_, err := a.WriteLogs(ctx, logBatch("api", [3]any{100, 9, "first"}, [3]any{200, 17, "second"}))
		require.NoError(t, err)
	}

	var shared string

	require.Eventually(t, func() bool {
		for _, s := range p.nodes {
			_ = s.Admin().MaintainNow(ctx)
		}

		held := make([][]string, 0, 2)
		for _, s := range p.nodes {
			held = append(held, prefixesOf(s.Parts(shard, sig)))
		}

		for _, prefix := range held[0] {
			if slices.Contains(held[1], prefix) {
				shared = prefix

				return true
			}
		}

		return false
	}, 20*time.Second, 100*time.Millisecond, "both owners name the same flushed part")

	ix, err := bucketindex.Load(ctx, a.backend, path.Dir(shared)+"/"+bucketindex.Object)
	require.NoError(t, err)

	i := slices.IndexFunc(ix.Entries, func(e bucketindex.Entry) bool { return e.Prefix == shared })
	require.GreaterOrEqual(t, i, 0)

	return ix.Entries[i]
}

func prefixesOf(parts []PartInfo) []string {
	out := make([]string, len(parts))
	for i := range parts {
		out[i] = parts[i].ID
	}

	return out
}

// restartLosing closes node-a, lets drop rewrite what it left on disk, and reopens it with the
// maintenance loop off, so the only thing that can bring the part back is the engine's own repair
// pass — a partsync mirror would restore the objects and hide a missing seam.
func (p *recoveredPair) restartLosing(t *testing.T, drop func(be *file.File)) *Storage {
	t.Helper()

	require.NoError(t, p.nodes["node-a"].Close(context.Background()))

	be, err := file.New(p.dirs["node-a"])
	require.NoError(t, err)
	drop(be)

	a := p.open(t, "node-a", WithFlushInterval(-1))
	p.nodes["node-a"] = a
	awaitMembership(t, p.nodes)

	return a
}

func dropPartObjects(ctx context.Context, t *testing.T, be *file.File, prefix string) {
	t.Helper()

	keys, err := be.List(ctx, prefix+"/")
	require.NoError(t, err)
	require.NotEmpty(t, keys)

	for _, k := range keys {
		require.NoError(t, be.Delete(ctx, k))
	}
}

// TestRecoveredEngineRepairsFromPeer is the defect end to end: a private-backend node restarts with
// a part gone from its disk that its peer still holds. The engine recovery builds must fetch it
// back; with no repair seam the want is recorded and then never serviced.
//
//nolint:paralleltest // owns an embedded etcd; runs serially
func TestRecoveredEngineRepairsFromPeer(t *testing.T) {
	ctx := context.Background()
	p := newRecoveredPair(t)
	lost := p.sharedPart(t, signal.Log)

	a := p.restartLosing(t, func(be *file.File) { dropPartObjects(ctx, t, be, lost.Prefix) })

	shard := shardKeyOf("default", 0, a.cluster.shardCount())
	eng, ok := a.lookupRecordEngine(signal.Log, shard)
	require.True(t, ok, "the engine exists from recovery, before any write")
	require.Positive(t, eng.Stats().WantedParts, "recovery recorded the loss")

	require.Eventually(t, func() bool {
		_ = eng.Merge(ctx, 0)

		return eng.Stats().WantedParts == 0
	}, 10*time.Second, 100*time.Millisecond, "the recovered engine fetches the part back from its peer")

	assert.Positive(t, eng.RepairStats().Fetched, "it came from the peer, not from local disk")
	assert.Zero(t, eng.LostParts())
}

// TestRecoveredEngineRepairsAdoptedWant is the #587 link: an obligation handed in for a part only
// the peer holds, which this node's index never named. #588 made the owner adopt it, and the
// adopted want is only worth anything if the engine it lands on can fetch — on a node that
// restarted with data on disk, that engine came out of recovery.
//
//nolint:paralleltest // owns an embedded etcd; runs serially
func TestRecoveredEngineRepairsAdoptedWant(t *testing.T) {
	ctx := context.Background()
	p := newRecoveredPair(t)
	lost := p.sharedPart(t, signal.Metric)

	a := p.restartLosing(t, func(be *file.File) {
		key := path.Dir(lost.Prefix) + "/" + bucketindex.Object

		ix, err := bucketindex.Load(ctx, be, key)
		require.NoError(t, err)
		require.True(t, ix.Remove(lost.Prefix))
		require.NoError(t, be.Write(ctx, key, ix.AppendBinary(nil)))

		dropPartObjects(ctx, t, be, lost.Prefix)
	})

	shard := shardKeyOf("default", 0, a.cluster.shardCount())
	eng, ok := a.lookupEngine(shard)
	require.True(t, ok, "the engine exists from recovery, before any write")
	require.False(t, eng.HasWants(), "the local index never named the part, so nothing states it is owed")

	eng.AdoptWants([]bucketindex.Want{bucketindex.WantOf(lost, bucketindex.Generation{})})
	require.True(t, eng.HasWants())

	require.Eventually(t, func() bool {
		_ = eng.Merge(ctx, 0)

		return !eng.HasWants()
	}, 10*time.Second, 100*time.Millisecond, "the adopted want is fetched from the peer")

	assert.Positive(t, eng.RepairStats().Fetched)
	assert.True(t, slices.ContainsFunc(eng.Parts(), func(pi engine.PartStat) bool { return pi.MinTime <= lost.MinTime }),
		"the peer's rows are part of this node's set again")
}

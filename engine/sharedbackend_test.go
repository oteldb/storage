package engine_test

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/backend/bucketindex"
	"github.com/oteldb/storage/backend/faultbackend"
	"github.com/oteldb/storage/engine"
	"github.com/oteldb/storage/query/fetch"
)

// The shared-store deployment (cluster.Config.PrivateBackend false) runs every replica of a shard
// over one object store under one prefix, so two engines flush into the same namespace and commit
// through the same index object. These are the reproducers for what that costs today; the part
// namespace is safe (#383: part ids are minted, so two engines cannot name the same key), but the
// index is still overwritten without compare-and-swap (#392).

const sharedPrefix = "default/metrics"

// sharedWriter mints the distinct node identities the engines of these tests stand in for: over a
// shared store the WAL flush watermark is kept per writer, so two engines sharing one id would
// share one slot — the very thing #397 is about.
var sharedWriter atomic.Int64

func newSharedEngine(t *testing.T, be backend.Backend) *engine.Engine {
	t.Helper()

	e, _ := newSharedWriter(t, be)

	return e
}

// newSharedWriter returns an engine over the shared prefix and the node identity it writes as.
func newSharedWriter(t *testing.T, be backend.Backend) (*engine.Engine, string) {
	t.Helper()

	id := fmt.Sprintf("node-%d", sharedWriter.Add(1))
	e := engine.New(engine.Config{Backend: be, Prefix: sharedPrefix, WriterID: id})
	require.NoError(t, e.LoadParts(context.Background()))

	return e, id
}

// queryable reports which of the given series a fresh reader over be can see, by the value of
// their "job" label.
func queryable(t *testing.T, be backend.Backend, jobs ...string) []string {
	t.Helper()

	r := engine.New(engine.Config{Backend: be, Prefix: sharedPrefix})
	require.NoError(t, r.LoadParts(context.Background()))

	var found []string
	for _, job := range jobs {
		got := fetchAll(t, r, fetch.Request{
			Start:    0,
			End:      10000,
			Matchers: []fetch.Matcher{eqMatcher("job", job)},
		})
		if len(got) > 0 && len(got[0].Timestamps) > 0 {
			found = append(found, job)
		}
	}

	return found
}

// TestSharedBackendFlushKeepsPeerPart is the sequential form: two engines over one backend flush
// one series each, both succeed, and only one of the two is afterwards queryable. Both parts are on
// the store under their own ids ([TestSharedBackendFlushKeepsPeerPartObjects]); the second commit
// writes an index that does not name the first.
func TestSharedBackendFlushKeepsPeerPart(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	be := backend.Memory()

	a := newSharedEngine(t, be)
	b := newSharedEngine(t, be)

	mustAppend(t, a, mkSeries("job", "a"), 100, 1.0)
	mustAppend(t, b, mkSeries("job", "b"), 200, 2.0)

	require.NoError(t, a.Flush(ctx))
	require.NoError(t, b.Flush(ctx))

	require.ElementsMatch(t, []string{"a", "b"}, queryable(t, be, "a", "b"),
		"a flush that reported success must not drop another engine's committed part")
}

// TestSharedBackendFlushKeepsPeerPartObjects covers the identity half of the shared-store loss (#383)
// on its own: two engines over one backend flush a part each, and neither writes over the other's
// objects, because a part id is minted rather than derived from a per-engine counter. The index
// still names only one of the two until the commit takes compare-and-swap (#392) — this is about
// the objects being intact on the store for that fix to name.
func TestSharedBackendFlushKeepsPeerPartObjects(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	be := backend.Memory()

	a := newSharedEngine(t, be)
	b := newSharedEngine(t, be)

	mustAppend(t, a, mkSeries("job", "a"), 100, 1.0)
	require.NoError(t, a.Flush(ctx))

	first := partDirs(t, be, sharedPrefix)
	require.Len(t, first, 1)

	// b has never seen a's part: it mints from its own state, exactly the split-brain case.
	written := objectBytes(t, be, sharedPrefix+"/"+first[0]+"/")
	require.NotEmpty(t, written)

	mustAppend(t, b, mkSeries("job", "b"), 200, 2.0)
	require.NoError(t, b.Flush(ctx))

	both := partDirs(t, be, sharedPrefix)
	require.Len(t, both, 2, "the two engines must mint distinct part ids")
	require.Contains(t, both, first[0])

	require.Equal(t, written, objectBytes(t, be, sharedPrefix+"/"+first[0]+"/"),
		"the second engine's flush must not write over the first engine's part objects")

	other := both[0]
	if other == first[0] {
		other = both[1]
	}

	require.NotEmpty(t, objectBytes(t, be, sharedPrefix+"/"+other+"/"),
		"the second engine's own part must be on the store too")
}

// objectBytes reads every object under prefix, so a later write over any of them is detectable.
func objectBytes(t *testing.T, be backend.Backend, prefix string) map[string][]byte {
	t.Helper()

	keys, err := be.List(context.Background(), prefix)
	require.NoError(t, err)

	out := make(map[string][]byte, len(keys))

	for _, k := range keys {
		v, err := be.Read(context.Background(), k)
		require.NoError(t, err)
		out[k] = v
	}

	return out
}

// TestSharedBackendConcurrentFlushKeepsPeerPart is the concurrent form, so the outcome cannot be
// dismissed as an artifact of the sequential test's ordering. One engine is suspended inside its
// index commit while the other completes a whole flush; the suspended commit then lands on a
// version that has moved under it, and must rebase onto the peer's rather than replace it.
func TestSharedBackendConcurrentFlushKeepsPeerPart(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	be := faultbackend.Wrap(backend.Memory())

	a := newSharedEngine(t, be)
	b := newSharedEngine(t, be)

	mustAppend(t, a, mkSeries("job", "a"), 100, 1.0)
	mustAppend(t, b, mkSeries("job", "b"), 200, 2.0)

	gate := faultbackend.NewGate()
	be.Add(gate.Rule(faultbackend.CompareAndSwap, func(op faultbackend.Op) bool {
		return strings.HasSuffix(op.Key, bucketindex.Object)
	}))

	var flushErr error
	var wg sync.WaitGroup
	wg.Go(func() { flushErr = a.Flush(ctx) })

	gate.Await(t)
	be.Reset()

	require.NoError(t, b.Flush(ctx))

	gate.Release()
	wg.Wait()
	require.NoError(t, flushErr)

	require.ElementsMatch(t, []string{"a", "b"}, queryable(t, be, "a", "b"),
		"an index commit must not clobber one written since it was prepared")
}

// TestSharedBackendFlushSweepsPeerPart shows the loss is permanent rather than an indexing
// oversight: the part objects the winning index does not name are deleted as orphans by the next
// engine to open the prefix, so the data is gone from the store as well as from the index.
func TestSharedBackendFlushSweepsPeerPart(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	be := backend.Memory()

	a := newSharedEngine(t, be)
	b := newSharedEngine(t, be)

	mustAppend(t, a, mkSeries("job", "a"), 100, 1.0)
	require.NoError(t, a.Flush(ctx))

	mustAppend(t, a, mkSeries("job", "a"), 300, 3.0)
	require.NoError(t, a.Flush(ctx))

	// b has been holding its own view since before either flush, so its commit names neither part.
	mustAppend(t, b, mkSeries("job", "b"), 200, 2.0)
	require.NoError(t, b.Flush(ctx))

	before, err := be.List(ctx, sharedPrefix+"/")
	require.NoError(t, err)

	// Opening the prefix sweeps whatever the index does not name.
	newSharedEngine(t, be)

	after, err := be.List(ctx, sharedPrefix+"/")
	require.NoError(t, err)

	require.Equal(t, before, after,
		"the open-time orphan sweep must not delete parts another engine committed")
}

package engine_test

import (
	"context"
	"strings"
	"sync"
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
// through the same index object. These are the reproducers for what that costs today; see #392
// (the index is overwritten without compare-and-swap) and #383 (part sequences are minted from
// per-engine counters into a shared namespace).

const sharedPrefix = "default/metrics"

func newSharedEngine(t *testing.T, be backend.Backend) *engine.Engine {
	t.Helper()

	e := engine.New(engine.Config{Backend: be, Prefix: sharedPrefix})
	require.NoError(t, e.LoadParts(context.Background()))

	return e
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
// one series each, both succeed, and only one of the two is afterwards queryable. The second
// flush mints the sequence the first already used, so it overwrites the first engine's part
// objects and then commits an index that does not name them.
func TestSharedBackendFlushKeepsPeerPart(t *testing.T) {
	t.Skip("reproducer for #383/#392: engines over a shared store mint colliding part sequences and commit without CAS")
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

// TestSharedBackendConcurrentFlushKeepsPeerPart is the concurrent form, so the loss cannot be
// dismissed as an artifact of the sequential test's ordering. One engine is suspended inside its
// index commit while the other completes a whole flush; the suspended commit then lands on top,
// writing an index that names only its own parts.
func TestSharedBackendConcurrentFlushKeepsPeerPart(t *testing.T) {
	t.Skip("reproducer for #392: the bucket index is committed without compare-and-swap")
	t.Parallel()

	ctx := context.Background()
	be := faultbackend.Wrap(backend.Memory())

	a := newSharedEngine(t, be)
	b := newSharedEngine(t, be)

	mustAppend(t, a, mkSeries("job", "a"), 100, 1.0)
	mustAppend(t, b, mkSeries("job", "b"), 200, 2.0)

	gate := faultbackend.NewGate()
	be.Add(gate.Rule(faultbackend.Write, func(op faultbackend.Op) bool {
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
	t.Skip("reproducer for #392: the orphan sweep deletes parts the lost index update named")
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

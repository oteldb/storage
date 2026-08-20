package recordengine_test

import (
	"context"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/backend/bucketindex"
	"github.com/oteldb/storage/backend/faultbackend"
	"github.com/oteldb/storage/query/fetch"
)

// TestFlushRebasesOnARivalIndexCommit is the shared-store case of #392, with the interleaving
// stated rather than raced for: the flush is suspended *inside* its conditional index write, a
// rival writer commits a part of its own over the same key, and the flush is released. It must
// lose, reload, and commit an index naming both parts — the entry is the only thing keeping
// either part reachable.
func TestFlushRebasesOnARivalIndexCommit(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	const (
		prefix = "t/recs"
		rival  = prefix + "/0000009999"
	)

	inner := backend.Memory()
	be := faultbackend.Wrap(inner)
	gate := faultbackend.NewGate()
	be.Add(gate.Rule(faultbackend.CompareAndSwap, func(op faultbackend.Op) bool {
		return strings.HasSuffix(op.Key, "/"+bucketindex.Object)
	}))

	e := newEngine(t, be)
	ingest(t, e, mkBatch("api", rrec{ts: 100, body: "p1"}))

	flushed := make(chan error, 1)
	go func() { flushed <- e.Flush(ctx) }()

	gate.Await(t)

	other := &bucketindex.Index{Generation: bucketindex.Generation{Term: 1, Counter: 1}}
	other.Add(bucketindex.Entry{Prefix: rival, MinTime: 1, MaxTime: 2})
	_, err := other.Save(ctx, inner, prefix+"/"+bucketindex.Object, backend.VersionAbsent)
	require.NoError(t, err)

	gate.Release()

	require.NoError(t, <-flushed, "a flush that rebases still lands, and only reports success once it has")

	ix, err := bucketindex.Load(ctx, inner, prefix+"/"+bucketindex.Object)
	require.NoError(t, err)

	prefixes := make([]string, 0, len(ix.Entries))
	for _, ent := range ix.Entries {
		prefixes = append(prefixes, ent.Prefix)
	}

	require.Contains(t, prefixes, rival, "the rival's part must survive this engine's commit")
	require.Len(t, prefixes, 2, "and this engine's own flushed part is committed alongside it")
}

// TestRebasedFlushServesTheAdoptedPart is #398 for the record engine: the entries a rebase carries
// forward go into the committed index, so they must go into the part set this engine reads too —
// an index naming a part the engine will not serve is a query that silently misses rows until the
// next LoadParts.
func TestRebasedFlushServesTheAdoptedPart(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	const prefix = "t/recs"

	be := faultbackend.Wrap(backend.Memory())
	a := newEngine(t, be)
	b := newEngine(t, be)

	ingest(t, a, mkBatch("api", rrec{ts: 100, body: "a"}))
	ingest(t, b, mkBatch("web", rrec{ts: 200, body: "b"}))

	gate := faultbackend.NewGate()
	be.Add(gate.Rule(faultbackend.CompareAndSwap, func(op faultbackend.Op) bool {
		return strings.HasSuffix(op.Key, "/"+bucketindex.Object)
	}))

	var (
		flushErr error
		wg       sync.WaitGroup
	)

	wg.Go(func() { flushErr = a.Flush(ctx) })

	gate.Await(t)
	be.Reset()

	require.NoError(t, b.Flush(ctx))

	gate.Release()
	wg.Wait()
	require.NoError(t, flushErr)

	ix, err := bucketindex.Load(ctx, be, prefix+"/"+bucketindex.Object)
	require.NoError(t, err)
	require.Len(t, ix.Entries, 2, "the rebased commit names both writers' parts")

	got := fetchAll(t, a, fetch.Request{Start: 0, End: 10000, Matchers: []fetch.Matcher{svcMatcher("web")}})
	require.Len(t, got, 1, "the rebasing engine serves the part it adopted")
	require.Equal(t, []int64{200}, got[0].Timestamps)

	require.Equal(t, 1, a.PartCount(), "which is still not a part it owns")
	require.Len(t, a.Parts(), 2, "though it is one it serves")
}

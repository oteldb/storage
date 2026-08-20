package engine_test

import (
	"context"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/backend/bucketindex"
	"github.com/oteldb/storage/backend/faultbackend"
	"github.com/oteldb/storage/query/fetch"
)

// The invariant these state: the index an engine commits and the part set it serves are the same
// set. A commit that rebases carries a rival's entries forward, so it must open them too — the
// index naming a part the engine will not serve is #398.

// TestConcurrentRebaseServesTheAdoptedPart states the interleaving rather than racing for it: one
// engine is suspended inside its conditional index write while the other completes a whole flush,
// so the suspended commit lands on a version that moved under it. What it publishes, it must
// answer for immediately — not at the next maintenance tick.
func TestConcurrentRebaseServesTheAdoptedPart(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	be := faultbackend.Wrap(backend.Memory())
	a := newSharedEngine(t, be)
	b := newSharedEngine(t, be)

	mustAppend(t, a, mkSeries("job", "api"), 100, 1.0)
	mustAppend(t, b, mkSeries("job", "web"), 200, 2.0)

	gate := faultbackend.NewGate()
	be.Add(gate.Rule(faultbackend.CompareAndSwap, func(op faultbackend.Op) bool {
		return strings.HasSuffix(op.Key, bucketindex.Object)
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

	require.Len(t, committedIndex(t, be).Entries, 2, "the rebased commit names both writers' parts")

	got := fetchAll(t, a, fetch.Request{
		Start: 0, End: 10000, Matchers: []fetch.Matcher{eqMatcher("job", "web")},
	})
	require.Len(t, got, 1, "the rebasing engine serves the part it adopted")
	assert.Equal(t, []int64{200}, got[0].Timestamps)

	own := fetchAll(t, a, fetch.Request{
		Start: 0, End: 10000, Matchers: []fetch.Matcher{eqMatcher("job", "api")},
	})
	require.Len(t, own, 1, "and still its own")
	assert.Equal(t, []int64{100}, own[0].Timestamps)
}

// TestAdoptedPartIsNotOwned separates the two sets: an adopted part is readable but not this
// engine's to merge, remove or delete, so it must not enter the part set the commit diffs against —
// which would make it look, on the next commit, like a part this engine had removed.
func TestAdoptedPartIsNotOwned(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	be := backend.Memory()
	a, b, _, _ := rivals(t, be)

	mustAppend(t, a, mkSeries("job", "api"), 100, 1.0)
	require.NoError(t, a.Flush(ctx))

	mustAppend(t, b, mkSeries("job", "web"), 200, 2.0)
	require.NoError(t, b.Flush(ctx))

	assert.Equal(t, 1, b.PartCount(), "one part is b's own")
	assert.Len(t, b.Parts(), 2, "and both are servable")

	mustAppend(t, b, mkSeries("job", "web"), 300, 3.0)
	require.NoError(t, b.Flush(ctx))

	after := committedIndex(t, be)
	assert.Empty(t, after.Removed, "a part this engine never owned is not one it can have removed")
	assert.Len(t, after.Entries, 3, "b's two parts and the one it adopted")
}

// TestUnreadableAdoptedPartStillCommits keeps the commit path robust: a rival's part that cannot be
// opened here — it merged it away between the two commits — must stay in the index (only its writer
// knows whether it is live) without failing the flush that adopted it.
func TestUnreadableAdoptedPartStillCommits(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	inner := backend.Memory()
	be := faultbackend.Wrap(inner)
	a := newSharedEngine(t, be)

	const rival = sharedPrefix + "/0000009999"

	other := &bucketindex.Index{Generation: bucketindex.Generation{Term: 1, Counter: 1}}
	other.Add(bucketindex.Entry{Prefix: rival, MinTime: 1, MaxTime: 2})
	_, err := other.Save(ctx, inner, sharedPrefix+"/"+bucketindex.Object, backend.VersionAbsent)
	require.NoError(t, err)

	mustAppend(t, a, mkSeries("job", "api"), 100, 1.0)
	require.NoError(t, a.Flush(ctx), "an entry this engine cannot open must not fail its flush")

	prefixes := make([]string, 0, 2)
	for _, ent := range committedIndex(t, be).Entries {
		prefixes = append(prefixes, ent.Prefix)
	}

	assert.Contains(t, prefixes, rival, "the entry is what keeps the rival's part reachable")
}

package enginetest

import (
	"context"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/backend/bucketindex"
	"github.com/oteldb/storage/backend/faultbackend"
)

// partIDs returns the engine's live part prefixes, sorted. A part's identity survives only if it was
// not rewritten, which is how the retention tests tell a drop from a merge.
func partIDs(e Engine) []string {
	parts := e.Parts()

	out := make([]string, 0, len(parts))
	for _, p := range parts {
		out = append(out, p.ID)
	}

	slices.Sort(out)

	return out
}

// retentionDropsWholePartWithoutRewrite: a part whose newest row is already past the cutoff holds
// nothing retention would keep, so it is retired on the manifest alone. The surviving part keeps its
// identity (a rewrite would mint a new prefix), which proves nothing was decoded and re-encoded.
func retentionDropsWholePartWithoutRewrite(t *testing.T, k Kind) {
	t.Helper()

	ctx := context.Background()
	e := k.open(t, backend.Memory())

	e.Append(t, api(100, 1), api(200, 2))
	require.NoError(t, e.Flush(ctx))

	e.Append(t, api(900, 9))
	require.NoError(t, e.Flush(ctx))

	before := partIDs(e)
	require.Len(t, before, 2)

	// Cutoff past every row of the first part, before every row of the second.
	require.NoError(t, e.Merge(ctx, 500))

	after := partIDs(e)
	require.Len(t, after, 1)
	assert.Equal(t, before[1], after[0], "the live part must not be rewritten to drop an unrelated one")
	assert.Equal(t, []Row{api(900, 9)}, rows(t, e, apiStream))
}

// retentionDropsExpiredAndRewritesStraddler: the two paths coexist in one cycle. The fully expired
// part is dropped whole, while the part straddling the cutoff is rewritten to shed its old rows (so
// its prefix changes).
func retentionDropsExpiredAndRewritesStraddler(t *testing.T, k Kind) {
	t.Helper()

	ctx := context.Background()
	e := k.open(t, backend.Memory())

	e.Append(t, api(100, 1))
	require.NoError(t, e.Flush(ctx))

	e.Append(t, api(400, 4), api(900, 9))
	require.NoError(t, e.Flush(ctx))

	before := partIDs(e)
	require.Len(t, before, 2)

	require.NoError(t, e.Merge(ctx, 500))

	after := partIDs(e)
	require.Len(t, after, 1)
	assert.NotContains(t, before, after[0], "the straddling part must be rewritten, not kept")
	assert.Equal(t, []Row{api(900, 9)}, rows(t, e, apiStream), "ts=100 and ts=400 are both past the cutoff")
}

// retentionDropReclaimsObjects: the drop is a real reclaim, not just a manifest edit. The retired
// part's backend objects are deleted once no reader holds it.
func retentionDropReclaimsObjects(t *testing.T, k Kind) {
	t.Helper()

	ctx := context.Background()
	be := backend.Memory()
	e := k.open(t, be)

	e.Append(t, api(100, 1))
	require.NoError(t, e.Flush(ctx))
	e.Append(t, api(900, 9))
	require.NoError(t, e.Flush(ctx))

	expired := partIDs(e)[0]

	keys, err := be.List(ctx, expired)
	require.NoError(t, err)
	require.NotEmpty(t, keys, "the part must have objects before the drop")

	require.NoError(t, e.Merge(ctx, 500))

	keys, err = be.List(ctx, expired)
	require.NoError(t, err)
	assert.Empty(t, keys, "the dropped part's objects must be reclaimed")
}

// retentionDropSurvivesRestart: the drop is durable. The committed bucket index no longer names the
// expired part, so a reload does not resurrect it.
func retentionDropSurvivesRestart(t *testing.T, k Kind) {
	t.Helper()

	ctx := context.Background()
	be := backend.Memory()
	e := k.open(t, be)

	e.Append(t, api(100, 1))
	require.NoError(t, e.Flush(ctx))
	e.Append(t, api(900, 9))
	require.NoError(t, e.Flush(ctx))
	require.NoError(t, e.Merge(ctx, 500))

	want := partIDs(e)

	reopened := k.open(t, be)
	require.NoError(t, reopened.LoadParts(ctx))

	assert.Equal(t, want, partIDs(reopened))
	assert.Equal(t, []Row{api(900, 9)}, rows(t, reopened, apiStream))
}

// retentionDropIsNotIdle: a cycle that only dropped parts is not a no-op. The idle counter drives the
// merge selector's stranding escape, and a productive cycle must reset it rather than advance it.
func retentionDropIsNotIdle(t *testing.T, k Kind) {
	t.Helper()

	ctx := context.Background()
	e := k.open(t, backend.Memory())

	e.Append(t, api(100, 1))
	require.NoError(t, e.Flush(ctx))

	// A single part, fully expired: nothing is left to select afterwards, so this cycle's only work
	// is the drop.
	require.NoError(t, e.Merge(ctx, 500))
	assert.Equal(t, 0, e.PartCount())
}

// retentionDropsEveryPart is the degenerate case: a cutoff past every row leaves no part and no rows,
// with nothing rewritten on the way.
func retentionDropsEveryPart(t *testing.T, k Kind) {
	t.Helper()

	ctx := context.Background()
	e := k.open(t, backend.Memory())

	e.Append(t, api(100, 1))
	require.NoError(t, e.Flush(ctx))
	e.Append(t, api(200, 2))
	require.NoError(t, e.Flush(ctx))

	require.NoError(t, e.Merge(ctx, 1000))

	assert.Empty(t, partIDs(e))
	assert.Empty(t, rows(t, e, apiStream))
}

// gateIndexCommit holds the first conditional write of the bucket index until the gate is released.
func gateIndexCommit(be *faultbackend.Backend) *faultbackend.Gate {
	gate := faultbackend.NewGate()
	be.Add(gate.Rule(faultbackend.CompareAndSwap, func(op faultbackend.Op) bool {
		return strings.HasSuffix(op.Key, "/"+bucketindex.Object)
	}))

	return gate
}

// flushRebasesOnARivalIndexCommit is the shared-store case of #392 with the interleaving stated: the
// engine's flush is suspended inside its conditional index write, a rival writer commits a part of
// its own over the same key, and the flush is released. The flush must not overwrite the rival's
// entry: it loses, reloads, and commits an index naming both parts, because the entry is the only
// thing keeping either part reachable.
func flushRebasesOnARivalIndexCommit(t *testing.T, k Kind) {
	t.Helper()

	ctx := context.Background()
	rival := k.Prefix + "/0000009999"

	inner := backend.Memory()
	be := faultbackend.Wrap(inner)
	gate := gateIndexCommit(be)

	e := k.open(t, be)
	e.Append(t, api(100, 1))

	flushed := make(chan error, 1)
	go func() { flushed <- e.Flush(ctx) }()

	gate.Await(t)

	// The rival commits through the raw backend, so it is not itself gated.
	other := &bucketindex.Index{Generation: bucketindex.Generation{Term: 1, Counter: 1}}
	other.Add(bucketindex.Entry{Prefix: rival, MinTime: 1, MaxTime: 2})
	_, err := other.Save(ctx, inner, k.indexKey(), backend.VersionAbsent)
	require.NoError(t, err)

	gate.Release()

	require.NoError(t, <-flushed, "a flush that rebases still lands, and only reports success once it has")

	ix, err := bucketindex.Load(ctx, inner, k.indexKey())
	require.NoError(t, err)

	got := prefixes(ix.Entries)
	require.Contains(t, got, rival, "the rival's part must survive this engine's commit")
	require.Len(t, got, 2, "and this engine's own flushed part is committed alongside it")
}

// flushFailsWhenTheIndexCommitCannotLand covers the other end of the retry loop: a commit that never
// wins must fail the flush. A part whose entry is not in the index is unreachable, and the next open
// sweeps it, so reporting success over that is the data loss.
func flushFailsWhenTheIndexCommitCannotLand(t *testing.T, k Kind) {
	t.Helper()

	// Every conditional index write loses, as on an endlessly contended prefix: the retry loop must
	// give up and say so rather than spin.
	be := faultbackend.Wrap(backend.Memory())
	be.Add(faultbackend.Rule{Kind: faultbackend.CompareAndSwap, Match: k.indexCommit, Lose: true})

	e := k.open(t, be)
	e.Append(t, api(100, 1))

	require.ErrorIs(t, e.Flush(context.Background()), bucketindex.ErrConflict)
}

// rebasedFlushServesTheAdoptedPart is #398: the entries a rebase carries forward go into the
// committed index, so they must go into the part set this engine reads too. An index naming a part
// the engine will not serve is a query that silently misses rows until the next LoadParts.
func rebasedFlushServesTheAdoptedPart(t *testing.T, k Kind) {
	t.Helper()

	ctx := context.Background()

	be := faultbackend.Wrap(backend.Memory())
	a := k.open(t, be)
	b := k.open(t, be)

	a.Append(t, api(100, 1))
	b.Append(t, web(200, 5))

	gate := gateIndexCommit(be)

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

	ix, err := bucketindex.Load(ctx, be, k.indexKey())
	require.NoError(t, err)
	require.Len(t, ix.Entries, 2, "the rebased commit names both writers' parts")

	require.Equal(t, []Row{web(200, 5)}, rows(t, a, "web"), "the rebasing engine serves the part it adopted")
	require.Equal(t, 1, a.PartCount(), "which is still not a part it owns")
	require.Len(t, a.Parts(), 2, "though it is one it serves")
}

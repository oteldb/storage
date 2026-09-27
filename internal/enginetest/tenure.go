package enginetest

import (
	"cmp"
	"context"
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/backend/bucketindex"
	"github.com/oteldb/storage/backend/faultbackend"
	"github.com/oteldb/storage/internal/reproduce"
)

// The tests below are two tenures of one shard over a shared object store: writer A under term 1,
// displaced while its work is in flight, and writer B under term 2. The engine's only view of the
// claim is its Term source, so a displaced writer here is exactly a displaced node's engine.

const (
	displacedTerm = 1
	ownerTerm     = 2

	// stalledMergeTimeout only fires on a broken test: a released merge returns promptly.
	stalledMergeTimeout = 30 * time.Second
)

func termOf(term uint64) func() uint64 { return func() uint64 { return term } }

func (k Kind) openTenure(t *testing.T, be backend.Backend, term uint64, capBytes int64) Engine {
	t.Helper()

	e := k.Open(t, Config{Backend: be, Term: termOf(term), MergeCapBytes: capBytes})
	require.NoError(t, e.LoadParts(context.Background()))

	return e
}

// stallMerge starts force-merging e and holds it at its index commit. The returned func lets the
// commit proceed and waits for the merge's result.
func stallMerge(t *testing.T, e Engine, gate *faultbackend.Gate) (commit func() error) {
	t.Helper()

	merged := make(chan error, 1)

	go func() { merged <- e.ForceMerge(context.Background()) }()

	gate.Await(t)

	return func() error {
		gate.Release()

		select {
		case err := <-merged:
			return err
		case <-time.After(stalledMergeTimeout):
			t.Fatal("the released merge never returned")

			return nil
		}
	}
}

func sortedRows(t *testing.T, e Engine) []Row {
	t.Helper()

	out := rows(t, e, apiStream)
	slices.SortFunc(out, func(a, b Row) int { return cmp.Compare(a.Ts, b.Ts) })

	return out
}

// requireStoredOnce requires every row once: in the index, where no two live parts may share a row by
// lineage, and in a read by a fresh node. The metric read alone would pass, since its merge by series
// and timestamp hides a duplicated sample.
func (k Kind) requireStoredOnce(t *testing.T, be backend.Backend, want []Row) {
	t.Helper()

	ix := k.loadIndex(t, be)
	lineage := bucketindex.LineageOf(ix.Entries)

	for i := range ix.Entries {
		a := &ix.Entries[i]
		t.Logf("live %s blocks=%v level=%d", a.Prefix, a.Blocks, a.Level)

		for j := i + 1; j < len(ix.Entries); j++ {
			b := &ix.Entries[j]
			assert.Falsef(t, lineage.Overlaps(*a, *b), "live parts %s %v and %s %v share rows",
				a.Prefix, a.Blocks, b.Prefix, b.Blocks)
		}
	}

	r := k.Open(t, Config{Backend: be})
	require.NoError(t, r.LoadPartsReadOnly(context.Background()))
	assert.Equal(t, want, sortedRows(t, r), "every row is read once after both tenures merged")
}

// splitBrainMergeSameInputs: A's merge of {P1,P2} outlives its lease, B takes the shard and merges
// the same two parts, then A's merge commits. Both outputs cover blocks 1-2 at level 1.
func splitBrainMergeSameInputs(t *testing.T, k Kind) {
	t.Helper()
	reproduce.Unfixed(t, 725, "a rebased merge adopts the rival's output as foreign and commits its own "+
		"beside it, so both outputs of the same inputs stay live under one block identity")

	ctx := context.Background()
	inner := backend.Memory()
	aBe := faultbackend.Wrap(inner)
	want := []Row{api(100, 1), api(200, 2)}

	a := k.openTenure(t, aBe, displacedTerm, 0)
	k.flushEach(t, a, inner, want...)

	commitA := stallMerge(t, a, gateIndexCommit(aBe))

	b := k.openTenure(t, inner, ownerTerm, 0)
	require.NoError(t, b.ForceMerge(ctx))
	require.Equal(t, 1, b.PartCount(), "the new owner merged both parts")

	require.NoError(t, commitA())

	k.requireStoredOnce(t, inner, want)
}

// splitBrainMergeOverlappingInputs: A merges {P1,P2} past its lease while B, which sees P1 as sealed
// under its smaller merge cap, flushes P3 and merges {P2,P3}. P2's rows are in both outputs.
func splitBrainMergeOverlappingInputs(t *testing.T, k Kind) {
	t.Helper()
	reproduce.Unfixed(t, 725, "a rebased merge adopts the rival's output as foreign and commits its own "+
		"beside it, so the input both merged is live twice")

	ctx := context.Background()
	inner := backend.Memory()
	aBe := faultbackend.Wrap(inner)

	big := make([]Row, 0, 200)
	for i := range int64(200) {
		big = append(big, api(1000+i, i))
	}

	a := k.openTenure(t, aBe, displacedTerm, 0)
	a.Append(t, big...)
	require.NoError(t, a.Flush(ctx))

	a.Append(t, api(2000, 2000))
	require.NoError(t, a.Flush(ctx))

	parts := a.Parts()
	require.Len(t, parts, 2)

	commitA := stallMerge(t, a, gateIndexCommit(aBe))

	b := k.openTenure(t, inner, ownerTerm, parts[0].Bytes)
	b.Append(t, api(3000, 3000))
	require.NoError(t, b.Flush(ctx))

	before := b.Parts()
	require.NoError(t, b.Merge(ctx, 0))

	p3 := removedParts(before, parts)
	require.Len(t, p3, 1)
	require.ElementsMatch(t, []Part{parts[1], p3[0]}, removedParts(before, b.Parts()),
		"the new owner merges P2 and P3, leaving P1 sealed")

	require.NoError(t, commitA())

	want := append(slices.Clone(big), api(2000, 2000), api(3000, 3000))
	k.requireStoredOnce(t, inner, want)
}

// displacedWriterKeepsItsTerm: A's commit loses the CAS to B's and rebases. The rebase must not
// hand A the term B's index carries, or A's commit supersedes the owner's tenure with its own write.
func displacedWriterKeepsItsTerm(t *testing.T, k Kind) {
	t.Helper()
	reproduce.Unfixed(t, 725, "a rebase adopts the rival's higher generation and Generation.Next keeps its term, "+
		"so the displaced writer commits under the new owner's term")

	ctx := context.Background()
	be := backend.Memory()

	a := k.openTenure(t, be, displacedTerm, 0)
	a.Append(t, api(100, 1))
	require.NoError(t, a.Flush(ctx))

	b := k.openTenure(t, be, ownerTerm, 0)
	b.Append(t, api(200, 2))
	require.NoError(t, b.Flush(ctx))

	owner := k.loadIndex(t, be).Generation
	require.Equal(t, uint64(ownerTerm), owner.Term)

	a.Append(t, api(300, 3))
	_ = a.Flush(ctx)

	got := k.loadIndex(t, be).Generation
	t.Logf("owner committed %+v, index after the displaced flush %+v", owner, got)
	assert.Equal(t, owner, got, "the displaced writer never commits under the owner's term")
}

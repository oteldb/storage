package profile

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/recordengine"
	"github.com/oteldb/storage/signal"
	"github.com/oteldb/storage/wal"
)

// attrStackBatch is [leafStackBatch] under a profile attribute: a distinct value is a distinct stream.
func attrStackBatch(leaf, attr string, ts int64) Profiles {
	pd := leafStackBatch("api", leaf, ts)
	d := &pd.Dictionary
	pd.Resources[0].Scopes[0].Profiles[0].AttributeIndices = []int32{d.AddAttribute(KeyValueAndUnit{
		KeyStrindex: d.InternString([]byte("profile.attr")),
		Value:       signal.StringValue([]byte(attr)),
	})}

	return pd
}

// twoLeafBatch is one stream at ts with two samples, on leaf→main and on second→main.
func twoLeafBatch(leaf, second string, ts int64) Profiles {
	pd := leafStackBatch("api", leaf, ts)
	d := &pd.Dictionary
	loc := d.AddLocation(Location{Lines: []Line{{FunctionIndex: d.AddFunction(Function{NameStrindex: d.InternString([]byte(second))})}}})
	pr := &pd.Resources[0].Scopes[0].Profiles[0]
	s := pr.AddSample()
	s.StackIndex = d.AddStack(loc, d.Stacks[pr.Samples[0].StackIndex].LocationIndices[1])
	s.Values = []int64{ts}

	return pd
}

// partStacks returns the stack ids in every part's symbol sidecars.
func partStacks(t *testing.T, eng *recordengine.Engine) map[string]struct{} {
	t.Helper()

	rd := eng.ReadSide(0, 0, func(recordengine.SideStore) {})
	defer rd.Release()

	out := map[string]struct{}{}

	for _, p := range rd.Parts {
		stored, err := p.Load(t.Context())
		require.NoError(t, err)

		tables, err := DecodeTables(stored)
		require.NoError(t, err)

		for id := range tables.t.t[tableStacks] {
			out[string(id.AppendBinary(nil))] = struct{}{}
		}
	}

	return out
}

// storedStacks returns the stack ids the engine's rows reference.
func storedStacks(t *testing.T, eng *recordengine.Engine) map[string]struct{} {
	t.Helper()

	it, err := eng.Fetch(t.Context(), fetch.Request{Signal: signal.Profile, End: 1 << 62, Projection: []string{ColStackID}})
	require.NoError(t, err)

	batches, err := fetch.Drain(t.Context(), it)
	require.NoError(t, err)

	out := map[string]struct{}{}

	for _, b := range batches {
		col, ok := b.Column(ColStackID)
		require.True(t, ok)

		for _, id := range col.Bytes {
			out[string(id)] = struct{}{}
		}
	}

	return out
}

func sortedKeys(m map[string]struct{}) []string { return slices.Sorted(maps.Keys(m)) }

// TestRejectedRecordsLeaveNoSymbols writes streams the primary sheds at MaxSeries and records it
// sheds at the in-flight cap, each on a stack nothing stored references, beside accepted rows. The
// parts the primary flushes, and those a promoted replica flushes, must hold exactly the stacks
// stored rows reference: a rejected record's symbols must not outlive the write.
func TestRejectedRecordsLeaveNoSymbols(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	primary := profileEngine(backend.Memory(), NewSymbolStore())
	replica := profileEngine(backend.Memory(), NewSymbolStore())

	apply := func(pd Profiles, limits recordengine.AppendLimits) recordengine.AppendResult {
		var res recordengine.AppendResult

		Project(&pd, func(b *recordengine.Batch) {
			accepted, r, err := primary.ApplyPrimary(recordengine.EncodeWAL(b), limits)
			require.NoError(t, err)
			require.NoError(t, replica.ApplyReplicated(accepted))

			res.Accepted += r.Accepted
			res.RejectedCardinality += r.RejectedCardinality
			res.RejectedBytes += r.RejectedBytes
		})

		return res
	}

	cardinality := recordengine.AppendLimits{MaxSeries: 1}
	require.Equal(t, 1, apply(attrStackBatch("kept", "a", 1), cardinality).Accepted)

	for i := range 5 {
		res := apply(attrStackBatch(fmt.Sprintf("shed-%d", i), fmt.Sprintf("b-%d", i), 2), cardinality)
		require.Equal(t, 1, res.RejectedCardinality)
	}

	res := apply(twoLeafBatch("kept", "over-cap", 3), recordengine.AppendLimits{MaxInFlightBytes: 1})
	require.Equal(t, 0, res.Accepted, "the head is already over the cap")
	require.Equal(t, 2, res.RejectedBytes)

	require.NoError(t, primary.Flush(ctx))

	want := storedStacks(t, primary)
	require.Len(t, want, 1)
	require.Equal(t, sortedKeys(want), sortedKeys(partStacks(t, primary)))

	require.NoError(t, replica.Flush(ctx))
	require.Equal(t, sortedKeys(want), sortedKeys(partStacks(t, replica)), "a promoted replica flushes the same symbols")

	kept := []stackCase{{leaf: "kept", id: []byte(sortedKeys(want)[0])}}
	requireResolves(t, engineResolver(t, primary), kept)
	requireResolves(t, engineResolver(t, replica), kept)
}

// TestPartiallyRejectedBatchLeavesNoSymbols is the single-node path, with and without a WAL: a batch
// whose first record is accepted and second shed at the in-flight cap keeps only the first record's
// stack.
func TestPartiallyRejectedBatchLeavesNoSymbols(t *testing.T) {
	t.Parallel()

	for _, logged := range []bool{false, true} {
		t.Run(fmt.Sprintf("wal=%v", logged), func(t *testing.T) {
			t.Parallel()

			cfg := recordengine.Config{Schema: Schema, Backend: backend.Memory(), Prefix: "p/profiles", SideStore: NewSymbolStore()}
			if logged {
				w, err := wal.Create(t.TempDir(), 0)
				require.NoError(t, err)
				t.Cleanup(func() { _ = w.Close() })

				cfg.WAL = w
			}

			eng := recordengine.New(cfg)

			pd := twoLeafBatch("kept", "over-cap", 1)
			Project(&pd, func(b *recordengine.Batch) {
				res, err := eng.AppendBatch(b, recordengine.AppendLimits{MaxInFlightBytes: 1})
				require.NoError(t, err)
				require.Equal(t, 1, res.Accepted)
				require.Equal(t, 1, res.RejectedBytes)
			})

			require.NoError(t, eng.Flush(t.Context()))

			want := storedStacks(t, eng)
			require.Len(t, want, 1)
			require.Equal(t, sortedKeys(want), sortedKeys(partStacks(t, eng)))
			requireResolves(t, engineResolver(t, eng), []stackCase{{leaf: "kept", id: []byte(sortedKeys(want)[0])}})
		})
	}
}

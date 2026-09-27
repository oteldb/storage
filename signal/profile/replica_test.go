package profile

import (
	"context"
	"fmt"
	"slices"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/internal/reproduce"
	"github.com/oteldb/storage/recordengine"
	"github.com/oteldb/storage/signal"
)

// leafStackBatch is one aggregated sample of service svc at ts, on the stack leaf→main.
func leafStackBatch(svc, leaf string, ts int64) Profiles {
	var pd Profiles

	d := &pd.Dictionary
	main := d.AddLocation(Location{Lines: []Line{{FunctionIndex: d.AddFunction(Function{NameStrindex: d.InternString([]byte("main"))})}}})
	fn := d.AddLocation(Location{Lines: []Line{{FunctionIndex: d.AddFunction(Function{NameStrindex: d.InternString([]byte(leaf))})}}})
	st := d.AddStack(fn, main)

	rp := pd.AddResource()
	rp.Resource = svcResource(svc)
	pr := rp.AddScope().AddProfile()
	pr.TimeNanos = ts
	s := pr.AddSample()
	s.StackIndex = st
	s.Values = []int64{ts}

	return pd
}

type stackCase struct {
	leaf string
	id   []byte
}

func profileEngine(be backend.Backend, side *SymbolStore) *recordengine.Engine {
	return recordengine.New(recordengine.Config{Schema: Schema, Backend: be, Prefix: "p/profiles", SideStore: side})
}

func tableIDs(s *SymbolStore) [5][]signal.SeriesID {
	var out [5][]signal.SeriesID
	for i, m := range s.acc.t {
		for id := range m {
			out[i] = append(out[i], id)
		}

		slices.SortFunc(out[i], func(a, b signal.SeriesID) int { return a.Compare(b) })
	}

	return out
}

func tableSizes(s *SymbolStore) [5]int {
	var out [5]int
	for i, m := range s.acc.t {
		out[i] = len(m)
	}

	return out
}

// engineResolver resolves over everything eng holds, as the profile resolver reads it: the live
// accumulator, an in-flight flush and every part.
func engineResolver(t *testing.T, eng *recordengine.Engine) *Resolver {
	t.Helper()

	var head *Tables

	rd := eng.ReadSide(0, 0, func(live recordengine.SideStore) { head = live.(*SymbolStore).Tables() })
	defer rd.Release()

	require.Nil(t, rd.Flushing)

	layers := make([]*Tables, 0, 1+len(rd.Parts))
	layers = append(layers, head)

	for _, p := range rd.Parts {
		stored, err := p.Load(t.Context())
		require.NoError(t, err)

		tables, err := DecodeTables(stored)
		require.NoError(t, err)

		layers = append(layers, tables)
	}

	return NewResolverFrom(layers...)
}

func requireResolves(t *testing.T, r *Resolver, stacks []stackCase) {
	t.Helper()

	for _, s := range stacks {
		frames := r.Resolve(s.id)
		require.Len(t, frames, 2, "stack %s", s.leaf)
		require.Equal(t, s.leaf, frames[0].Function)
		require.Equal(t, "main", frames[1].Function)
	}
}

// TestReplicaSideStoreBoundedByHead drives a non-owning replica through many owner flushes and
// replica refreshes. Its symbol accumulator must hold only what its remaining head references, while
// every stack still resolves on the replica and survives the replica's promotion.
func TestReplicaSideStoreBoundedByHead(t *testing.T) {
	reproduce.Unfixed(t, 704, "a replica's side store keeps every symbol ever replicated to it")
	t.Parallel()

	ctx := context.Background()
	be := backend.Memory()
	primary := profileEngine(be, NewSymbolStore())
	replicaSide := NewSymbolStore()
	replica := profileEngine(be, replicaSide)

	var (
		all  []stackCase
		want = NewSymbolStore()
	)

	record := func(leaf string, pd Profiles, onPrimary, inHead bool) {
		Project(&pd, func(b *recordengine.Batch) {
			all = append(all, stackCase{leaf: leaf, id: slices.Clone(b.Bytes[bStackID][0])})

			payload := recordengine.EncodeWAL(b)
			if onPrimary {
				accepted, _, err := primary.ApplyPrimary(payload, recordengine.AppendLimits{})
				require.NoError(t, err)

				payload = accepted
			}

			require.NoError(t, replica.ApplyReplicated(payload))

			if inHead {
				require.NoError(t, want.Absorb(b.Side))
			}
		})
	}

	// A stream the owner has not flushed yet: absent from every part, so it stays in the replica's
	// head across every refresh while the deltas that carried its symbols are long gone.
	record("pending", leafStackBatch("pending", "pending", 1), false, true)

	const rounds = 50
	for i := range rounds {
		leaf := fmt.Sprintf("fn-%d", i)
		record(leaf, leafStackBatch("api", leaf, int64(i+1)), true, false)
		require.NoError(t, primary.Flush(ctx))
		require.NoError(t, replica.RefreshReplica(ctx))
	}

	record("unflushed", leafStackBatch("api", "unflushed", rounds+1), true, true)
	require.NoError(t, replica.RefreshReplica(ctx))
	require.Equal(t, 2, replica.HeadRecordCount())

	require.Equal(t, tableSizes(want), tableSizes(replicaSide),
		"the replica's accumulator holds only the symbols its head references")
	require.Equal(t, tableIDs(want), tableIDs(replicaSide))

	requireResolves(t, engineResolver(t, replica), all)

	require.NoError(t, replica.Flush(ctx))

	promoted := profileEngine(be, NewSymbolStore())
	require.NoError(t, promoted.LoadParts(ctx))

	requireResolves(t, engineResolver(t, promoted), all)
}

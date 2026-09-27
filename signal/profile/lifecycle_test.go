package profile

import (
	"slices"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/recordengine"
	"github.com/oteldb/storage/signal"
)

func appendProfiles(t *testing.T, eng *recordengine.Engine, pd Profiles) {
	t.Helper()

	Project(&pd, func(b *recordengine.Batch) {
		res, err := eng.AppendBatch(b, recordengine.AppendLimits{})
		require.NoError(t, err)
		require.Equal(t, b.Len(), res.Accepted)
	})
}

// partTableIDs returns the ids of every symbol table across the engine's parts.
func partTableIDs(t *testing.T, eng *recordengine.Engine) [5][]signal.SeriesID {
	t.Helper()

	rd := eng.ReadSide(0, 0, func(recordengine.SideStore) {})
	defer rd.Release()

	var out [5][]signal.SeriesID

	for _, p := range rd.Parts {
		stored, err := p.Load(t.Context())
		require.NoError(t, err)

		tables, err := DecodeTables(stored)
		require.NoError(t, err)

		for i, m := range tables.t.t {
			for id := range m {
				out[i] = append(out[i], id)
			}
		}
	}

	for i := range out {
		slices.SortFunc(out[i], func(a, b signal.SeriesID) int { return a.Compare(b) })
		out[i] = slices.Compact(out[i])
	}

	return out
}

// onlyTables is what an engine that ingested and flushed pd alone persists.
func onlyTables(t *testing.T, pd Profiles) [5][]signal.SeriesID {
	t.Helper()

	eng := profileEngine(backend.Memory(), NewSymbolStore())
	appendProfiles(t, eng, pd)
	require.NoError(t, eng.Flush(t.Context()))

	return partTableIDs(t, eng)
}

func TestResetDropsSideStore(t *testing.T) {
	t.Parallel()

	side := NewSymbolStore()
	eng := profileEngine(backend.Memory(), side)

	appendProfiles(t, eng, leafStackBatch("api", "a", 1))
	require.NoError(t, eng.Reset(t.Context()))
	require.Equal(t, [5]int{}, tableSizes(side), "Reset empties the live store")

	appendProfiles(t, eng, leafStackBatch("api", "b", 2))

	want := NewSymbolStore()
	pd := leafStackBatch("api", "b", 2)
	Project(&pd, func(b *recordengine.Batch) { require.NoError(t, want.Absorb(b.Side)) })
	require.Equal(t, tableIDs(want), tableIDs(side), "the live store holds only what was written after Reset")

	require.NoError(t, eng.Flush(t.Context()))
	require.Equal(t, onlyTables(t, leafStackBatch("api", "b", 2)), partTableIDs(t, eng))
}

// TestRetentionMergeDropsExpiredSymbols rewrites a part that straddles the retention cutoff: the
// replacement keeps the surviving row's stack and everything it reaches, and nothing only the expired
// row referenced.
func TestRetentionMergeDropsExpiredSymbols(t *testing.T) {
	t.Parallel()

	eng := profileEngine(backend.Memory(), NewSymbolStore())
	appendProfiles(t, eng, leafStackBatch("api", "expired", 1))
	appendProfiles(t, eng, leafStackBatch("api", "kept", 100))
	require.NoError(t, eng.Flush(t.Context()))
	require.NoError(t, eng.Merge(t.Context(), 50))

	require.Equal(t, 1, eng.PartCount())
	require.Equal(t, onlyTables(t, leafStackBatch("api", "kept", 100)), partTableIDs(t, eng))

	stacks := storedStacks(t, eng)
	require.Len(t, stacks, 1)
	requireResolves(t, engineResolver(t, eng), []stackCase{{leaf: "kept", id: []byte(sortedKeys(stacks)[0])}})
}

package engine

import (
	"context"
	"slices"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/backend/bucketindex"
	backendfile "github.com/oteldb/storage/backend/file"
	"github.com/oteldb/storage/index/series"
	"github.com/oteldb/storage/signal"
)

func identityCacheInput(n int) identitySnapshot {
	registry := series.New()
	refs := make([]uint32, n)
	for i := range n {
		id := registry.Add(benchSeries(i))
		refs[i], _ = registry.Ref(id)
	}
	view := registry.View()
	slices.SortFunc(refs, func(a, b uint32) int { return view.ID(int(a)).Compare(view.ID(int(b))) })
	return identitySnapshot{view: view, refs: refs}
}

func TestVerifiedIdentityCacheBudgetAndLowWordCollision(t *testing.T) {
	t.Parallel()
	const budget = 4096
	cache := newVerifiedIdentityCache(budget)
	id, collision, missing := signal.SeriesID{Hi: 1, Lo: 1}, signal.SeriesID{Hi: 2, Lo: 1}, signal.SeriesID{Hi: 3, Lo: 1}
	cache.putIDs(1, []signal.SeriesID{id})
	cache.putIDs(2, []signal.SeriesID{collision})
	for i := range 1000 {
		cache.putIDs(1, []signal.SeriesID{{Lo: uint64(i + 2)}})
		require.LessOrEqual(t, cache.sizeBytes(), int64(budget))
	}
	registry := series.New()
	for _, seriesID := range []signal.SeriesID{id, collision, missing} {
		registry.AddEntry(series.Entry{ID: seriesID, Series: benchSeries(1)})
	}
	entries := identitySnapshot{view: registry.View(), refs: []uint32{0, 1, 2}}
	known := make([]bool, len(entries.refs))
	snapshot := &seriesSnapshot{tokens: map[uint64]struct{}{1: {}, 2: {}}}
	require.Equal(t, 2, cache.match(snapshot, entries, known, 0))
	require.Equal(t, []bool{true, true, false}, known, "a low-word hit needs the matching high word")

	clear(known)
	snapshot = &seriesSnapshot{tokens: map[uint64]struct{}{2: {}}}
	require.Equal(t, 1, cache.match(snapshot, entries, known, 0))
	require.Equal(t, []bool{false, true, false}, known)
	require.Nil(t, newVerifiedIdentityCache(0))
}

func TestOlderRootCannotReplaceNewerMembership(t *testing.T) {
	t.Parallel()
	cache := newVerifiedIdentityCache(4096)
	registry := series.New()
	id := registry.Add(benchSeries(1))
	entries := identitySnapshot{view: registry.View(), refs: []uint32{0}}
	cache.putIDs(2, []signal.SeriesID{id})
	cache.putIDs(1, []signal.SeriesID{id})
	known := make([]bool, len(entries.refs))
	snapshot := &seriesSnapshot{tokens: map[uint64]struct{}{2: {}}}
	require.Equal(t, 1, cache.match(snapshot, entries, known, 0))
	require.Equal(t, []bool{true}, known)
}

func TestVerifiedIdentityAcrossHeadsAndCatalogChanges(t *testing.T) {
	t.Parallel()
	files, err := backendfile.New(t.TempDir())
	require.NoError(t, err)
	reads := &cpuFixtureFile{File: files}
	e := separateCatalogEngine(reads)
	t.Cleanup(func() { require.NoError(t, e.CloseWAL()) })
	for i := range 32 {
		_, err = e.Append(benchSeries(i), 100, 1)
		require.NoError(t, err)
	}
	require.NoError(t, e.Flush(t.Context()))
	input := identityCacheInput(32)
	reads.Reset()
	known, _, err := e.seriesTable.knownIdentities(t.Context(), input, e.verifiedIdentities)
	require.NoError(t, err)
	require.NotContains(t, known, false)
	require.Zero(t, reads.Bytes(), "a completed root write seeds exact ID membership")
	for i := range 32 {
		_, err = e.Append(benchSeries(i), 200, 1)
		require.NoError(t, err)
	}
	require.NoError(t, e.Flush(t.Context()))
	require.Len(t, e.seriesTable.roots, 1)
	// The old descriptor cannot authorize a different root, even with the same ID.
	other := catalogRoot(t, reads, "other", 99)
	require.NoError(t, e.installSeriesRootsLocked(t.Context(), []bucketindex.SeriesRoot{other}))
	reads.Reset()
	known, _, err = e.seriesTable.knownIdentities(t.Context(), input, e.verifiedIdentities)
	require.NoError(t, err)
	require.NotContains(t, known, false, "adding an unrelated root preserves the original membership proof")
	require.Zero(t, reads.Bytes(), "the unchanged root should not be read again")
	// A new snapshot with only the unrelated root must not reuse existence from the old root.
	handle := e.seriesTable.roots[slices.IndexFunc(e.seriesTable.roots, func(h *seriesRootHandle) bool { return h.binding == other })]
	onlyOther := newSeriesSnapshot([]*seriesRootHandle{handle})
	defer onlyOther.release()
	known, _, err = onlyOther.knownIdentities(t.Context(), input, e.verifiedIdentities)
	require.NoError(t, err)
	require.NotContains(t, known, true)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	_, _, err = e.seriesTable.knownIdentities(ctx, input, e.verifiedIdentities)
	require.ErrorIs(t, err, context.Canceled)
}

func TestDescriptorMembershipChecksOnlyID(t *testing.T) {
	t.Parallel()
	e := separateCatalogEngine(backend.Memory())
	t.Cleanup(func() { require.NoError(t, e.CloseWAL()) })
	s := benchSeries(1)
	_, err := e.Append(s, 100, 1)
	require.NoError(t, err)
	require.NoError(t, e.Flush(t.Context()))
	// A mismatched descriptor here catches any accidental materialization on the lookup path.
	registry := series.New()
	registry.AddEntry(series.Entry{ID: s.Hash(), Series: benchSeries(2)})
	input := identitySnapshot{view: registry.View(), refs: []uint32{0}}
	for _, cache := range []*verifiedIdentityCache{e.verifiedIdentities, nil} {
		known, _, err := e.seriesTable.knownIdentities(t.Context(), input, cache)
		require.NoError(t, err)
		require.Equal(t, []bool{true}, known)
	}
}

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

func TestVerifiedIdentityCacheOwnershipAndBudget(t *testing.T) {
	t.Parallel()
	const budget = 4096
	cache := newVerifiedIdentityCache(budget)
	generation := uint64(1)
	key := signal.SeriesID{Lo: 1}
	raw := []byte("canonical")
	cache.put(generation, key, raw)
	raw[0] = 'X'
	got, ok := cache.get(generation, key)
	require.True(t, ok)
	require.Equal(t, "canonical", string(got))
	for i := range 1000 {
		cache.put(generation, signal.SeriesID{Lo: uint64(i + 2)}, raw)
		require.LessOrEqual(t, cache.sizeBytes(), int64(budget))
	}
	_, ok = cache.get(generation, key)
	require.True(t, ok, "a saturated generation does not churn existing proofs")
	key.Lo = 2000
	cache.put(generation, key, make([]byte, budget))
	_, ok = cache.get(generation, key)
	require.False(t, ok, "an oversized identity bypasses the cache")
	require.Nil(t, newVerifiedIdentityCache(0))
}

func TestVerifiedIdentityOldGenerationCannotReplaceNewProofs(t *testing.T) {
	t.Parallel()
	cache := newVerifiedIdentityCache(4096)
	id := signal.SeriesID{Lo: 1}
	require.True(t, cache.put(2, id, []byte("new")))
	require.False(t, cache.put(1, id, []byte("old")))
	raw, ok := cache.get(2, id)
	require.True(t, ok)
	require.Equal(t, "new", string(raw))
	_, ok = cache.get(1, id)
	require.False(t, ok)
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
	known, err := e.seriesTable.knownIdentities(t.Context(), input, e.verifiedIdentities)
	require.NoError(t, err)
	require.NotContains(t, known, false)
	require.Zero(t, reads.Bytes(), "a completed root write seeds exact canonical verification")
	for i := range 32 {
		_, err = e.Append(benchSeries(i), 200, 1)
		require.NoError(t, err)
	}
	require.NoError(t, e.Flush(t.Context()))
	require.Len(t, e.seriesTable.roots, 1)
	// The old descriptor cannot authorize a different root, even with the same ID.
	other := catalogRoot(t, reads, "other", 99)
	require.NoError(t, e.installSeriesRootsLocked(t.Context(), []bucketindex.SeriesRoot{other}))
	known, err = e.seriesTable.knownIdentities(t.Context(), input, e.verifiedIdentities)
	require.NoError(t, err)
	require.NotContains(t, known, false, "adding an unrelated root preserves the original proof")
	// A new snapshot with only the unrelated root must not reuse existence from the old root.
	handle := e.seriesTable.roots[slices.IndexFunc(e.seriesTable.roots, func(h *seriesRootHandle) bool { return h.binding == other })]
	onlyOther := newSeriesSnapshot([]*seriesRootHandle{handle})
	defer onlyOther.release()
	known, err = onlyOther.knownIdentities(t.Context(), input, e.verifiedIdentities)
	require.NoError(t, err)
	require.NotContains(t, known, true)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	_, err = e.seriesTable.knownIdentities(ctx, input, e.verifiedIdentities)
	require.ErrorIs(t, err, context.Canceled)
}

func TestVerifiedIdentityRejectsCanonicalConflict(t *testing.T) {
	t.Parallel()
	e := separateCatalogEngine(backend.Memory())
	t.Cleanup(func() { require.NoError(t, e.CloseWAL()) })
	s := benchSeries(1)
	_, err := e.Append(s, 100, 1)
	require.NoError(t, err)
	require.NoError(t, e.Flush(t.Context()))
	// Deliberately force an ID collision to exercise the canonical check independently of hashing.
	registry := series.New()
	registry.AddEntry(series.Entry{ID: s.Hash(), Series: benchSeries(2)})
	input := identitySnapshot{view: registry.View(), refs: []uint32{0}}
	_, err = e.seriesTable.knownIdentities(t.Context(), input, e.verifiedIdentities)
	require.ErrorContains(t, err, "canonical identity conflict")
	_, err = e.seriesTable.knownIdentities(t.Context(), input)
	require.ErrorContains(t, err, "canonical identity conflict")
}

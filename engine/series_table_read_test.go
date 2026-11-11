package engine

import (
	"slices"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/backend/bucketindex"
	"github.com/oteldb/storage/index/postings"
	"github.com/oteldb/storage/internal/seriesindex"
	"github.com/oteldb/storage/internal/seriestable"
	"github.com/oteldb/storage/internal/watermark"
	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/readbudget"
	"github.com/oteldb/storage/signal"
)

func sharedReadFixture(t *testing.T, missing bool) (*part, backend.Backend, []signal.Series) {
	t.Helper()
	be := backend.Memory()
	e := New(Config{Backend: be, Prefix: "numeric", AggregateStats: true})
	t.Cleanup(func() { require.NoError(t, e.Close(t.Context())) })
	identities := []signal.Series{benchSeries(0), benchSeries(1), benchSeries(2)}
	for i := range 2 {
		_, err := e.Append(identities[i], int64(100+i), float64(2+i))
		require.NoError(t, err)
	}
	require.NoError(t, e.Flush(t.Context()))
	p := e.parts[0]
	covered := []watermark.Entry{{ID: identities[0].Hash(), Max: 1000}, {ID: identities[1].Hash(), Max: 1001}}
	slices.SortFunc(covered, func(a, b watermark.Entry) int { return a.ID.Compare(b.ID) })
	require.NoError(t, be.Write(t.Context(), watermark.Key(p.prefix), watermark.Encode(nil, covered)))
	require.NoError(t, be.Delete(t.Context(), identityKey(p.prefix)))
	descriptors := slices.Clone(identities)
	if missing {
		descriptors = descriptors[1:]
	}
	slices.SortFunc(descriptors, func(a, b signal.Series) int { return a.Hash().Compare(b.Hash()) })
	roots := make([]*seriesRootHandle, 0, 2)
	for _, prefix := range []string{"metadata/a", "metadata/b"} {
		var builder seriestable.Builder
		for i := range descriptors {
			require.NoError(t, builder.Add(descriptors[i].Hash(), descriptors[i]))
		}
		run, err := seriestable.Write(t.Context(), be, prefix, &builder)
		require.NoError(t, err)
		roots = append(roots, &seriesRootHandle{
			binding: bucketindex.SeriesRoot{Prefix: prefix, ManifestDigest: run.Digest()}, run: run,
		})
	}
	p.sharedSeries = newSeriesSnapshot(roots)
	return p, be, identities
}

func TestSharedSeriesReadsUseNumericMetadata(t *testing.T) {
	t.Parallel()
	p, _, identities := sharedReadFixture(t, false)
	require.NoError(t, p.validateSharedSeriesIDs(t.Context()))
	var got []signal.SeriesID
	require.NoError(t, p.visitDiskSeries(t.Context(), nil, func(id signal.SeriesID, raw []byte, meta seriesindex.Metadata) error {
		identity, used, err := signal.DecodeSeries(raw)
		require.NoError(t, err)
		require.Equal(t, len(raw), used)
		require.Equal(t, id, identity.Hash())
		i := slices.IndexFunc(identities, func(s signal.Series) bool { return s.Hash() == id })
		require.Less(t, i, 2, "global-only descriptors must not appear in the numeric part")
		require.EqualValues(t, 1000+i, meta.Covered)
		require.True(t, meta.HasStats)
		require.EqualValues(t, 1, meta.Stats.Count)
		require.InDelta(t, 2+i, meta.Stats.Sum, 0)
		got = append(got, id)
		return nil
	}))
	require.ElementsMatch(t, []signal.SeriesID{identities[0].Hash(), identities[1].Hash()}, got)
	matcher := fetch.Matcher{Name: []byte("instance"), Match: func(v signal.Value) bool {
		return v.Equal(signal.StringValue([]byte("1")))
	}}
	got = nil
	require.NoError(t, p.visitDiskSeries(t.Context(), []fetch.Matcher{matcher}, func(id signal.SeriesID, _ []byte, _ seriesindex.Metadata) error {
		got = append(got, id)
		return nil
	}))
	require.Equal(t, []signal.SeriesID{identities[1].Hash()}, got)
	for i := range 2 {
		s, meta, found, err := p.lookupDiskSeries(t.Context(), identities[i].Hash())
		require.NoError(t, err)
		require.True(t, found)
		require.True(t, s.Equal(identities[i]))
		require.EqualValues(t, 1000+i, meta.Covered)
	}
	_, _, found, err := p.lookupDiskSeries(t.Context(), identities[2].Hash())
	require.NoError(t, err)
	require.False(t, found)
	ids, err := p.diskSeriesIDs(t.Context())
	require.NoError(t, err)
	actual, err := postings.ToSlice(ids)
	require.NoError(t, err)
	require.ElementsMatch(t, []signal.SeriesID{identities[0].Hash(), identities[1].Hash()}, actual)
}

func TestSharedSeriesMissingDescriptorFailsClosed(t *testing.T) {
	t.Parallel()
	p, _, identities := sharedReadFixture(t, true)
	require.ErrorIs(t, p.validateSharedSeriesIDs(t.Context()), seriesindex.ErrCorrupt)
	identity, _, found, err := p.lookupDiskSeries(t.Context(), identities[0].Hash())
	require.ErrorIs(t, err, seriesindex.ErrCorrupt)
	require.False(t, found)
	require.Empty(t, identity)
}

func TestSharedSeriesRequiresCoveredWatermarks(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name string
		raw  []byte
	}{
		{name: "missing"},
		{name: "corrupt", raw: []byte("bad watermark")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			p, be, identities := sharedReadFixture(t, false)
			key := watermark.Key(p.prefix)
			if tc.raw == nil {
				require.NoError(t, be.Delete(t.Context(), key))
			} else {
				require.NoError(t, be.Write(t.Context(), key, tc.raw))
			}
			require.Error(t, p.validateSharedSeriesIDs(t.Context()))
			_, _, err := p.diskSeriesMetadata(t.Context(), identities[0].Hash())
			require.Error(t, err, "stored timestamps cannot replace an unavailable covered watermark")
		})
	}
}

func TestSharedSeriesReadBudget(t *testing.T) {
	t.Parallel()
	p, _, _ := sharedReadFixture(t, false)
	workspace, err := p.diskSeriesWorkspace(t.Context(), 1)
	require.NoError(t, err)
	require.Positive(t, workspace)
	budget := readbudget.New(1)
	ctx := readbudget.With(t.Context(), budget)
	require.ErrorIs(t, p.validateSharedSeriesIDs(ctx), readbudget.ErrExceeded)
	require.Zero(t, budget.Peak())
}

func TestSharedSeriesQueryResolvesPredicatesOnce(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name         string
		withIdentity bool
		countOnly    bool
	}{
		{name: "fetch", withIdentity: true},
		{name: "count", countOnly: true},
		{name: "grouped count", withIdentity: true, countOnly: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			be := backend.Memory()
			e := New(Config{Backend: be, Prefix: "shared-query", DiskSeries: true, SeparateSeries: true,
				MetricSeriesChunks: true, AggregateStats: true})
			t.Cleanup(func() { require.NoError(t, e.Close(t.Context())) })
			identity := benchSeries(0)
			for i := range 3 {
				_, err := e.Append(identity, int64(100+i), float64(i))
				require.NoError(t, err)
				if i == 1 {
					_, err := e.Append(benchSeries(1), 101, 8)
					require.NoError(t, err)
				}
				require.NoError(t, e.Flush(t.Context()))
			}
			require.Len(t, e.parts, 3)
			require.Len(t, e.seriesTable.roots, 2)
			require.Zero(t, e.head.series.Len())
			calls := 0
			r := fetch.Request{Start: 0, End: 1000, Matchers: []fetch.Matcher{{Name: []byte("instance"), Match: func(v signal.Value) bool {
				calls++
				return v.Equal(signal.StringValue([]byte("0")))
			}}}}
			require.NoError(t, e.seriesTable.visit(t.Context(), r.Matchers, func(signal.SeriesID, []byte) error { return nil }))
			e.mu.RLock()
			e.head.resolve(r.Matchers)
			e.mu.RUnlock()
			once := calls
			calls = 0
			plan, err := e.planDiskQuery(t.Context(), r, tc.withIdentity, tc.countOnly)
			require.NoError(t, err)
			defer plan.releaseParts()
			require.Positive(t, once)
			require.Equal(t, once, calls, "predicate work equals one root scan across all numeric parts")
			require.Equal(t, []signal.SeriesID{identity.Hash()}, plan.ids)
			if tc.withIdentity {
				require.True(t, plan.series[0].Equal(identity))
			}
			if !tc.countOnly {
				for i, pt := range e.parts {
					meta := plan.diskMetadata[pt][identity.Hash()]
					require.EqualValues(t, 100+i, meta.Covered)
					require.True(t, meta.HasStats)
					require.InDelta(t, i, meta.Stats.Sum, 0)
				}
			}
		})
	}
}

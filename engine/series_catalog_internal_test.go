package engine

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/backend/bucketindex"
	"github.com/oteldb/storage/internal/partid"
	"github.com/oteldb/storage/internal/seriestable"
	"github.com/oteldb/storage/query/fetch"
)

func catalogRoot(t *testing.T, be backend.Backend, prefix string, id int) bucketindex.SeriesRoot {
	t.Helper()
	var builder seriestable.Builder
	s := benchSeries(id)
	require.NoError(t, builder.Add(s.Hash(), s))
	run, err := seriestable.Write(t.Context(), be, prefix, &builder)
	require.NoError(t, err)
	binding := bucketindex.SeriesRoot{Prefix: prefix, ManifestDigest: run.Digest()}
	require.NoError(t, run.Close())
	return binding
}

func separateCatalogEngine(be backend.Backend) *Engine {
	return New(Config{Backend: be, Prefix: "series-catalog", DiskSeries: true, MetricSeriesChunks: true, SeparateSeries: true})
}

func TestSeriesCatalogRequiresOptInBeforeMutation(t *testing.T) {
	t.Parallel()
	for _, operation := range []string{"load", "unclaimed", "replica", "reset"} {
		t.Run(operation, func(t *testing.T) {
			t.Parallel()
			be := backend.Memory()
			root := catalogRoot(t, be, "series-catalog/s-root", 1)
			e := New(Config{Backend: be, Prefix: "series-catalog", DiskSeries: true})
			ix := &bucketindex.Index{SeriesRoots: []bucketindex.SeriesRoot{root}}
			_, err := ix.Save(t.Context(), be, e.indexKey(), "")
			require.NoError(t, err)
			before, err := be.Read(t.Context(), e.indexKey())
			require.NoError(t, err)
			keys, err := be.List(t.Context(), "")
			require.NoError(t, err)
			switch operation {
			case "load":
				err = e.LoadParts(t.Context())
			case "unclaimed":
				err = e.LoadPartsUnclaimed(t.Context())
			case "replica":
				err = e.RefreshReplica(t.Context())
			case "reset":
				err = e.Reset(t.Context())
			}
			require.ErrorContains(t, err, "SeparateSeries")
			after, err := be.Read(t.Context(), e.indexKey())
			require.NoError(t, err)
			require.Equal(t, before, after)
			afterKeys, err := be.List(t.Context(), "")
			require.NoError(t, err)
			require.ElementsMatch(t, keys, afterKeys)
		})
	}
}

func TestSeriesCatalogRequiresFreshSamples(t *testing.T) {
	t.Parallel()
	be := backend.Memory()
	e := separateCatalogEngine(be)
	ix := &bucketindex.Index{Entries: []bucketindex.Entry{{Prefix: "series-catalog/legacy", MinTime: 1, MaxTime: 2}}}
	_, err := ix.Save(t.Context(), be, e.indexKey(), "")
	require.NoError(t, err)
	require.ErrorContains(t, e.LoadParts(t.Context()), "fresh sample catalog")
	require.Empty(t, e.parts)
}

func TestSeriesCatalogRebasePreservesPendingRoots(t *testing.T) {
	t.Parallel()
	be := backend.Memory()
	e := separateCatalogEngine(be)
	t.Cleanup(func() { require.NoError(t, e.CloseWAL()) })
	a := catalogRoot(t, be, "series-catalog/s-a", 1)
	b := catalogRoot(t, be, "series-catalog/s-b", 2)
	c := catalogRoot(t, be, "series-catalog/s-c", 3)
	ix := &bucketindex.Index{SeriesRoots: []bucketindex.SeriesRoot{a}}
	version, err := ix.Save(t.Context(), be, e.indexKey(), "")
	require.NoError(t, err)
	require.NoError(t, e.LoadPartsReadOnly(t.Context()))
	e.mu.Lock()
	err = e.installSeriesRootsLocked(t.Context(), []bucketindex.SeriesRoot{b})
	e.mu.Unlock()
	require.NoError(t, err)
	ix.SeriesRoots, err = bucketindex.MergeSeriesRoots(ix.SeriesRoots, []bucketindex.SeriesRoot{c})
	require.NoError(t, err)
	_, err = ix.Save(t.Context(), be, e.indexKey(), version)
	require.NoError(t, err)
	e.mu.Lock()
	err = e.updateIndexLocked(t.Context())
	e.mu.Unlock()
	require.NoError(t, err)
	stored, _, err := bucketindex.LoadVersioned(t.Context(), be, e.indexKey())
	require.NoError(t, err)
	require.Equal(t, []bucketindex.SeriesRoot{a, b, c}, stored.SeriesRoots)
}

func TestSeriesCatalogFailedLoadPreservesSnapshot(t *testing.T) {
	t.Parallel()
	be := backend.Memory()
	e := separateCatalogEngine(be)
	t.Cleanup(func() { require.NoError(t, e.CloseWAL()) })
	a := catalogRoot(t, be, "series-catalog/s-a", 1)
	b := catalogRoot(t, be, "series-catalog/s-b", 2)
	ix := &bucketindex.Index{SeriesRoots: []bucketindex.SeriesRoot{a}}
	version, err := ix.Save(t.Context(), be, e.indexKey(), "")
	require.NoError(t, err)
	require.NoError(t, e.LoadPartsReadOnly(t.Context()))
	previous := e.seriesTable
	ix.SeriesRoots = []bucketindex.SeriesRoot{a, b}
	ix.Entries = []bucketindex.Entry{{Prefix: "series-catalog/corrupt", MinTime: 1, MaxTime: 2}}
	require.NoError(t, be.Write(t.Context(), "series-catalog/corrupt/manifest", []byte("invalid")))
	_, err = ix.Save(t.Context(), be, e.indexKey(), version)
	require.NoError(t, err)
	require.Error(t, e.LoadPartsReadOnly(t.Context()))
	require.Same(t, previous, e.seriesTable)
	require.Equal(t, []bucketindex.SeriesRoot{a}, e.seriesTable.bindings())
}

func TestSeriesCatalogSweepKeepsPendingRoots(t *testing.T) {
	t.Parallel()
	be := backend.Memory()
	e := separateCatalogEngine(be)
	t.Cleanup(func() { require.NoError(t, e.CloseWAL()) })
	var liveID, orphanID partid.ID
	liveID[15], orphanID[15] = 1, 2
	live := catalogRoot(t, be, "series-catalog/s-"+liveID.String(), 1)
	orphan := catalogRoot(t, be, "series-catalog/s-"+orphanID.String(), 2)
	e.cfg.Now = func() time.Time { return time.Unix(86400, 0) }
	e.mu.Lock()
	err := e.installSeriesRootsLocked(t.Context(), []bucketindex.SeriesRoot{live})
	if err == nil {
		err = e.sweepOrphansLocked(t.Context())
	}
	e.mu.Unlock()
	require.NoError(t, err)
	_, err = be.Read(t.Context(), live.Prefix+"/manifest")
	require.NoError(t, err)
	_, err = be.Read(t.Context(), orphan.Prefix+"/manifest")
	require.ErrorIs(t, err, backend.ErrNotExist)
}

func TestSeparateSeriesResetRefusesWithoutMutation(t *testing.T) {
	t.Parallel()
	e := separateCatalogEngine(backend.Memory())
	before := e.head
	require.ErrorContains(t, e.Reset(t.Context()), "unsupported")
	require.Same(t, before, e.head)
}

func TestSeriesCatalogClosePreservesActiveFetch(t *testing.T) {
	t.Parallel()
	for _, closeWAL := range []bool{false, true} {
		name := "Close"
		if closeWAL {
			name = "CloseWAL"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			e := separateCatalogEngine(backend.Memory())
			s := benchSeries(1)
			accepted, err := e.Append(s, 10, 2)
			require.NoError(t, err)
			require.True(t, accepted)
			require.NoError(t, e.Flush(t.Context()))
			it, err := e.Fetch(t.Context(), fetch.Request{Start: 0, End: 20})
			require.NoError(t, err)
			defer func() { require.NoError(t, it.Close()) }()
			run := e.seriesTable.roots[0].run
			if closeWAL {
				err = e.CloseWAL()
			} else {
				err = e.Close(t.Context())
			}
			require.NoError(t, err)
			require.Nil(t, e.seriesTable)
			reader, err := run.NewReader(t.Context())
			require.NoError(t, err)
			require.NoError(t, reader.Close())
			batch, err := it.Next(t.Context())
			require.NoError(t, err)
			require.True(t, s.Equal(batch.Series))
			require.Equal(t, []float64{2}, batch.Values)
			require.NoError(t, it.Close())
			_, err = run.NewReader(t.Context())
			require.ErrorIs(t, err, seriestable.ErrClosed)
		})
	}
}

func TestSeriesCatalogSharedReferenceAlwaysFences(t *testing.T) {
	t.Parallel()
	for _, damage := range []string{"marker", "descriptor"} {
		t.Run(damage, func(t *testing.T) {
			t.Parallel()
			be := backend.Memory()
			writer := separateCatalogEngine(be)
			accepted, err := writer.Append(benchSeries(1), 10, 2)
			require.NoError(t, err)
			require.True(t, accepted)
			require.NoError(t, writer.Close(t.Context()))
			ix, version, err := bucketindex.LoadVersioned(t.Context(), be, writer.indexKey())
			require.NoError(t, err)
			require.Len(t, ix.Entries, 1)
			if damage == "marker" {
				require.NoError(t, be.Delete(t.Context(), seriesReferenceKey(ix.Entries[0].Prefix)))
			} else {
				ix.SeriesRoots = []bucketindex.SeriesRoot{catalogRoot(t, be, "series-catalog/s-wrong", 2)}
				_, err = ix.Save(t.Context(), be, writer.indexKey(), version)
				require.NoError(t, err)
			}
			before, err := be.Read(t.Context(), writer.indexKey())
			require.NoError(t, err)
			reader := separateCatalogEngine(be)
			defer func() { require.NoError(t, reader.CloseWAL()) }()
			for range corruptLoadsBeforeWant + 2 {
				err = reader.LoadParts(t.Context())
				require.ErrorIs(t, err, errSharedSeriesReference)
				require.True(t, reader.IndexFenced())
				require.Empty(t, reader.wants)
				require.Empty(t, reader.pendingWants)
				require.Nil(t, reader.seriesTable)
				after, readErr := be.Read(t.Context(), reader.indexKey())
				require.NoError(t, readErr)
				require.Equal(t, before, after)
			}
		})
	}
}

func TestSeriesCatalogMissingNumericPartStillRecordsWant(t *testing.T) {
	t.Parallel()
	be := backend.Memory()
	writer := separateCatalogEngine(be)
	accepted, err := writer.Append(benchSeries(1), 10, 2)
	require.NoError(t, err)
	require.True(t, accepted)
	require.NoError(t, writer.Close(t.Context()))
	ix, err := bucketindex.Load(t.Context(), be, writer.indexKey())
	require.NoError(t, err)
	require.Len(t, ix.Entries, 1)
	require.NoError(t, be.Delete(t.Context(), ix.Entries[0].Prefix+"/manifest"))
	reader := separateCatalogEngine(be)
	defer func() { require.NoError(t, reader.CloseWAL()) }()
	require.NoError(t, reader.LoadParts(t.Context()))
	require.Len(t, reader.wants, 1)
	require.Equal(t, ix.Entries[0].Prefix, reader.wants[0].Prefix)
	require.Equal(t, ix.SeriesRoots, reader.seriesTable.bindings())
}

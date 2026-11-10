package engine

import (
	"context"
	"slices"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/backend/backendtest"
	"github.com/oteldb/storage/backend/faultbackend"
	"github.com/oteldb/storage/internal/memsize"
	"github.com/oteldb/storage/internal/seriesindex"
	"github.com/oteldb/storage/signal"
)

func partBloomFixture(tb testing.TB, be backend.Backend, count int) *Engine {
	tb.Helper()
	e := New(Config{Backend: be, Prefix: "part-id-bloom", DiskSeries: true})
	tb.Cleanup(func() { _ = e.Close(context.Background()) })
	ids, ts, values := make([]signal.SeriesID, count), make([]int64, count), make([]float64, count)
	for i := range ids {
		ids[i], ts[i] = benchSeries(i).Hash(), 100
	}
	result, err := e.AppendBatch(ids, ts, values, nil, benchSeries, AppendLimits{})
	require.NoError(tb, err)
	require.Equal(tb, count, result.Accepted)
	require.NoError(tb, e.Flush(tb.Context()))
	return e
}

func absentBloomIDs(p *part, count int) []signal.SeriesID {
	ids := make([]signal.SeriesID, 0, count)
	for i := uint64(0); len(ids) < count; i++ {
		id := signal.SeriesID{Hi: mixDiskBloom(i), Lo: mixDiskBloom(i ^ 0x9e3779b97f4a7c15)}
		if p.index.mayContainID(id) && !p.idBloom.Load().mayContain(id) {
			ids = append(ids, id)
		}
	}
	slices.SortFunc(ids, signal.SeriesID.Compare)
	return ids
}

func TestPartIDBloom(t *testing.T) {
	t.Parallel()
	for _, count := range []int{-1, 0, 1, 6, 7, 1000, maxInt} {
		b := newPartIDBloom(count)
		if count <= 0 {
			require.Nil(t, b)
			require.True(t, b.mayContain(signal.SeriesID{}))
			continue
		}
		require.LessOrEqual(t, b.sizeBytes(), int64(1<<20)+memsize.Of[partIDBloom]())
		for i := range min(count, 10000) {
			b.add(signal.SeriesID{Lo: uint64(i)})
		}
		for i := range min(count, 10000) {
			require.True(t, b.mayContain(signal.SeriesID{Lo: uint64(i)}))
		}
	}
	b := newPartIDBloom(10000)
	for i := range 10000 {
		b.add(signal.SeriesID{Lo: uint64(i)})
	}
	positive := 0
	for i := range 10000 {
		if b.mayContain(signal.SeriesID{Lo: uint64(i + 10000)}) {
			positive++
		}
	}
	require.Less(t, positive, 300, "bounded probes remain selective at the target load")
}

func TestPartIDBloomAvoidsAbsentMetadataIO(t *testing.T) {
	t.Parallel()
	be := backendtest.NewByteCounter(backend.Memory())
	e := partBloomFixture(t, be, 1024)
	require.Len(t, e.parts, 1)
	p := e.parts[0]
	require.NotNil(t, p.idBloom.Load())
	ids := absentBloomIDs(p, 100)
	be.Reset()
	for _, id := range ids {
		_, found, err := p.diskSeriesMetadata(t.Context(), id)
		require.NoError(t, err)
		require.False(t, found)
	}
	var reader seriesindex.MetadataReader
	require.NoError(t, p.visitDiskMetadata(t.Context(), ids, &reader, func(signal.SeriesID, seriesindex.Metadata) { t.Error("absent ID returned metadata") }))
	require.Zero(t, be.Reads(), be.Report())
	bloom := p.idBloom.Swap(nil)
	require.NoError(t, p.visitDiskMetadata(t.Context(), ids, &reader, func(signal.SeriesID, seriesindex.Metadata) { t.Error("absent ID returned metadata") }))
	require.Positive(t, be.Reads(), "the same in-range IDs require index reads without the filter")
	p.idBloom.Store(bloom)
	present := benchSeries(100).Hash()
	meta, found, err := p.diskSeriesMetadata(t.Context(), present)
	require.NoError(t, err)
	require.True(t, found)
	require.EqualValues(t, 100, meta.Covered)
}

func TestPartIDBloomFailedValidationDoesNotPublish(t *testing.T) {
	t.Parallel()
	for _, failure := range []string{"mismatch", "cancel"} {
		t.Run(failure, func(t *testing.T) {
			t.Parallel()
			be := faultbackend.Wrap(backend.Memory())
			e := partBloomFixture(t, be, 1024)
			original := e.parts[0]
			p, err := openPart(t.Context(), be, original.prefix, nil)
			require.NoError(t, err)
			t.Cleanup(p.closeReaders)
			require.Nil(t, p.idBloom.Load())
			ctx, cancel := context.WithCancel(t.Context())
			t.Cleanup(cancel)
			rule := faultbackend.Rule{Kind: faultbackend.Read, Match: func(op faultbackend.Op) bool { return op.Key == sidxKey(p.prefix) }}
			if failure == "mismatch" {
				rule.Replace = func(_ faultbackend.Op, raw []byte) []byte {
					out := slices.Clone(raw)
					out[int(p.index.paged.offset)+512*sidxEntryW+8] ^= 1
					return out
				}
			} else {
				rule.After = func(faultbackend.Op, []byte) { cancel() }
			}
			be.Add(rule)
			err = p.validateDiskSeriesIDs(ctx)
			require.Error(t, err)
			if failure == "mismatch" {
				require.ErrorIs(t, err, seriesindex.ErrCorrupt)
			} else {
				require.ErrorIs(t, err, context.Canceled)
			}
			require.False(t, p.diskIDsChecked)
			require.Nil(t, p.idBloom.Load(), "partial scans must never advertise an absence")
			require.True(t, p.idBloom.Load().mayContain(signal.SeriesID{Lo: 42}))
		})
	}
}

func TestPartIDBloomMixedLegacyAndReload(t *testing.T) {
	t.Parallel()
	be := backend.Memory()
	legacy, identities := diskRecoveryFixture(t, be, 9)
	e := New(Config{Backend: be, Prefix: legacy.cfg.Prefix, DiskSeries: true})
	t.Cleanup(func() { _ = e.Close(context.Background()) })
	require.NoError(t, e.LoadPartsReadOnly(t.Context()))
	require.Nil(t, e.parts[0].idBloom.Load(), "legacy parts retain unfiltered on-demand lookup")
	newSeries := benchSeries(100)
	ok, err := e.Append(newSeries, 100, 1)
	require.NoError(t, err)
	require.True(t, ok)
	require.NoError(t, e.Flush(t.Context()))
	require.Len(t, e.parts, 2)
	reloaded := New(Config{Backend: be, Prefix: e.cfg.Prefix, DiskSeries: true})
	t.Cleanup(func() { _ = reloaded.Close(context.Background()) })
	require.NoError(t, reloaded.LoadPartsReadOnly(t.Context()))
	filtered := 0
	for _, p := range reloaded.parts {
		if p.idBloom.Load() != nil {
			filtered++
		}
		for _, s := range []signal.Series{identities[0], newSeries} {
			_, _, err := p.diskSeriesMetadata(t.Context(), s.Hash())
			require.NoError(t, err)
		}
	}
	require.Equal(t, 1, filtered)
}

func TestPartIDBloomMemoryIsReportedSeparately(t *testing.T) {
	t.Parallel()
	e := partBloomFixture(t, backend.Memory(), 1024)
	p := e.parts[0]
	filterBytes := p.idBloom.Load().sizeBytes()
	base := e.head.identityBytes() + e.identityOrder.bytes.Load()
	require.Equal(t, base+filterBytes, e.IdentityBytes())
	require.Equal(t, e.IdentityBytes(), e.Stats().IdentityBytes)
	before := e.head.residentIdentityBytes()
	_, err := e.PruneIdentitiesWith(t.Context(), PruneOptions{Force: true})
	require.NoError(t, err)
	require.Equal(t, before, e.head.residentIdentityBytes())
	require.NotNil(t, p.idBloom.Load(), "catalog recount keeps immutable part filters")
}

func TestPartIDBloomWriterSeedDoesNotSkipValidation(t *testing.T) {
	t.Parallel()
	be := faultbackend.Wrap(backend.Memory())
	e := partBloomFixture(t, be, 1024)
	p := e.parts[0]
	require.NotNil(t, p.idBloom.Load())
	require.False(t, p.diskIDsChecked)
	for i := range 1024 {
		require.True(t, p.idBloom.Load().mayContain(benchSeries(i).Hash()))
	}
	be.Add(faultbackend.Rule{Kind: faultbackend.Read, Match: func(op faultbackend.Op) bool { return op.Key == sidxKey(p.prefix) }, Replace: func(_ faultbackend.Op, raw []byte) []byte {
		out := slices.Clone(raw)
		out[int(p.index.paged.offset)+512*sidxEntryW+8] ^= 1
		return out
	}})
	require.ErrorIs(t, p.validateDiskSeriesIDs(t.Context()), seriesindex.ErrCorrupt)
	require.False(t, p.diskIDsChecked)
}

func TestPartIDBloomMemoryDoesNotCountAliasedHandles(t *testing.T) {
	t.Parallel()
	e := New(Config{DiskSeries: true})
	p := &part{}
	p.idBloom.Store(newPartIDBloom(1000))
	e.parts = []*part{p}
	e.foreignParts = map[string]*part{"same": p}
	e.retiring = []*part{p, p}
	want := e.head.identityBytes() + e.identityOrder.bytes.Load() + p.idBloom.Load().sizeBytes()
	require.Equal(t, want, e.IdentityBytes())
	require.Equal(t, want, e.Stats().IdentityBytes)
	e.parts = nil
	require.Equal(t, want, e.IdentityBytes(), "adopted/retired aliases keep one charge")
	e.foreignParts = nil
	require.Equal(t, want, e.IdentityBytes(), "duplicate retirement entries keep one charge")
}

func FuzzPartIDBloomNoFalseNegatives(f *testing.F) {
	f.Add(uint64(0), uint64(0), uint16(64))
	f.Add(uint64(0xdeadbeef), uint64(0xf00d), uint16(7))
	f.Fuzz(func(t *testing.T, high, low uint64, count uint16) {
		n := int(count)%1024 + 1
		b := newPartIDBloom(n)
		for i := range n {
			b.add(signal.SeriesID{Hi: high ^ uint64(i), Lo: low + uint64(i)})
		}
		for i := range n {
			require.True(t, b.mayContain(signal.SeriesID{Hi: high ^ uint64(i), Lo: low + uint64(i)}))
		}
		require.LessOrEqual(t, b.sizeBytes(), int64(1<<20)+memsize.Of[partIDBloom]())
	})
}

func TestPartIDBloomSaturationHasNoFalseNegatives(t *testing.T) {
	t.Parallel()
	b := newPartIDBloom(1)
	for i := range 1000 {
		b.add(signal.SeriesID{Hi: uint64(i), Lo: uint64(i * 7)})
	}
	for i := range 1000 {
		require.True(t, b.mayContain(signal.SeriesID{Hi: uint64(i), Lo: uint64(i * 7)}))
	}
	require.Len(t, b.words, 1)
	require.True(t, (&partIDBloom{}).mayContain(signal.SeriesID{Lo: 1}), "an uninitialized filter never advertises absence")
}

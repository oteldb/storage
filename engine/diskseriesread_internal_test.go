package engine

import (
	"context"
	"encoding/binary"
	"slices"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/backend/backendtest"
	"github.com/oteldb/storage/index/identity"
	"github.com/oteldb/storage/index/postings"
	"github.com/oteldb/storage/index/series"
	"github.com/oteldb/storage/internal/seriesindex"
	"github.com/oteldb/storage/internal/watermark"
	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/readbudget"
	"github.com/oteldb/storage/signal"
)

func diskRecoveryFixture(t *testing.T, b backend.Backend, n int) (*Engine, []signal.Series) {
	t.Helper()
	e := New(Config{Backend: b, Prefix: "disk-recovery"})
	t.Cleanup(func() { require.NoError(t, e.Close(t.Context())) })
	identities := make([]signal.Series, n)
	for i := range identities {
		identities[i] = signal.Series{Attributes: signal.NewAttributes(signal.KeyValue{Key: []byte("host"), Value: signal.StringValue([]byte(strconv.Itoa(i)))})}
		_, err := e.Append(identities[i], int64(i+1), float64(i))
		require.NoError(t, err)
	}
	require.NoError(t, e.Flush(t.Context()))
	require.Len(t, e.parts, 1)
	return e, identities
}

func TestDiskSeriesLegacyRead(t *testing.T) {
	t.Parallel()
	for _, mode := range []string{"part", "ancient", "new index"} {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			b := backendtest.NewByteCounter(backend.Memory())
			writer, identities := diskRecoveryFixture(t, b, 9)
			p := writer.parts[0]
			switch mode {
			case "ancient":
				data := binary.AppendUvarint(nil, uint64(len(identities)+1))
				for _, s := range append(identities, signal.Series{Attributes: signal.NewAttributes(signal.KeyValue{Key: []byte("dead"), Value: signal.IntValue(1)})}) {
					raw := s.AppendHashInput(nil)
					data = binary.AppendUvarint(data, uint64(len(raw)))
					data = append(data, raw...)
				}
				require.NoError(t, b.Write(t.Context(), writer.seriesKey(), data))
				require.NoError(t, b.Delete(t.Context(), identityKey(p.prefix)))
			case "new index":
				sorted := slices.Clone(identities)
				slices.SortFunc(sorted, func(a, b signal.Series) int { return a.Hash().Compare(b.Hash()) })
				builder := &seriesindex.Builder{}
				for _, s := range sorted {
					require.NoError(t, builder.Add(s.Hash(), s, seriesindex.Metadata{Covered: 99}))
				}
				require.NoError(t, builder.WriteObject(t.Context(), b, seriesIndexKey(p.prefix)))
			}
			reader := New(Config{Backend: b, Prefix: writer.cfg.Prefix, DiskSeries: true})
			t.Cleanup(func() { require.NoError(t, reader.Close(t.Context())) })
			before, err := b.List(t.Context(), "")
			require.NoError(t, err)
			b.Reset()
			require.NoError(t, reader.LoadPartsReadOnly(t.Context()))
			require.Zero(t, reader.head.series.Len(), "recovery must not retain part identity entries")
			require.Equal(t, 9, reader.SeriesCount())
			require.NotContains(t, b.Report(), "/identity")
			require.NotContains(t, b.Report(), "/series.bin")
			p = reader.parts[0]
			workspace, err := p.diskSeriesWorkspace(t.Context(), 1)
			require.NoError(t, err)
			require.Positive(t, workspace)
			ids, err := p.diskSeriesIDs(t.Context())
			require.NoError(t, err)
			got, err := postings.ToSlice(ids)
			require.NoError(t, err)
			expected := make([]signal.SeriesID, len(identities))
			for i, s := range identities {
				expected[i] = s.Hash()
			}
			slices.SortFunc(expected, signal.SeriesID.Compare)
			require.Equal(t, expected, got)
			var selected []signal.SeriesID
			matcher := fetch.Matcher{Name: []byte("host"), Match: func(v signal.Value) bool { return string(v.AppendText(nil)) == "3" }}
			require.NoError(t, p.visitDiskSeries(t.Context(), []fetch.Matcher{matcher}, func(id signal.SeriesID, raw []byte, m seriesindex.Metadata) error {
				s, n, err := signal.DecodeSeries(raw)
				require.NoError(t, err)
				require.Equal(t, len(raw), n)
				require.True(t, s.Equal(identities[3]))
				require.Positive(t, m.Covered)
				selected = append(selected, id)
				return nil
			}))
			require.Equal(t, []signal.SeriesID{identities[3].Hash()}, selected)
			s, meta, found, err := p.lookupDiskSeries(t.Context(), identities[3].Hash())
			require.NoError(t, err)
			require.True(t, found)
			require.True(t, s.Equal(identities[3]))
			require.Positive(t, meta.Covered)
			after, err := b.List(t.Context(), "")
			require.NoError(t, err)
			require.Equal(t, before, after, "read-only load must preserve every source object")
			removed, err := reader.PruneIdentitiesWith(t.Context(), PruneOptions{Force: true})
			require.NoError(t, err)
			require.Zero(t, removed)
			require.Equal(t, 9, reader.SeriesCount())
		})
	}
}

func TestDiskSeriesLegacyCorruption(t *testing.T) {
	t.Parallel()
	for _, mode := range []string{"corrupt", "incomplete", "duplicate", "missing"} {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			b := backend.Memory()
			writer, identities := diskRecoveryFixture(t, b, 2)
			p := writer.parts[0]
			switch mode {
			case "corrupt":
				require.NoError(t, b.Write(t.Context(), identityKey(p.prefix), []byte("bad identity")))
			case "missing":
				require.NoError(t, b.Delete(t.Context(), identityKey(p.prefix)))
			default:
				entries := []series.Entry{{ID: identities[0].Hash(), Series: identities[0]}}
				if mode == "duplicate" {
					entries = append(entries, entries[0])
				}
				require.NoError(t, b.Write(t.Context(), identityKey(p.prefix), identity.Encode(nil, entries)))
			}
			reader := New(Config{Backend: b, Prefix: writer.cfg.Prefix, DiskSeries: true})
			t.Cleanup(func() { require.NoError(t, reader.Close(t.Context())) })
			require.NoError(t, reader.LoadPartsReadOnly(t.Context()))
			if mode == "missing" {
				require.Empty(t, reader.parts)
				require.True(t, reader.HasWants())
				return
			}
			p = reader.parts[0]
			err := p.visitDiskSeries(t.Context(), []fetch.Matcher{{Name: []byte("absent"), Match: func(signal.Value) bool { return false }}}, func(signal.SeriesID, []byte, seriesindex.Metadata) error { return nil })
			require.Error(t, err, "an empty match must not conceal missing/corrupt identity data")
		})
	}
}

func TestDiskSeriesIDsPageBound(t *testing.T) {
	t.Parallel()
	b := backendtest.NewByteCounter(backend.Memory())
	writer, identities := diskRecoveryFixture(t, b, 3*indexPageEntries+7)
	p := writer.parts[0]
	p.index.paged.drop()
	b.Reset()
	ids, err := p.diskSeriesIDs(t.Context())
	require.NoError(t, err)
	got, err := postings.ToSlice(ids)
	require.NoError(t, err)
	expected := make([]signal.SeriesID, len(identities))
	for i, s := range identities {
		expected[i] = s.Hash()
	}
	slices.SortFunc(expected, signal.SeriesID.Compare)
	require.Equal(t, expected, got)
	require.LessOrEqual(t, b.Bytes(), int64(len(identities)*sidxEntryW))
	require.Nil(t, p.index.paged.view.Load(), "streaming enumeration must not pin the entire sidx")
	require.NotContains(t, b.Report(), "/identity")
}

func TestDiskSeriesLegacyWorkspaceIncludesTimestampFallback(t *testing.T) {
	t.Parallel()
	b := backendtest.NewByteCounter(backend.Memory())
	writer, _ := diskRecoveryFixture(t, b, 2)
	p := writer.parts[0]
	require.NoError(t, b.Delete(t.Context(), watermark.Key(p.prefix)))
	b.Reset()
	bytes, err := p.diskSeriesWorkspace(t.Context(), 1)
	require.NoError(t, err)
	col, shared, err := wholeMergeColumnBytes(p, colTs)
	require.NoError(t, err)
	require.GreaterOrEqual(t, bytes, col+shared)
	require.NotContains(t, b.Report(), "/identity", "workspace sizing must precede payload reads")
	require.NoError(t, p.visitDiskSeries(t.Context(), nil, func(signal.SeriesID, []byte, seriesindex.Metadata) error { return nil }))
	require.Nil(t, p.seriesMax, "legacy per-operation fallback must not retain tenant-wide watermarks")
}

type opaqueDiskBackend struct {
	backend.Backend

	reads int
}

func (b *opaqueDiskBackend) Read(ctx context.Context, key string) ([]byte, error) {
	b.reads++
	return b.Backend.Read(ctx, key)
}

func TestDiskSeriesRejectsOpaqueReadsBeforeAdmission(t *testing.T) {
	t.Parallel()
	for _, cached := range []bool{false, true} {
		t.Run(strconv.FormatBool(cached), func(t *testing.T) {
			t.Parallel()
			b := backend.Memory()
			writer, identities := diskRecoveryFixture(t, b, 2)
			original := writer.parts[0]
			sorted := slices.Clone(identities)
			slices.SortFunc(sorted, func(a, b signal.Series) int { return a.Hash().Compare(b.Hash()) })
			builder := &seriesindex.Builder{}
			for _, s := range sorted {
				require.NoError(t, builder.Add(s.Hash(), s, seriesindex.Metadata{Covered: 99}))
			}
			require.NoError(t, builder.WriteObject(t.Context(), b, seriesIndexKey(original.prefix)))
			opaque := &opaqueDiskBackend{Backend: b}
			p := &part{reader: original.reader, be: opaque, prefix: original.prefix, index: original.index}
			if cached {
				_, err := p.seriesIndex(t.Context())
				require.NoError(t, err)
			}
			opaque.reads = 0
			ctx := readbudget.With(t.Context(), readbudget.New(1<<20))
			_, err := p.diskSeriesWorkspace(ctx, 1)
			require.ErrorIs(t, err, readbudget.ErrExceeded)
			require.Zero(t, opaque.reads, "opaque index payload must not be read before admission")
		})
	}
}

func TestDiskSeriesCorruptIndexFencesRecovery(t *testing.T) {
	t.Parallel()
	b := backend.Memory()
	writer, identities := diskRecoveryFixture(t, b, 1)
	builder := &seriesindex.Builder{}
	require.NoError(t, builder.Add(identities[0].Hash(), identities[0], seriesindex.Metadata{}))
	key := seriesIndexKey(writer.parts[0].prefix)
	require.NoError(t, builder.WriteObject(t.Context(), b, key))
	raw, err := b.Read(t.Context(), key)
	require.NoError(t, err)
	raw[len(raw)-1] ^= 1
	require.NoError(t, b.Write(t.Context(), key, raw))
	reader := New(Config{Backend: b, Prefix: writer.cfg.Prefix, DiskSeries: true})
	t.Cleanup(func() { require.NoError(t, reader.Close(t.Context())) })
	require.ErrorIs(t, reader.LoadPartsReadOnly(t.Context()), seriesindex.ErrCorrupt)
	require.True(t, reader.IndexFenced())
	require.Empty(t, reader.parts)
}

func TestDiskSeriesRepairRestoresPartMetadata(t *testing.T) {
	t.Parallel()
	b := backend.Memory()
	writer, identities := diskRecoveryFixture(t, b, 1)
	prefix := writer.parts[0].prefix
	builder := &seriesindex.Builder{}
	require.NoError(t, builder.Add(identities[0].Hash(), identities[0], seriesindex.Metadata{Covered: 1}))
	require.NoError(t, builder.WriteObject(t.Context(), b, seriesIndexKey(prefix)))
	key := prefix + "/manifest"
	manifest, err := b.Read(t.Context(), key)
	require.NoError(t, err)
	require.NoError(t, b.Delete(t.Context(), key))
	reader := New(Config{Backend: b, Prefix: writer.cfg.Prefix, DiskSeries: true})
	t.Cleanup(func() { require.NoError(t, reader.Close(t.Context())) })
	require.NoError(t, reader.LoadPartsReadOnly(t.Context()))
	require.True(t, reader.HasWants())
	require.Zero(t, reader.SeriesCount())
	require.NoError(t, b.Write(t.Context(), key, manifest))
	reader.repairWants(t.Context())
	require.False(t, reader.HasWants())
	require.Equal(t, 1, reader.SeriesCount())
	require.Zero(t, reader.head.series.Len())
	require.Len(t, reader.parts, 1)
	s, meta, found, err := reader.parts[0].lookupDiskSeries(t.Context(), identities[0].Hash())
	require.NoError(t, err)
	require.True(t, found)
	require.True(t, s.Equal(identities[0]))
	require.Equal(t, int64(1), meta.Covered)
}

func TestDiskSeriesWrongPartIndexFencesRecovery(t *testing.T) {
	t.Parallel()
	b := backend.Memory()
	writer, _ := diskRecoveryFixture(t, b, 1)
	other := signal.Series{Attributes: signal.NewAttributes(signal.KeyValue{Key: []byte("other"), Value: signal.IntValue(99)})}
	builder := &seriesindex.Builder{}
	require.NoError(t, builder.Add(other.Hash(), other, seriesindex.Metadata{}))
	require.NoError(t, builder.WriteObject(t.Context(), b, seriesIndexKey(writer.parts[0].prefix)))
	reader := New(Config{Backend: b, Prefix: writer.cfg.Prefix, DiskSeries: true})
	t.Cleanup(func() { require.NoError(t, reader.Close(t.Context())) })
	require.ErrorIs(t, reader.LoadPartsReadOnly(t.Context()), seriesindex.ErrCorrupt)
	require.True(t, reader.IndexFenced())
	require.Empty(t, reader.parts, "a mismatched index must fail before readable publication")
}

func TestDiskSeriesPruneRecountsEmptyHead(t *testing.T) {
	t.Parallel()
	b := backend.Memory()
	writer, _ := diskRecoveryFixture(t, b, 2)
	reader := New(Config{Backend: b, Prefix: writer.cfg.Prefix, DiskSeries: true})
	t.Cleanup(func() { require.NoError(t, reader.Close(t.Context())) })
	require.NoError(t, reader.LoadPartsReadOnly(t.Context()))
	require.Equal(t, 2, reader.SeriesCount())
	reader.mu.Lock()
	parts := reader.parts
	reader.parts = nil
	reader.identityDirty = true
	reader.mu.Unlock()
	removed, err := reader.PruneIdentities(t.Context())
	require.NoError(t, err)
	require.Zero(t, removed)
	require.Zero(t, reader.SeriesCount(), "part-only cardinality must shrink even below head prune thresholds")
	for _, p := range parts {
		p.closeReaders()
	}
}

func TestDiskSeriesRefreshDropsTrimmedMetadata(t *testing.T) {
	t.Parallel()
	b := backend.Memory()
	writer, identities := diskRecoveryFixture(t, b, 1)
	replica := New(Config{Backend: b, Prefix: writer.cfg.Prefix, DiskSeries: true})
	t.Cleanup(func() { require.NoError(t, replica.Close(t.Context())) })
	_, err := replica.Append(identities[0], 1, 0)
	require.NoError(t, err)
	require.NoError(t, replica.RefreshReplica(t.Context()))
	require.Empty(t, replica.head.samples)
	require.Zero(t, replica.head.series.Len(), "replica coverage trimming must reclaim active metadata")
	require.Equal(t, 1, replica.SeriesCount())
}

package engine

import (
	"encoding/binary"
	"slices"
	"strconv"
	"strings"
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
			require.Len(t, got, len(identities))
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
	require.Len(t, got, len(identities))
	require.True(t, slices.IsSortedFunc(got, signal.SeriesID.Compare))
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
	require.False(t, strings.Contains(b.Report(), "/identity"), "workspace sizing must precede payload reads")
	require.NoError(t, p.visitDiskSeries(t.Context(), nil, func(signal.SeriesID, []byte, seriesindex.Metadata) error { return nil }))
	require.Nil(t, p.seriesMax, "legacy per-operation fallback must not retain tenant-wide watermarks")
}

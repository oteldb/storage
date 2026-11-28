package engine

import (
	"encoding/binary"
	"hash/crc32"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend/file"
	"github.com/oteldb/storage/block"
	"github.com/oteldb/storage/index/series"
	"github.com/oteldb/storage/signal"
)

func TestPrimarySeriesIndexBindsRows(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	be, err := file.New(t.TempDir())
	require.NoError(t, err)
	e := New(Config{Backend: be, Prefix: "primary-index", DiskSeries: true, SeparateSeries: true,
		MetricSeriesChunks: true})
	t.Cleanup(func() { require.NoError(t, e.Close(ctx)) })
	w, err := newPartStreamWriterMode(ctx, e, "", compressProfile{}, 0, false, false, nil, true)
	require.NoError(t, err)
	defer w.abort()
	registry := series.New()
	s := benchSeries(1)
	id := registry.Add(s)
	ref, ok := registry.Ref(id)
	require.True(t, ok)
	require.NoError(t, w.appendSeries(idToU128(id), []int64{10}, []float64{2}, nil, 37))
	out, err := w.finishWithIdentities(ctx, identitySnapshot{view: registry.View(), refs: []uint32{ref}})
	require.NoError(t, err)
	out.closeReaders()
	_, duplicate := out.reader.ColumnDescByName(colSeries)
	require.False(t, duplicate)
	reopened, err := openPart(ctx, be, out.prefix, nil)
	require.NoError(t, err)
	src, err := reopened.index.idSource(ctx)
	require.NoError(t, err)
	require.Equal(t, id, src.At(0))
	require.Equal(t, rowRange{0, 1}, src.RangeAt(0))
	reopened.closeReaders()

	raw, err := be.Read(ctx, sidxKey(out.prefix))
	require.NoError(t, err)
	ents, _, _, err := parseSeriesIndex(raw, true)
	require.NoError(t, err)
	ents[0] ^= 1
	binary.BigEndian.PutUint32(raw[len(raw)-4:], crc32.Checksum(raw[:len(raw)-4], sidxCRC))
	require.NoError(t, be.Write(ctx, sidxKey(out.prefix), raw))
	_, err = openPart(ctx, be, out.prefix, nil)
	require.ErrorIs(t, err, block.ErrCorrupt)
}

func TestSingletonIndexLookup(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	be, err := file.New(t.TempDir())
	require.NoError(t, err)
	const count = 2304
	const key = "singleton-index"
	require.NoError(t, be.Write(ctx, sidxKey(key), singletonIndexFixture(count)))
	p, ok := openPagedIndex(ctx, be, key, count, nil)
	require.True(t, ok)
	require.True(t, p.singletons)
	require.Equal(t, 1, p.maxRows)
	for _, k := range []int{0, 255, 256, 1023, 1024, count - 1} {
		id := signal.SeriesID{Lo: uint64(2*k + 1)}
		for _, page := range []int{sparseIndexPageEntries, indexPageEntries} {
			pos, rows, found, err := p.lookupPosition(ctx, id, page)
			require.NoError(t, err)
			require.True(t, found)
			require.Equal(t, k, pos)
			require.Equal(t, rowRange{k, k + 1}, rows)
		}
	}
	c := diskPartIDs{index: p, ctx: ctx, pos: -1}
	for k := range count {
		require.True(t, c.Next())
		require.Equal(t, signal.SeriesID{Lo: uint64(2*k + 1)}, c.At())
	}
	require.False(t, c.Next())
	require.NoError(t, c.Err())
}

func singletonIndexFixture(count int) []byte {
	dst := binary.BigEndian.AppendUint32(nil, sidxMagic)
	dst = append(dst, sidxSingletonVersion)
	dst = binary.AppendUvarint(dst, uint64(count))
	dst = binary.AppendUvarint(dst, uint64(count))
	for i := range count {
		dst = signal.SeriesID{Lo: uint64(2*i + 1)}.AppendBinary(dst)
	}
	return binary.BigEndian.AppendUint32(dst, crc32.Checksum(dst, sidxCRC))
}

package engine

import (
	"context"
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
	require.True(t, out.reader.Manifest().RequiresSeriesIndex())
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

func TestPrimarySeriesIndexDefaultMerge(t *testing.T) {
	t.Parallel()
	for _, name := range []string{"newer-version", "temporary-index-read"} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			testPrimarySeriesIndexLoadFailure(t, name)
		})
	}
}

func testPrimarySeriesIndexLoadFailure(t *testing.T, failure string) {
	t.Helper()
	ctx := t.Context()
	be, err := file.New(t.TempDir())
	require.NoError(t, err)
	cfg := Config{Backend: be, Prefix: "required-index", DiskSeries: true, SeparateSeries: true, MetricSeriesChunks: true}
	writer := New(cfg)
	t.Cleanup(func() { require.NoError(t, writer.Close(context.Background())) })
	for i := range 2 {
		ok, err := writer.Append(benchSeries(1), int64(10+i), float64(i))
		require.NoError(t, err)
		require.True(t, ok)
		require.NoError(t, writer.Flush(ctx))
	}
	for _, p := range writer.parts {
		require.False(t, p.reader.Manifest().RequiresSeriesIndex())
	}
	ok, err := writer.MergeSamplesStep(ctx, MergeOptions{Force: true})
	require.NoError(t, err)
	require.True(t, ok)
	require.Len(t, writer.parts, 1)
	merged := writer.parts[0]
	m := merged.reader.Manifest()
	require.EqualValues(t, 4, m.Version)
	require.True(t, m.RequiresSeriesIndex())
	_, hasSeries := merged.reader.ColumnDescByName(colSeries)
	require.False(t, hasSeries)

	catalog, catalogVersion, err := be.ReadVersioned(ctx, writer.indexKey())
	require.NoError(t, err)
	check := New(cfg)
	require.NoError(t, check.LoadPartsReadOnly(ctx))
	require.Len(t, check.parts, 1)
	require.Equal(t, 2, check.parts[0].rows())
	require.NoError(t, check.Close(ctx))

	var expected error
	switch failure {
	case "newer-version":
		m.Version++
		require.NoError(t, be.Write(ctx, merged.prefix+"/manifest", m.Encode(nil)))
		expected = block.ErrUnsupportedVersion
	case "temporary-index-read":
		expected = context.DeadlineExceeded
		cfg.Backend = unreadablePrimaryIndex{File: be, key: sidxKey(merged.prefix)}
	}
	reader := New(cfg)
	t.Cleanup(func() { require.NoError(t, reader.Close(context.Background())) })
	for range corruptLoadsBeforeWant + 2 {
		require.ErrorIs(t, reader.LoadPartsReadOnly(ctx), expected)
		require.True(t, reader.IndexFenced())
		require.False(t, reader.HasWants())
		require.Empty(t, reader.corruptLoads)
	}
	after, afterVersion, err := be.ReadVersioned(ctx, writer.indexKey())
	require.NoError(t, err)
	require.Equal(t, catalog, after)
	require.Equal(t, catalogVersion, afterVersion)
}

type unreadablePrimaryIndex struct {
	*file.File

	key string
}

func (b unreadablePrimaryIndex) Read(ctx context.Context, key string) ([]byte, error) {
	if key == b.key {
		return nil, context.DeadlineExceeded
	}
	return b.File.Read(ctx, key)
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

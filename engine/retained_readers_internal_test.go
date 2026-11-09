package engine

import (
	"math"
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/oteldb/storage/query/fetch"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend/bucketindex"
	"github.com/oteldb/storage/backend/file"
	"github.com/oteldb/storage/block"
	"github.com/oteldb/storage/encoding/chunk"
)

func TestRetiredPartReaderSurvivesActiveFetch(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	b, err := file.New(t.TempDir())
	require.NoError(t, err)
	write := func(base int64) {
		w := block.NewPartWriter(block.WithGranuleSize(2), block.WithSizingStats())
		require.NoError(t, w.AddColumn(block.Column{Name: "ts", Kind: block.KindInt64, Codec: chunk.CodecNone, Block: true, Int64: []int64{base, base + 1, base + 2, base + 3}}))
		require.NoError(t, block.WritePart(ctx, b, "part", w))
	}
	write(100)
	reader, err := block.OpenPart(ctx, b, "part")
	require.NoError(t, err)
	p := &part{reader: reader}
	p.acquire()
	defer func() {
		if p.refs.Load() != 0 {
			p.release()
		}
	}()
	first, err := reader.ColumnBlocks(ctx, "ts")
	require.NoError(t, err)
	old, err := first.DecodeInt64(0)
	require.NoError(t, err)
	require.Equal(t, []int64{100, 101}, old)
	p.closeReaders()
	write(200)
	old, err = first.DecodeInt64(1)
	require.NoError(t, err)
	require.Equal(t, []int64{102, 103}, old, "retirement must not release an active fetch's generation")
	p.release()
	replacement, err := block.OpenPart(ctx, b, "part")
	require.NoError(t, err)
	defer func() { require.NoError(t, replacement.Close()) }()
	next, err := replacement.ColumnBlocks(ctx, "ts")
	require.NoError(t, err)
	got, err := next.DecodeInt64(1)
	require.NoError(t, err)
	require.Equal(t, []int64{202, 203}, got)
}

func TestRetainedReaderSeesRestoredColumnOnReusedPart(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	dir := t.TempDir()
	b, err := file.New(dir)
	require.NoError(t, err)
	e := New(Config{Backend: b, Prefix: "metrics"})
	t.Cleanup(func() { _ = e.Close(ctx) })
	for i := range 16 {
		_, err := e.Append(apiSeries(), int64(i+1), float64(i+1))
		require.NoError(t, err)
	}
	require.NoError(t, e.Flush(ctx))
	originalPart := e.parts[0]
	query := func() error {
		it, err := e.Fetch(ctx, fetch.Request{Start: 0, End: 100})
		if err != nil {
			return err
		}
		_, err = fetch.Drain(ctx, it)
		return err
	}
	require.NoError(t, query())
	index := -1
	for i, name := range originalPart.reader.ColumnNames() {
		if name == colValue {
			index = i
		}
	}
	require.NotEqual(t, -1, index)
	key := originalPart.prefix + "/c/" + strconv.Itoa(index)
	saved, err := b.Read(ctx, key)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(dir, filepath.FromSlash(key)), make([]byte, len(saved)), 0o600))
	require.Error(t, query(), "the retained inode must observe the corruption")
	require.NoError(t, b.Delete(ctx, key))
	writer, err := b.CreateObject(ctx, key)
	require.NoError(t, err)
	_, err = writer.Write(saved)
	require.NoError(t, err)
	require.NoError(t, writer.Commit(ctx))
	require.NoError(t, e.RefreshReplica(ctx))
	require.Same(t, originalPart, e.parts[0], "repair reuses the matching live part descriptor")
	require.NoError(t, query(), "a new read must use the restored inode, without eviction or restart")
}

func TestForeignRefreshKeepsReaderOwnership(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	b, err := file.New(t.TempDir())
	require.NoError(t, err)
	e := New(Config{Backend: b, Prefix: "foreign"})
	t.Cleanup(func() { _ = e.Close(ctx) })
	for i := range 16 {
		_, err := e.Append(apiSeries(), int64(i+1), float64(i+1))
		require.NoError(t, err)
	}
	require.NoError(t, e.Flush(ctx))
	p := e.parts[0]
	e.parts = nil
	e.foreign = []bucketindex.Entry{{Prefix: p.prefix}}
	e.foreignParts = map[string]*part{p.prefix: p}
	e.openForeignLocked(ctx)
	e.openForeignLocked(ctx)
	require.Same(t, p, e.foreignParts[p.prefix])
	p.acquire()
	defer func() {
		if p.refs.Load() != 0 {
			p.release()
		}
	}()
	decoder, err := p.reader.ColumnBlocks(ctx, colValue)
	require.NoError(t, err)
	index := -1
	for i, name := range p.reader.ColumnNames() {
		if name == colValue {
			index = i
		}
	}
	require.NotEqual(t, -1, index)
	key := p.prefix + "/c/" + strconv.Itoa(index)
	saved, err := b.Read(ctx, key)
	require.NoError(t, err)
	require.NoError(t, b.Write(ctx, key, make([]byte, len(saved))))
	values, err := decoder.DecodeFloat64(0)
	require.NoError(t, err, "reusing foreign parts must retain their opened generation")
	require.Equal(t, uint64(0x3ff0000000000000), math.Float64bits(values[0]))
	e.foreign = nil
	e.openForeignLocked(ctx)
	require.Empty(t, e.foreignParts)
	values, err = decoder.DecodeFloat64(0)
	require.NoError(t, err)
	require.Equal(t, uint64(0x3ff0000000000000), math.Float64bits(values[0]))
	p.release()
	require.True(t, p.readersClosing.Load(), "a dropped foreign owner must release its scope after the active fetch")
}

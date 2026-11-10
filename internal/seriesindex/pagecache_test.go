package seriesindex

import (
	"bytes"
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/backend/backendtest"
	backendfile "github.com/oteldb/storage/backend/file"
	"github.com/oteldb/storage/internal/readerpool"
)

func TestTablePageCacheAcrossCursors(t *testing.T) {
	t.Parallel()
	inner := backend.Memory()
	require.NoError(t, inner.Write(t.Context(), "index", tableData(t, 4096, 1024)))
	counter := backendtest.NewByteCounter(inner)
	cached := backend.Cached(counter, 4<<20)
	r, err := OpenTable(t.Context(), cached, "index")
	require.NoError(t, err)
	probe := func(c *Cursor) {
		t.Helper()
		for _, i := range []int{3, 513, 1025, 2049, 4095} {
			require.True(t, c.Seek(fmt.Appendf(nil, "key-%08d", i)))
			require.Equal(t, fmt.Sprintf("value-%08d", i), string(c.Value()))
		}
		require.NoError(t, c.Err())
	}
	c := r.Cursor(t.Context())
	probe(c)
	reads := counter.Reads()
	bytesRead := counter.Bytes()
	clear(c.directory)
	clear(c.first)
	clear(c.packed)
	clear(c.raw)
	probe(r.Cursor(t.Context()))
	require.Equal(t, reads, counter.Reads(), "fresh cursors must reuse cached ranges")
	require.Equal(t, bytesRead, counter.Bytes(), "cursor scratch must not alias cached bytes")
}

func TestTablePageCacheInvalidation(t *testing.T) {
	t.Parallel()
	inner := backend.Memory()
	cached := backend.Cached(inner, 4<<20)
	for _, generation := range []string{"old", "replacement"} {
		var data bytes.Buffer
		w := NewTableWriter(&data)
		w.limit = 128
		for i := range 128 {
			require.NoError(t, w.Append(fmt.Appendf(nil, "key-%08d", i), []byte(generation)))
		}
		require.NoError(t, w.Finish())
		require.NoError(t, cached.Write(t.Context(), "index", data.Bytes()))
		r, err := OpenTable(t.Context(), cached, "index")
		require.NoError(t, err)
		for range 2 {
			c := r.Cursor(t.Context())
			require.True(t, c.Seek([]byte("key-00000073")))
			require.Equal(t, generation, string(c.Value()))
		}
	}
	require.NoError(t, cached.Delete(t.Context(), "index"))
	_, err := OpenTable(t.Context(), cached, "index")
	require.ErrorIs(t, err, backend.ErrNotExist)
}

type countedPageFile struct {
	*backendfile.File

	reads int
}

func (f *countedPageFile) ReadAt(ctx context.Context, key string, off, n int64) ([]byte, error) {
	f.reads++
	return f.File.ReadAt(ctx, key, off, n)
}

func (f *countedPageFile) ReadAtAppend(ctx context.Context, dst []byte, key string, off, n int64) ([]byte, error) {
	f.reads++
	return f.File.ReadAtAppend(ctx, dst, key, off, n)
}

func BenchmarkTableCachedRandomSeeks(b *testing.B) {
	files, err := backendfile.New(b.TempDir())
	require.NoError(b, err)
	data := tableData(b, 65536, 64<<10)
	require.NoError(b, files.Write(b.Context(), "index", data))
	counter := &countedPageFile{File: files}
	cached := backend.Cached(counter, 16<<20)
	owner := readerpool.NewScope()
	b.Cleanup(func() { require.NoError(b, owner.Close()) })
	ctx := owner.Context(b.Context())
	owner.Acquire()
	defer owner.Release()
	r, err := OpenTable(ctx, cached, "index")
	require.NoError(b, err)
	keys := make([][]byte, 16)
	for i := range keys {
		keys[i] = fmt.Appendf(nil, "key-%08d", (i*7919)%65536)
	}
	for _, key := range keys {
		c := r.Cursor(ctx)
		if !c.Seek(key) {
			b.Fatal(c.Err())
		}
	}
	startReads := counter.reads
	b.ReportAllocs()
	for b.Loop() {
		c := r.Cursor(ctx)
		for _, key := range keys {
			if !c.Seek(key) {
				b.Fatal(c.Err())
			}
		}
	}
	b.ReportMetric(float64(counter.reads-startReads)/float64(b.N), "file-reads/op")
}

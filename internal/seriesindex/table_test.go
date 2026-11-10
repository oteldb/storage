package seriesindex

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"strconv"
	"testing"

	"github.com/go-faster/sdk/gold"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	backendfile "github.com/oteldb/storage/backend/file"
)

func TestMain(m *testing.M) {
	gold.Init()
	os.Exit(m.Run())
}

func tableData(tb testing.TB, count, limit int) []byte {
	tb.Helper()
	var out bytes.Buffer
	w := NewTableWriter(&out)
	w.limit = limit
	for i := range count {
		require.NoError(tb, w.Append(fmt.Appendf(nil, "key-%08d", i), fmt.Appendf(nil, "value-%08d", i)))
	}
	require.NoError(tb, w.Finish())
	return out.Bytes()
}

func TestTableRoundTrip(t *testing.T) {
	t.Parallel()
	for _, disk := range []bool{false, true} {
		t.Run(strconv.FormatBool(disk), func(t *testing.T) {
			t.Parallel()
			var b = backend.Memory()
			if disk {
				f, err := backendfile.New(t.TempDir())
				require.NoError(t, err)
				b = f
			}
			require.NoError(t, b.Write(t.Context(), "index", tableData(t, 1000, 100)))
			r, err := OpenTable(t.Context(), b, "index")
			require.NoError(t, err)
			require.EqualValues(t, 1000, r.Count())
			c := r.Cursor(t.Context())
			for i := range 1000 {
				require.True(t, c.Next())
				require.Equal(t, fmt.Sprintf("key-%08d", i), string(c.Key()))
				require.Equal(t, fmt.Sprintf("value-%08d", i), string(c.Value()))
			}
			require.False(t, c.Next())
			require.NoError(t, c.Err())
			for _, i := range []int{999, 0, 500, 998} {
				require.True(t, c.Seek(fmt.Appendf(nil, "key-%08d", i)))
				require.Equal(t, fmt.Sprintf("key-%08d", i), string(c.Key()))
			}
			require.True(t, c.Seek([]byte("key-00000500x")))
			require.Equal(t, "key-00000501", string(c.Key()))
			require.False(t, c.Seek([]byte("zzz")))
			require.NoError(t, c.Err())
		})
	}
}

func TestTableGolden(t *testing.T) {
	t.Parallel()
	gold.Bytes(t, tableData(t, 0, 100), "table_empty")
	gold.Bytes(t, tableData(t, 3, 30), "table")
}

func TestTableRejectsCorruption(t *testing.T) {
	t.Parallel()
	data := tableData(t, 10, 100)
	for i := range data {
		bad := bytes.Clone(data)
		bad[i] ^= 1
		b := backend.Memory()
		require.NoError(t, b.Write(t.Context(), "index", bad))
		r, err := OpenTable(t.Context(), b, "index")
		if err == nil {
			c := r.Cursor(t.Context())
			for c.Next() {
			}
			err = c.Err()
		}
		require.Error(t, err, "corrupt byte %d", i)
	}
	for i := range data {
		b := backend.Memory()
		require.NoError(t, b.Write(t.Context(), "index", data[:i]))
		_, err := OpenTable(t.Context(), b, "index")
		require.Error(t, err, "truncate at %d", i)
	}
}

func TestTableRejectsOrderingAndWriteFailures(t *testing.T) {
	t.Parallel()
	w := NewTableWriter(io.Discard)
	require.NoError(t, w.Append([]byte("b"), nil))
	require.Error(t, w.Append([]byte("b"), nil))
	require.Error(t, w.Append([]byte("a"), nil))
	f := NewTableWriter(shortWriter{})
	require.NoError(t, f.Append([]byte("a"), []byte("value")))
	require.ErrorIs(t, f.Finish(), io.ErrShortWrite)
}

type shortWriter struct{}

func (shortWriter) Write([]byte) (int, error) { return 0, nil }

type rangedTable struct {
	backend.Backend

	data []byte
	read int
}

func (b *rangedTable) Size(context.Context, string) (int64, error)  { return int64(len(b.data)), nil }
func (b *rangedTable) Read(context.Context, string) ([]byte, error) { panic("whole index read") }
func (b *rangedTable) ReadAt(ctx context.Context, key string, off, n int64) ([]byte, error) {
	return b.ReadAtAppend(ctx, nil, key, off, n)
}
func (b *rangedTable) ReadAtAppend(ctx context.Context, dst []byte, _ string, off, n int64) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return dst, err
	}
	end := min(off+n, int64(len(b.data)))
	b.read += int(end - off)
	return append(dst, b.data[off:end]...), nil
}

func TestTableSeeksWithoutReadingWholeIndex(t *testing.T) {
	t.Parallel()
	b := &rangedTable{Backend: backend.Memory(), data: tableData(t, 10000, 100)}
	r, err := OpenTable(t.Context(), b, "index")
	require.NoError(t, err)
	require.Equal(t, footerSize, b.read, "opening retains only the fixed footer")
	c := r.Cursor(t.Context())
	require.True(t, c.Seek([]byte("key-00005000")))
	require.Equal(t, "value-00005000", string(c.Value()))
	require.Less(t, b.read, len(b.data)/20)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	c = r.Cursor(ctx)
	require.False(t, c.Next())
	require.ErrorIs(t, c.Err(), context.Canceled)
}

func FuzzTableRead(f *testing.F) {
	f.Add(tableData(f, 0, 100), []byte(""))
	f.Add(tableData(f, 10, 100), []byte("key-00000005"))
	f.Fuzz(func(t *testing.T, data, key []byte) {
		if len(data) > 1<<20 {
			t.Skip()
		}
		b := backend.Memory()
		if err := b.Write(t.Context(), "index", data); err != nil {
			t.Fatal(err)
		}
		r, err := OpenTable(t.Context(), b, "index")
		if err != nil || r.MaxPageBytes() > 4<<20 {
			return
		}
		c := r.Cursor(t.Context())
		c.Seek(key)
		for i := 0; i < 256 && c.Next(); i++ {
			if len(c.Key()) > maxPageSize {
				t.Fatal("decoded key exceeds page bound")
			}
		}
	})
}

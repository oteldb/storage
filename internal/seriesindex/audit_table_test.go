package seriesindex

import (
	"bytes"
	"context"
	"encoding/binary"
	"hash/crc32"
	"io"
	"math"
	"slices"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/backend/backendtest"
	"github.com/oteldb/storage/backend/faultbackend"
	"github.com/oteldb/storage/readbudget"
)

func appendRecord(dst []byte, prefix uint64, key, value []byte) []byte {
	dst = binary.AppendUvarint(dst, prefix)
	dst = binary.AppendUvarint(dst, uint64(len(key)))
	dst = append(dst, key...)
	dst = binary.AppendUvarint(dst, uint64(len(value)))
	return append(dst, value...)
}

func rawTable(tb testing.TB, raw, first []byte, count uint32) []byte {
	tb.Helper()
	var data bytes.Buffer
	w := NewTableWriter(&data)
	w.buf = raw
	w.first = first
	w.count = count
	w.total = count
	require.NoError(tb, w.Finish())
	return data.Bytes()
}

func openTestTable(tb testing.TB, data []byte) *Table {
	tb.Helper()
	b := backend.Memory()
	require.NoError(tb, b.Write(context.Background(), "index", data))
	r, err := OpenTable(context.Background(), b, "index")
	require.NoError(tb, err)
	return r
}

func TestTableValidChecksumMalformedRecords(t *testing.T) {
	t.Parallel()
	good := appendRecord(nil, 0, []byte("a"), nil)
	cases := []struct {
		name       string
		raw, first []byte
		count      uint32
	}{
		{"duplicate", appendRecord(bytes.Clone(good), 1, nil, nil), []byte("a"), 2},
		{"descending", appendRecord(bytes.Clone(good), 0, []byte("0"), nil), []byte("a"), 2},
		{"bad prefix", appendRecord(bytes.Clone(good), 2, []byte("b"), nil), []byte("a"), 2},
		{"prefix integer", []byte{128, 128, 128}, []byte("a"), 1},
		{"suffix integer", []byte{0, 128, 128}, []byte("a"), 1},
		{"suffix bounds", []byte{0, 127, 0}, []byte("a"), 1},
		{"value integer", []byte{0, 1, 'a', 128}, []byte("a"), 1},
		{"value bounds", []byte{0, 1, 'a', 127}, []byte("a"), 1},
		{"trailing bytes", append(bytes.Clone(good), 0), []byte("a"), 1},
		{"first key mismatch", good, []byte("b"), 1},
		{"too many records", appendRecord(nil, 0, []byte("a"), []byte("1234")), []byte("a"), 2},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			r := openTestTable(t, rawTable(t, tc.raw, tc.first, tc.count))
			c := r.Cursor(t.Context())
			require.False(t, c.Seek([]byte("a")))
			require.ErrorIs(t, c.Err(), ErrCorrupt)
			require.False(t, c.Next())
			require.False(t, c.Seek(nil))
		})
	}
}

func TestTableWriterTerminalState(t *testing.T) {
	t.Parallel()
	for _, mode := range []string{"finish twice", "append after finish", "ordering sticky", "count limit", "record limit", "negative write", "oversize write"} {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			var out bytes.Buffer
			w := NewTableWriter(&out)
			require.NoError(t, w.Append([]byte("a"), nil))
			var err error
			switch mode {
			case "finish twice":
				require.NoError(t, w.Finish())
				before := out.Len()
				err = w.Finish()
				require.Equal(t, before, out.Len())
			case "append after finish":
				require.NoError(t, w.Finish())
				before := out.Len()
				err = w.Append([]byte("b"), nil)
				require.Equal(t, before, out.Len())
			case "ordering sticky":
				err = w.Append([]byte("0"), nil)
			case "count limit":
				w.total = math.MaxUint32
				err = w.Append([]byte("b"), nil)
			case "record limit":
				err = w.Append(make([]byte, maxPageSize), nil)
			case "negative write":
				w.dst = invalidCountWriter{-1}
				err = w.Finish()
				require.ErrorIs(t, err, io.ErrShortWrite)
			case "oversize write":
				w.dst = invalidCountWriter{math.MaxInt}
				err = w.Finish()
				require.ErrorIs(t, err, io.ErrShortWrite)
			}
			require.Error(t, err)
			require.Same(t, err, w.Append([]byte("z"), nil))
			require.Same(t, err, w.Finish())
		})
	}
}

type invalidCountWriter struct{ n int }

func (w invalidCountWriter) Write([]byte) (int, error) { return w.n, nil }

func TestTableFooterBounds(t *testing.T) {
	t.Parallel()
	good := tableData(t, 2, 100)
	for _, tc := range []struct {
		name string
		edit func([]byte)
	}{
		{"reserved", func(f []byte) { f[64] = 1 }},
		{"empty raw", func(f []byte) { binary.BigEndian.PutUint32(f[24:], 0) }},
		{"excess count", func(f []byte) { binary.BigEndian.PutUint32(f[20:], math.MaxUint32) }},
		{"excess series", func(f []byte) { binary.BigEndian.PutUint32(f[28:], math.MaxUint32) }},
		{"empty offset", func(f []byte) { binary.BigEndian.PutUint32(f[16:], 0); binary.BigEndian.PutUint32(f[20:], 0) }},
		{"empty page count", func(f []byte) { binary.BigEndian.PutUint32(f[16:], 0) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			data := bytes.Clone(good)
			f := data[len(data)-footerSize:]
			tc.edit(f)
			binary.BigEndian.PutUint32(f[68:], crc32.Checksum(f[:68], checksumTable))
			b := backend.Memory()
			require.NoError(t, b.Write(t.Context(), "index", data))
			_, err := OpenTable(t.Context(), b, "index")
			require.ErrorIs(t, err, ErrCorrupt)
		})
	}
}

func TestCursorWorkspaceLongKeys(t *testing.T) {
	t.Parallel()
	var data bytes.Buffer
	w := NewTableWriter(&data)
	w.limit = 200 << 10
	for i := range 4 {
		key := bytes.Repeat([]byte{byte(i + 1)}, 96<<10)
		value := bytes.Repeat([]byte{byte(i + 9)}, 32<<10)
		require.NoError(t, w.Append(key, value))
	}
	require.NoError(t, w.Finish())
	r := openTestTable(t, data.Bytes())
	c := r.Cursor(t.Context())
	for c.Next() {
		retained := cap(c.key) + cap(c.raw) + cap(c.packed) + cap(c.first) + cap(c.directory) + cap(c.last)
		require.LessOrEqual(t, int64(retained), r.MaxPageBytes())
	}
	require.NoError(t, c.Err())
}

func TestOpenTableNativeSizeAndOpaqueBudget(t *testing.T) {
	t.Parallel()
	counted := backendtest.NewByteCounter(backend.Memory())
	data := tableData(t, 100, 100)
	require.NoError(t, counted.Write(t.Context(), "index", data))
	_, err := OpenTable(t.Context(), counted, "index")
	require.NoError(t, err)
	require.Equal(t, int64(footerSize), counted.Bytes())
	opaque := &opaqueTable{Backend: counted}
	ctx := readbudget.With(t.Context(), readbudget.New(1<<20))
	counted.Reset()
	_, err = OpenTable(ctx, opaque, "index")
	require.ErrorIs(t, err, readbudget.ErrExceeded)
	require.Zero(t, counted.Bytes())
	noRanges := faultbackend.Wrap(counted)
	_, err = OpenTable(ctx, noRanges, "index")
	require.ErrorIs(t, err, readbudget.ErrExceeded)
	require.Empty(t, noRanges.Ops(), "native sizes must not hide a whole-object range fallback")
	require.Zero(t, counted.Bytes())
}

type opaqueTable struct{ backend.Backend }

func FuzzTableRoundTrip(f *testing.F) {
	f.Add([]byte("a\x00b\x00c"), []byte("value"), uint16(30))
	f.Add([]byte{0, 255, 0, 1}, []byte{}, uint16(100))
	f.Fuzz(func(t *testing.T, input, value []byte, limit uint16) {
		if len(input)+len(value) > 64<<10 {
			t.Skip()
		}
		keys := bytes.Split(input, []byte{0})
		slices.SortFunc(keys, bytes.Compare)
		keys = slices.CompactFunc(keys, bytes.Equal)
		var data bytes.Buffer
		w := NewTableWriter(&data)
		w.limit = max(int(limit), 1)
		for _, key := range keys {
			if err := w.Append(key, value); err != nil {
				t.Fatal(err)
			}
		}
		if err := w.Finish(); err != nil {
			t.Fatal(err)
		}
		b := backend.Memory()
		if err := b.Write(t.Context(), "index", data.Bytes()); err != nil {
			t.Fatal(err)
		}
		r, err := OpenTable(t.Context(), b, "index")
		if err != nil {
			t.Fatal(err)
		}
		c := r.Cursor(t.Context())
		for _, key := range keys {
			if !c.Next() || !bytes.Equal(c.Key(), key) || !bytes.Equal(c.Value(), value) {
				t.Fatalf("roundtrip key %x: %v", key, c.Err())
			}
		}
		if c.Next() || c.Err() != nil {
			t.Fatalf("tail: %v", c.Err())
		}
		for _, key := range keys {
			if !c.Seek(key) || !bytes.Equal(c.Key(), key) {
				t.Fatalf("seek %x: %v", key, c.Err())
			}
		}
	})
}

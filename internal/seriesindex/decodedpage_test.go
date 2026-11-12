package seriesindex

import (
	"bytes"
	"fmt"
	"strconv"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
)

func TestDecodedPagesMatchUncached(t *testing.T) {
	t.Parallel()
	for _, prefixLen := range []int{0, 8192} {
		t.Run(strconv.Itoa(prefixLen), func(t *testing.T) {
			t.Parallel()
			var data bytes.Buffer
			w := NewTableWriter(&data)
			w.limit = 24 << 10
			prefix := bytes.Repeat([]byte{'a'}, prefixLen)
			keys := make([][]byte, 1024)
			for i := range keys {
				keys[i] = fmt.Appendf(bytes.Clone(prefix), "key-%08d", i)
				require.NoError(t, w.Append(keys[i], fmt.Appendf(nil, "value-%08d", i)))
			}
			require.NoError(t, w.Finish())
			raw := backend.Memory()
			require.NoError(t, raw.Write(t.Context(), "index", data.Bytes()))
			cached := backend.Cached(raw, 16<<20)
			plain, err := OpenTable(t.Context(), raw, "index")
			require.NoError(t, err)
			table, err := OpenTable(t.Context(), cached, "index")
			require.NoError(t, err)
			for range 2 {
				a, b := plain.Cursor(t.Context()), table.Cursor(t.Context())
				for _, i := range []int{513, 3, 1023, 17, 16, 18, 900} {
					for _, key := range [][]byte{keys[i], append(bytes.Clone(keys[i]), 'x')} {
						require.Equal(t, a.Seek(key), b.Seek(key))
						require.Equal(t, a.Key(), b.Key())
						require.Equal(t, a.Value(), b.Value())
						require.Equal(t, a.Next(), b.Next())
						require.Equal(t, a.Key(), b.Key())
						require.Equal(t, a.Value(), b.Value())
					}
				}
				require.NoError(t, b.Err())
				require.NotEmpty(t, b.decoded)
				if prefixLen == 0 {
					require.EqualValues(t, decodedExpanded, b.decoded[0])
				} else {
					require.Zero(t, b.decoded[0])
				}
				retained := cap(b.decoded) + cap(b.first) + cap(b.directory) + cap(b.sought)
				if b.decoded[0] == 0 {
					retained += cap(b.key)
				}
				require.LessOrEqual(t, int64(retained), table.MaxPageBytes())
				b.reset(t.Context(), plain)
				for i := range keys {
					require.True(t, b.Next())
					require.Equal(t, keys[i], b.Key())
				}
				require.False(t, b.Next())
				require.NoError(t, b.Err())
				b.reset(t.Context(), table)
				require.True(t, b.Seek(nil))
				for i := range keys {
					if i == 0 {
						require.Equal(t, keys[i], b.Key())
						continue
					}
					require.True(t, b.Next())
					require.Equal(t, keys[i], b.Key())
				}
				require.False(t, b.Next())
				require.NoError(t, b.Err())
			}
			require.LessOrEqual(t, cached.(interface{ Stats() backend.CacheStats }).Stats().Bytes, int64(16<<20))
			var wg sync.WaitGroup
			for j := range 8 {
				wg.Go(func() {
					c := table.Cursor(t.Context())
					for i := range 1024 {
						if !c.Seek(keys[(i*7919+j)%len(keys)]) {
							t.Error(c.Err())
							return
						}
					}
				})
			}
			wg.Wait()
		})
	}
}

func TestDecodedPagesValidateDirectoryOnHit(t *testing.T) {
	t.Parallel()
	b := backend.Cached(backend.Memory(), 1<<20)
	require.NoError(t, b.Write(t.Context(), "index", tableData(t, 100, 64<<10)))
	table, err := OpenTable(t.Context(), b, "index")
	require.NoError(t, err)
	c := table.Cursor(t.Context())
	require.True(t, c.Next())
	p, err := c.entry(0)
	require.NoError(t, err)
	p.count--
	require.False(t, table.Cursor(t.Context()).loadDecoded(b.(backend.DecodedPageViewer), 0, p))
	p.count++
	p.first = []byte("wrong")
	c = table.Cursor(t.Context())
	require.False(t, c.loadDecoded(b.(backend.DecodedPageViewer), 0, p))
	require.ErrorIs(t, c.Err(), ErrCorrupt)
}

func FuzzDecodedPageRead(f *testing.F) {
	f.Add(tableData(f, 10, 64), []byte("key-00000005"))
	f.Add(rawTable(f, []byte{0, 1, 'a', 0}, []byte("a"), 1), []byte("a"))
	f.Fuzz(func(t *testing.T, data, key []byte) {
		if len(data) > 1<<20 || len(key) > 4096 {
			t.Skip()
		}
		raw := backend.Memory()
		require.NoError(t, raw.Write(t.Context(), "index", data))
		b := backend.Cached(raw, 4<<20)
		table, err := OpenTable(t.Context(), b, "index")
		if err != nil || table.MaxPageBytes() > 16<<20 {
			return
		}
		for range 2 {
			c := table.Cursor(t.Context())
			if c.Seek(key) {
				previous := bytes.Clone(c.Key())
				for c.Next() {
					if bytes.Compare(previous, c.Key()) >= 0 {
						t.Fatal("unordered decoded keys")
					}
					previous = append(previous[:0], c.Key()...)
				}
			}
		}
	})
}

func TestDecodedPageExpansionBound(t *testing.T) {
	t.Parallel()
	first := bytes.Repeat([]byte{'a'}, 1<<16)
	first[len(first)-1] = 0
	raw := appendRecord(nil, 0, first, nil)
	for i := range 255 {
		raw = appendRecord(raw, (1<<16)-1, []byte{byte(i + 1)}, nil)
	}
	// All records remain sorted but full key materialization would use 16 MiB.
	data := rawTable(t, raw, first, 256)
	b := backend.Cached(backend.Memory(), 2<<20)
	require.NoError(t, b.Write(t.Context(), "index", data))
	table, err := OpenTable(t.Context(), b, "index")
	require.NoError(t, err)
	c := table.Cursor(t.Context())
	require.True(t, c.Seek(nil))
	require.Zero(t, c.decoded[0])
	require.LessOrEqual(t, len(c.decoded), 2*len(raw)+decodedHeader)
	for range 255 {
		require.True(t, c.Next())
	}
	require.False(t, c.Next())
	require.NoError(t, c.Err())
}

func TestDecodedPagesRejectCorruption(t *testing.T) {
	t.Parallel()
	data := tableData(t, 10, 100)
	for i := range data {
		bad := bytes.Clone(data)
		bad[i] ^= 1
		b := backend.Cached(backend.Memory(), 1<<20)
		require.NoError(t, b.Write(t.Context(), "index", bad))
		table, err := OpenTable(t.Context(), b, "index")
		if err == nil {
			c := table.Cursor(t.Context())
			c.Seek(nil)
			for c.Next() {
			}
			err = c.Err()
		}
		require.Error(t, err, "corrupt byte %d", i)
	}
	good := appendRecord(nil, 0, []byte("a"), nil)
	for _, raw := range [][]byte{
		appendRecord(bytes.Clone(good), 1, nil, nil),
		appendRecord(bytes.Clone(good), 0, []byte("0"), nil),
		appendRecord(bytes.Clone(good), 2, []byte("b"), nil),
	} {
		b := backend.Cached(backend.Memory(), 1<<20)
		require.NoError(t, b.Write(t.Context(), "index", rawTable(t, raw, []byte("a"), 2)))
		table, err := OpenTable(t.Context(), b, "index")
		require.NoError(t, err)
		for range 2 {
			c := table.Cursor(t.Context())
			require.False(t, c.Seek(nil), "no partially validated page can be exposed or cached")
			require.ErrorIs(t, c.Err(), ErrCorrupt)
		}
	}
}

//nolint:paralleltest // AllocsPerRun changes process GOMAXPROCS.
func TestDecodedPageReusesScratchWithoutAliasing(t *testing.T) {
	be := backend.Memory()
	require.NoError(t, be.Write(t.Context(), "index", tableData(t, 1000, 64<<10)))
	table, err := OpenTable(t.Context(), backend.Cached(be, 1<<20), "index")
	require.NoError(t, err)
	c := table.Cursor(t.Context())
	p, err := c.entry(0)
	require.NoError(t, err)
	c.decoding = p
	packed, err := backend.ReadAt(t.Context(), be, "index", p.offset, int64(p.packed))
	require.NoError(t, err)
	first, err := c.DecodePage(packed)
	require.NoError(t, err)
	want := bytes.Clone(first)
	require.LessOrEqual(t, testing.AllocsPerRun(100, func() {
		if _, err := c.DecodePage(packed); err != nil {
			panic(err)
		}
	}), float64(1))
	require.Equal(t, want, first, "cached representation must not alias reusable scratch")
	c.reset(t.Context(), table)
	require.NotZero(t, cap(c.decodeScratch))
	plain, err := OpenTable(t.Context(), be, "index")
	require.NoError(t, err)
	c.reset(t.Context(), plain)
	require.Nil(t, c.decodeScratch, "bare-backend admission excludes cold decoder scratch")
}

func TestStreamingPageSeeksPreserveCache(t *testing.T) {
	t.Parallel()
	be := backend.Memory()
	require.NoError(t, be.Write(t.Context(), "index", tableData(t, 4096, 1024)))
	table, err := OpenTable(t.Context(), backend.Cached(be, 1<<20), "index")
	require.NoError(t, err)
	c := table.Cursor(t.Context())
	require.True(t, c.Seek([]byte("key-00002048")))
	require.NotNil(t, c.decoded)
	c.streaming = true
	for _, i := range []int{0, 1, 127, 128, 129, 4095, 2048, 2} {
		key := fmt.Appendf(nil, "key-%08d", i)
		require.True(t, c.Seek(key))
		require.Nil(t, c.decoded)
		require.Equal(t, fmt.Sprintf("value-%08d", i), string(c.Value()))
		other := table.Cursor(t.Context())
		require.True(t, other.Seek(key))
		require.Equal(t, c.Value(), other.Value())
	}
}

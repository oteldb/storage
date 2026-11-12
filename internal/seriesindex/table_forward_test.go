package seriesindex

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
)

func TestTableForwardSeekReusesPage(t *testing.T) {
	t.Parallel()
	be := &rangedTable{Backend: backend.Memory(), data: tableData(t, 1000, 64<<10)}
	r, err := OpenTable(t.Context(), be, "index")
	require.NoError(t, err)
	c := r.Cursor(t.Context())
	require.True(t, c.Seek([]byte("key-00000000")))
	loaded := be.read
	for _, i := range []int{1, 1, 15, 99, 999} {
		key := fmt.Sprintf("key-%08d", i)
		require.True(t, c.Seek([]byte(key)))
		require.Equal(t, key, string(c.Key()))
		require.Equal(t, fmt.Sprintf("value-%08d", i), string(c.Value()))
	}
	require.Equal(t, loaded, be.read, "forward probes inside a validated page need no directory reads")
	require.True(t, c.Seek([]byte("key-00000003")))
	require.Equal(t, "value-00000003", string(c.Value()))
	require.Greater(t, be.read, loaded, "backward probes revalidate and rewind the page")
	require.True(t, c.Seek([]byte("key-00000003x")))
	require.Equal(t, "key-00000004", string(c.Key()))
}

func FuzzTableForwardSeek(f *testing.F) {
	f.Add([]byte{0, 1, 128, 255})
	f.Add([]byte{255, 1, 1, 0})
	f.Add([]byte{3, 5, 7, 9, 11, 13})
	data := tableData(f, 1000, 1024)
	f.Fuzz(func(t *testing.T, probes []byte) {
		if len(probes) > 256 {
			t.Skip()
		}
		be := &rangedTable{Backend: backend.Memory(), data: data}
		r, err := OpenTable(t.Context(), be, "index")
		require.NoError(t, err)
		c := r.Cursor(t.Context())
		for _, probe := range probes {
			i := int(probe) * 997 % 1000
			key := fmt.Appendf(nil, "key-%08d", i)
			want := i
			if probe&1 != 0 {
				key = append(key, 'x')
				want++
			}
			if want >= 1000 {
				require.False(t, c.Seek(key))
				continue
			}
			require.True(t, c.Seek(key))
			require.Equal(t, fmt.Sprintf("value-%08d", want), string(c.Value()))
			if probe&2 != 0 {
				c.Next()
			}
		}
	})
}

func TestTableForwardMissReusesLowerBound(t *testing.T) {
	t.Parallel()
	be := &rangedTable{Backend: backend.Memory(), data: tableData(t, 1000, 64<<10)}
	table, err := OpenTable(t.Context(), be, "index")
	require.NoError(t, err)
	cursor := table.Cursor(t.Context())
	require.True(t, cursor.Seek([]byte("key-00000003a")))
	require.Equal(t, "key-00000004", string(cursor.Key()))
	loaded := be.read
	for _, key := range []string{"key-00000003a", "key-00000003b", "key-00000003z", "key-00000004"} {
		require.True(t, cursor.Seek([]byte(key)))
		require.Equal(t, "key-00000004", string(cursor.Key()))
	}
	require.Equal(t, loaded, be.read, "same-gap forward probes must not reread or revalidate pages")
	require.True(t, cursor.Next())
	require.Equal(t, "key-00000005", string(cursor.Key()))
	require.True(t, cursor.Seek([]byte("key-00000003z")))
	require.Equal(t, "key-00000004", string(cursor.Key()), "Next invalidates the previous lower-bound proof")
	require.True(t, cursor.Seek([]byte("key-00000002z")))
	require.Equal(t, "key-00000003", string(cursor.Key()), "backward seeks retain ordinary lower-bound semantics")
}

func TestTableOversizedSeekDoesNotRetainQuery(t *testing.T) {
	t.Parallel()
	be := &rangedTable{Backend: backend.Memory(), data: tableData(t, 10, 1024)}
	table, err := OpenTable(t.Context(), be, "index")
	require.NoError(t, err)
	cursor := table.Cursor(t.Context())
	require.False(t, cursor.Seek(bytes.Repeat([]byte("z"), int(table.maxRaw)+1)))
	require.Zero(t, cap(cursor.sought))
}

package seriesindex

import (
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
			require.True(t, c.Seek(fmt.Appendf(nil, "key-%08d", i)))
			require.Equal(t, fmt.Sprintf("value-%08d", i), string(c.Value()))
		}
	})
}

package profile

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNodeCodecRoundTrip(t *testing.T) {
	t.Parallel()
	root := &Node{Name: "query", Dur: 5 * time.Millisecond, Children: []*Node{
		{Name: "engine.fetch", Dur: 3 * time.Millisecond, Counters: map[string]int64{"rows": 10, "parts_scanned": 2}, Children: []*Node{
			{Name: "scan", Dur: 2 * time.Millisecond, Counters: map[string]int64{"rows": 10}},
		}},
	}}

	got, tail, err := Decode(root.Encode(nil))
	require.NoError(t, err)
	assert.Empty(t, tail)
	assert.Equal(t, root.Name, got.Name)
	require.Len(t, got.Children, 1)
	assert.Equal(t, int64(10), got.Children[0].Counters["rows"])
	require.Len(t, got.Children[0].Children, 1)
	assert.Equal(t, "scan", got.Children[0].Children[0].Name)
}

func FuzzNodeDecode(f *testing.F) {
	f.Add((&Node{Name: "x", Counters: map[string]int64{"a": 1}}).Encode(nil))
	f.Fuzz(func(_ *testing.T, data []byte) {
		_, _, _ = Decode(data) // must never panic
	})
}

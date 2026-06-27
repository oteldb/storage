package cluster_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/cluster"
	"github.com/oteldb/storage/signal"
)

func ser(svc string) signal.Series {
	return signal.Series{Resource: signal.Resource{Attributes: signal.NewAttributes(
		signal.KeyValue{Key: []byte("service.name"), Value: signal.StringValue([]byte(svc))},
	)}}
}

func TestSeriesListRoundTrip(t *testing.T) {
	t.Parallel()

	in := []signal.Series{ser("api"), ser("web")}

	out, err := cluster.DecodeSeriesList(cluster.EncodeSeriesList(in))
	require.NoError(t, err)
	require.Len(t, out, 2)
	assert.Equal(t, in[0].Hash(), out[0].Hash())
	assert.Equal(t, in[1].Hash(), out[1].Hash())
}

func TestSideTablesRoundTrip(t *testing.T) {
	t.Parallel()

	in := map[string][]byte{"stacks": []byte("abc"), "strings": {}, "functions": []byte("\x00\x01\x02")}

	out, err := cluster.DecodeSideTables(cluster.EncodeSideTables(in))
	require.NoError(t, err)
	assert.Equal(t, []byte("abc"), out["stacks"])
	assert.Equal(t, []byte("\x00\x01\x02"), out["functions"])
	assert.Empty(t, out["strings"])
}

func TestKeyListRoundTrip(t *testing.T) {
	t.Parallel()

	in := []cluster.KeyInfo{
		{Key: []byte("http.method"), Scope: 0b101},
		{Key: []byte{}, Scope: 0}, // empty key, zero scope
		{Key: []byte("\x00\xff\x01"), Scope: 0b010},
	}

	out, err := cluster.DecodeKeyList(cluster.EncodeKeyList(in))
	require.NoError(t, err)
	require.Len(t, out, 3)
	assert.Equal(t, []byte("http.method"), out[0].Key)
	assert.Equal(t, uint8(0b101), out[0].Scope)
	assert.Empty(t, out[1].Key)
	assert.Equal(t, []byte("\x00\xff\x01"), out[2].Key)
	assert.Equal(t, uint8(0b010), out[2].Scope)
}

// FuzzDecodeEnum: arbitrary bytes to the peer-response decoders must error or decode, never panic.
func FuzzDecodeEnum(f *testing.F) {
	f.Add(cluster.EncodeSeriesList([]signal.Series{ser("api")}))
	f.Add(cluster.EncodeSideTables(map[string][]byte{"stacks": []byte("x")}))
	f.Add(cluster.EncodeKeyList([]cluster.KeyInfo{{Key: []byte("k"), Scope: 1}}))
	f.Add([]byte{0x02, 0xff})

	f.Fuzz(func(_ *testing.T, data []byte) {
		_, _ = cluster.DecodeSeriesList(data)
		_, _ = cluster.DecodeSideTables(data)
		_, _ = cluster.DecodeKeyList(data)
	})
}

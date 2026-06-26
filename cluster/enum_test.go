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

// FuzzDecodeEnum: arbitrary bytes to the peer-response decoders must error or decode, never panic.
func FuzzDecodeEnum(f *testing.F) {
	f.Add(cluster.EncodeSeriesList([]signal.Series{ser("api")}))
	f.Add(cluster.EncodeSideTables(map[string][]byte{"stacks": []byte("x")}))
	f.Add([]byte{0x02, 0xff})

	f.Fuzz(func(_ *testing.T, data []byte) {
		_, _ = cluster.DecodeSeriesList(data)
		_, _ = cluster.DecodeSideTables(data)
	})
}

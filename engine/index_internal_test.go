package engine

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/signal"
)

func TestSeqOfPrefix(t *testing.T) {
	t.Parallel()

	assert.Equal(t, 7, seqOfPrefix("default/metrics/0000000007"))
	assert.Equal(t, 0, seqOfPrefix("p/0000000000"))
	assert.Equal(t, -1, seqOfPrefix("default/metrics/not-a-number"))
}

func TestEncodeDecodeSeriesSetRoundTrip(t *testing.T) {
	t.Parallel()

	h := newHead()
	h.registerSeries(signal.Series{Attributes: signal.NewAttributes(
		signal.KeyValue{Key: []byte("job"), Value: signal.StringValue([]byte("api"))},
	)})
	h.registerSeries(signal.Series{Attributes: signal.NewAttributes(
		signal.KeyValue{Key: []byte("job"), Value: signal.StringValue([]byte("web"))},
	)})

	var got []signal.Series
	require.NoError(t, decodeSeriesSet(encodeSeriesSet(h.series), func(s signal.Series) {
		got = append(got, s)
	}))
	assert.Len(t, got, 2)
}

func TestDecodeSeriesSetRejectsCorrupt(t *testing.T) {
	t.Parallel()

	noop := func(signal.Series) {}
	cases := map[string][]byte{
		"empty":           {},
		"truncated count": {0x80}, // incomplete uvarint
		"missing record":  {2},    // claims 2 records, none follow
		"bad length":      {1, 200},
	}
	for name, data := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			require.Error(t, decodeSeriesSet(data, noop))
		})
	}
}

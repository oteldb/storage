package logengine

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/index/series"
	"github.com/oteldb/storage/signal"
)

func TestSeriesSetCodecRoundTrip(t *testing.T) {
	t.Parallel()

	ix := series.New()
	a := signal.Series{Resource: signal.Resource{Attributes: signal.NewAttributes(
		signal.KeyValue{Key: []byte("service.name"), Value: signal.StringValue([]byte("api"))},
	)}}
	b := signal.Series{Scope: signal.Scope{Name: []byte("lib")}}
	ix.Add(a)
	ix.Add(b)

	var got []signal.Series
	require.NoError(t, decodeSeriesSet(encodeSeriesSet(ix), func(s signal.Series) { got = append(got, s) }))
	assert.Len(t, got, 2, "every identity round-trips")
}

func TestDecodeSeriesSetTruncated(t *testing.T) {
	t.Parallel()

	require.Error(t, decodeSeriesSet(nil, func(signal.Series) {}), "empty ⇒ bad count")
	require.Error(t, decodeSeriesSet([]byte{0x02, 0xff}, func(signal.Series) {}), "truncated length")
}

func TestSeqOfPrefix(t *testing.T) {
	t.Parallel()

	assert.Equal(t, 42, seqOfPrefix("tenant/logs/0000000042"))
	assert.Equal(t, -1, seqOfPrefix("tenant/logs/notanumber"))
}

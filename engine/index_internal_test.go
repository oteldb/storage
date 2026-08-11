package engine

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/index/series"
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
	require.NoError(t, decodeSeriesSet(encodeSeriesSet(nil, h.series), func(s signal.Series) {
		got = append(got, s)
	}))
	assert.Len(t, got, 2)

	// Encoding into a hinted buffer (the flush path) appends in place and round-trips the same set.
	enc := encodeSeriesSet(make([]byte, 0, 4096), h.series)
	assert.Equal(t, 4096, cap(enc), "a sufficient hint must not reallocate")

	var hinted []signal.Series

	require.NoError(t, decodeSeriesSet(enc, func(s signal.Series) {
		hinted = append(hinted, s)
	}))
	assert.Len(t, hinted, 2)
}

func TestSeriesSetSizeHint(t *testing.T) {
	t.Parallel()

	assert.Equal(t, 0, seriesSetSizeHint(0, 0, 100))
	assert.Equal(t, 0, seriesSetSizeHint(1000, 0, 100))
	// 10 B/series over 200 series, with the margin.
	assert.Greater(t, seriesSetSizeHint(1000, 100, 200), 2000)
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

func benchSeriesIndex(b *testing.B, n int) *series.Index {
	b.Helper()

	h := newHead()
	for i := range n {
		h.registerSeries(signal.Series{Attributes: signal.NewAttributes(
			signal.KeyValue{Key: []byte("job"), Value: signal.StringValue([]byte("api"))},
			signal.KeyValue{Key: []byte("instance"), Value: signal.StringValue(fmt.Appendf(nil, "host-%06d:9100", i))},
		)})
	}

	return h.series
}

func BenchmarkEncodeSeriesSet(b *testing.B) {
	ix := benchSeriesIndex(b, 100_000)
	size := len(encodeSeriesSet(nil, ix))

	b.ReportAllocs()
	b.SetBytes(int64(size))
	b.ResetTimer()

	for range b.N {
		encodeSeriesSet(make([]byte, 0, size), ix)
	}
}

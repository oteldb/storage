package cluster_test

import (
	"encoding/binary"
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/cluster"
	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/signal"
)

// hugeCount is a length prefix no payload can back: allocating from it panics the process
// ("makeslice: cap out of range") instead of rejecting the message.
const hugeCount = uint64(1) << 62

// fetchReqWithMatcherCount builds a fetch request whose matcher count is n, with nothing after it.
func fetchReqWithMatcherCount(n uint64) []byte {
	buf := []byte{byte(signal.Metric)}
	buf = binary.AppendUvarint(buf, 0) // empty tenant
	buf = binary.AppendVarint(buf, 0)  // start
	buf = binary.AppendVarint(buf, 0)  // end

	return binary.AppendUvarint(buf, n)
}

// TestDecodeRejectsOversizedCounts is #473: a length prefix larger than the bytes that could back
// it must be an error, not an allocation. The decoders run on both sides of the read RPC — batches
// on the query coordinator, the request on the node being queried — so a panic here is a remote
// process kill in either direction.
func TestDecodeRejectsOversizedCounts(t *testing.T) {
	t.Parallel()

	t.Run("Batches", func(t *testing.T) {
		t.Parallel()
		_, err := cluster.DecodeBatches(binary.AppendUvarint(nil, hugeCount))
		require.Error(t, err)
	})

	t.Run("LogBatches", func(t *testing.T) {
		t.Parallel()
		_, err := cluster.DecodeLogBatches(binary.AppendUvarint(nil, hugeCount))
		require.Error(t, err)
	})

	t.Run("SampleCount", func(t *testing.T) {
		t.Parallel()

		id := signal.Series{Attributes: signal.NewAttributes(
			signal.KeyValue{Key: []byte("job"), Value: signal.StringValue([]byte("api"))},
		)}.AppendHashInput(nil)

		buf := binary.AppendUvarint(nil, 1)
		buf = binary.AppendUvarint(buf, uint64(len(id)))
		buf = append(buf, id...)
		buf = binary.AppendUvarint(buf, hugeCount)

		_, err := cluster.DecodeBatches(buf)
		require.Error(t, err)
	})

	t.Run("FetchRequestMatchers", func(t *testing.T) {
		t.Parallel()
		_, err := cluster.ParseFetchRequest(fetchReqWithMatcherCount(hugeCount))
		require.Error(t, err)
	})

	t.Run("FetchRequestConditions", func(t *testing.T) {
		t.Parallel()

		buf := fetchReqWithMatcherCount(0)
		buf = binary.AppendUvarint(buf, hugeCount)

		_, err := cluster.ParseFetchRequest(buf)
		require.Error(t, err)
	})

	t.Run("AggregateMatchers", func(t *testing.T) {
		t.Parallel()

		buf := binary.AppendUvarint(nil, 0) // empty tenant
		for range 3 {                       // start, end, step
			buf = binary.AppendVarint(buf, 0)
		}
		buf = binary.AppendUvarint(buf, hugeCount)

		_, _, _, _, _, err := cluster.DecodeAggregateRequest(buf)
		require.Error(t, err)
	})
}

// TestDecodeBatchesRoundTrip guards the guards: a well-formed payload still decodes.
func TestDecodeBatchesRoundTrip(t *testing.T) {
	t.Parallel()

	s := signal.Series{Attributes: signal.NewAttributes(
		signal.KeyValue{Key: []byte("job"), Value: signal.StringValue([]byte("api"))},
	)}
	in := []*fetch.Batch{{Series: s, Timestamps: []int64{1, 2}, Values: []float64{1.5, math.Inf(1)}}}

	out, err := cluster.DecodeBatches(cluster.EncodeBatches(in))
	require.NoError(t, err)
	require.Len(t, out, 1)
	assert.Equal(t, []int64{1, 2}, out[0].Timestamps)
	assert.Equal(t, []float64{1.5, math.Inf(1)}, out[0].Values)

	logs := []*fetch.Batch{{Series: s, Timestamps: []int64{7}, Columns: []fetch.NamedColumn{
		{Name: "body", Kind: fetch.KindBytes, Bytes: [][]byte{[]byte("hi")}},
		{Name: "sev", Kind: fetch.KindInt64, Int64: []int64{9}},
		{Name: "val", Kind: fetch.KindFloat64, Float64: []float64{2.5}},
	}}}

	enc, err := cluster.EncodeLogBatches(logs)
	require.NoError(t, err)

	got, err := cluster.DecodeLogBatches(enc)
	require.NoError(t, err)
	require.Len(t, got, 1)
	assert.Equal(t, []int64{7}, got[0].Timestamps)
	require.Len(t, got[0].Columns, 3)
	assert.Equal(t, [][]byte{[]byte("hi")}, got[0].Columns[0].Bytes)
	assert.Equal(t, []int64{9}, got[0].Columns[1].Int64)
	assert.Equal(t, []float64{2.5}, got[0].Columns[2].Float64)
}

// FuzzDecodeRead: arbitrary bytes to the read RPC's decoders — the batch codecs a coordinator runs
// on a peer's response and the request codec a peer runs on ours — must error or decode, never
// panic.
func FuzzDecodeRead(f *testing.F) {
	s := signal.Series{Attributes: signal.NewAttributes(
		signal.KeyValue{Key: []byte("job"), Value: signal.StringValue([]byte("api"))},
	)}

	f.Add(cluster.EncodeBatches([]*fetch.Batch{{Series: s, Timestamps: []int64{1}, Values: []float64{2}}}))

	logs, err := cluster.EncodeLogBatches([]*fetch.Batch{{Series: s, Timestamps: []int64{1}, Columns: []fetch.NamedColumn{
		{Name: "body", Kind: fetch.KindBytes, Bytes: [][]byte{[]byte("hi")}},
	}}})
	require.NoError(f, err)
	f.Add(logs)

	f.Add(cluster.FetchRequest{
		Signal: signal.Log, Tenant: "t", Start: 1, End: 2,
		Equal:      []fetch.EqualMatcher{{Name: "job", Value: "api"}},
		Conditions: []cluster.ConditionHint{{Column: "body", Equal: fetch.EqualMatcher{Name: "body", Value: "x"}}},
	}.Encode())
	f.Add(fetchReqWithMatcherCount(hugeCount))
	f.Add(binary.AppendUvarint(nil, hugeCount))
	f.Add([]byte{0x02, 0xff})

	f.Fuzz(func(_ *testing.T, data []byte) {
		_, _ = cluster.DecodeBatches(data)
		_, _ = cluster.DecodeLogBatches(data)
		_, _ = cluster.ParseFetchRequest(data)
		_, _, _, _, _, _ = cluster.DecodeAggregateRequest(data)
	})
}

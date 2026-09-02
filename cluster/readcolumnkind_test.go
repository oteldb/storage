package cluster_test

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/cluster"
	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/signal"
)

func kindBatch(cols ...fetch.NamedColumn) *fetch.Batch {
	s := signal.Series{Resource: signal.Resource{Attributes: signal.NewAttributes(
		signal.KeyValue{Key: []byte("service.name"), Value: signal.StringValue([]byte("api"))},
	)}}

	return &fetch.Batch{ID: s.Hash(), Series: s, Timestamps: []int64{1}, Columns: cols}
}

func roundTrip(t *testing.T, b *fetch.Batch) *fetch.Batch {
	t.Helper()

	enc, err := cluster.EncodeLogBatches([]*fetch.Batch{b})
	require.NoError(t, err)

	out, err := cluster.DecodeLogBatches(enc)
	require.NoError(t, err)
	require.Len(t, out, 1)

	return out[0]
}

func TestLogBatchColumnKindRoundTrip(t *testing.T) {
	t.Parallel()

	got := roundTrip(t, kindBatch(
		fetch.Int64Column("i", []int64{7}),
		fetch.Float64Column("f", []float64{1.5}),
		fetch.BytesColumn("b", [][]byte{[]byte("x")}),
	))

	i, ok := got.Column("i")
	require.True(t, ok)
	assert.Equal(t, fetch.KindInt64, i.Kind)
	assert.Equal(t, []int64{7}, i.Int64)

	f, ok := got.Column("f")
	require.True(t, ok)
	assert.Equal(t, fetch.KindFloat64, f.Kind)
	assert.Equal(t, []float64{1.5}, f.Float64)

	b, ok := got.Column("b")
	require.True(t, ok)
	assert.Equal(t, fetch.KindBytes, b.Kind)
	assert.Equal(t, [][]byte{[]byte("x")}, b.Bytes)
}

// TestLogBatchEmptyColumnKeepsKind is the regression: a row-less column of any kind has no
// populated slice, so an encoder that derives the kind by nil-checking flattens every empty column
// to an empty int64 one.
func TestLogBatchEmptyColumnKeepsKind(t *testing.T) {
	t.Parallel()

	got := roundTrip(t, kindBatch(
		fetch.BytesColumn("emptyBytes", nil),
		fetch.Float64Column("emptyFloat", nil),
		fetch.Int64Column("emptyInt", nil),
	))

	eb, ok := got.Column("emptyBytes")
	require.True(t, ok)
	assert.Equal(t, fetch.KindBytes, eb.Kind, "an empty bytes column is not an empty int64 column")
	assert.Zero(t, eb.Len())

	ef, ok := got.Column("emptyFloat")
	require.True(t, ok)
	assert.Equal(t, fetch.KindFloat64, ef.Kind, "an empty float column is not an empty int64 column")
	assert.Zero(t, ef.Len())

	ei, ok := got.Column("emptyInt")
	require.True(t, ok)
	assert.Equal(t, fetch.KindInt64, ei.Kind)
	assert.Zero(t, ei.Len())

	_, ok = got.Column("neverSent")
	assert.False(t, ok, "an absent column stays absent")
}

func TestLogBatchUnknownColumnKindErrors(t *testing.T) {
	t.Parallel()

	t.Run("encode rejects a kindless column", func(t *testing.T) {
		t.Parallel()

		_, err := cluster.EncodeLogBatches([]*fetch.Batch{
			kindBatch(fetch.NamedColumn{Name: "nokind", Int64: []int64{1}}),
		})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "nokind")
	})

	t.Run("encode rejects an out-of-range kind", func(t *testing.T) {
		t.Parallel()

		_, err := cluster.EncodeLogBatches([]*fetch.Batch{
			kindBatch(fetch.NamedColumn{Name: "future", Kind: fetch.KindBytes + 1}),
		})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "future")
	})

	t.Run("decode rejects an unknown kind tag", func(t *testing.T) {
		t.Parallel()

		const name = "tagged"

		enc, err := cluster.EncodeLogBatches([]*fetch.Batch{
			kindBatch(fetch.Int64Column(name, []int64{1})),
		})
		require.NoError(t, err)

		// The kind tag is the byte right after the column name on the wire.
		at := bytes.Index(enc, []byte(name))
		require.Positive(t, at)
		enc[at+len(name)] = 0xff

		_, err = cluster.DecodeLogBatches(enc)
		require.Error(t, err)
	})
}

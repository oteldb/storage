package fetch_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/query/fetch"
)

func TestColumnKind(t *testing.T) {
	t.Parallel()

	assert.False(t, fetch.KindUnknown.Valid(), "the zero kind is never valid")
	assert.False(t, (fetch.KindBytes + 1).Valid())

	for _, c := range []struct {
		col  fetch.NamedColumn
		kind fetch.ColumnKind
		str  string
		n    int
	}{
		{fetch.Int64Column("i", []int64{1, 2}), fetch.KindInt64, "int64", 2},
		{fetch.Float64Column("f", []float64{1}), fetch.KindFloat64, "float64", 1},
		{fetch.BytesColumn("b", nil), fetch.KindBytes, "bytes", 0},
	} {
		require.True(t, c.kind.Valid())
		assert.Equal(t, c.kind, c.col.Kind)
		assert.Equal(t, c.str, c.kind.String())
		assert.Equal(t, c.n, c.col.Len())
	}

	assert.Equal(t, "unknown", fetch.KindUnknown.String())
	assert.Zero(t, fetch.NamedColumn{}.Len())
}

func TestRowValueUsesKind(t *testing.T) {
	t.Parallel()

	b := &fetch.Batch{
		Timestamps: []int64{1},
		Columns: []fetch.NamedColumn{
			fetch.Int64Column("i", []int64{7}),
			fetch.Float64Column("f", []float64{1.5}),
			fetch.BytesColumn("b", [][]byte{[]byte("x")}),
			{Name: "nokind", Int64: []int64{7}}, // a populated slice with no kind: a nil-checking reader would take it.
		},
	}

	v, ok := fetch.RowValue(b, 0, "i")
	require.True(t, ok)
	assert.Equal(t, int64(7), v.Int())

	v, ok = fetch.RowValue(b, 0, "f")
	require.True(t, ok)
	assert.InEpsilon(t, 1.5, v.Double(), 1e-9)

	v, ok = fetch.RowValue(b, 0, "b")
	require.True(t, ok)
	assert.Equal(t, "x", string(v.Str()))

	_, ok = fetch.RowValue(b, 0, "nokind")
	assert.False(t, ok, "a kindless column yields no value instead of one guessed from a non-nil slice")
}

package block

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/encoding/chunk"
)

func TestStreamWriterReservedU128Runs(t *testing.T) {
	t.Parallel()
	w := NewStreamWriter()
	require.Error(t, w.ReserveU128Runs(0, 10))
	require.NoError(t, w.AddColumn(Column{Name: "id", Kind: KindInt128}))
	for _, n := range []int{10, 0, -1} {
		require.NoError(t, w.ReserveU128Runs(0, n))
		assert.GreaterOrEqual(t, cap(w.cols[0].runs), 10)
		assert.Zero(t, w.Rows())
	}
	a, b := chunk.U128{Lo: 1}, chunk.U128{Lo: 2}
	require.NoError(t, w.AppendU128Run(0, a, 3))
	require.NoError(t, w.AppendU128Run(0, a, 5))
	require.NoError(t, w.AppendU128Run(0, b, 2))
	assert.Equal(t, []chunk.U128Run{{Value: a, Count: 8}, {Value: b, Count: 2}}, w.cols[0].runs)
	assert.Equal(t, 10, w.Rows())
	require.NoError(t, w.AddColumn(Column{Name: "ts", Kind: KindInt64, Block: true}))
	require.Error(t, w.ReserveU128Runs(1, 1))
}

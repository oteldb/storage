package block

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/encoding/chunk"
)

func TestWriterOwnedRunView(t *testing.T) {
	t.Parallel()
	w := NewStreamWriter()
	require.NoError(t, w.AddColumn(Column{Name: "id", Kind: KindInt128}))
	require.NoError(t, w.ReserveU128Runs(0, 8))
	for _, count := range []int{1, 2, 3} {
		runs, err := w.AppendU128RunView(0, chunk.U128{Lo: 1}, count)
		require.NoError(t, err)
		current, err := w.U128Runs(0)
		require.NoError(t, err)
		require.Same(t, &current[0], &runs[0])
	}
	runs, err := w.AppendU128RunView(0, chunk.U128{Lo: 2}, 4)
	require.NoError(t, err)
	require.Equal(t, []chunk.U128Run{{Value: chunk.U128{Lo: 1}, Count: 6}, {Value: chunk.U128{Lo: 2}, Count: 4}}, runs)
	require.Equal(t, 10, w.Rows())
	_, err = w.U128Runs(1)
	require.Error(t, err)
}

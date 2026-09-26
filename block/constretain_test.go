package block

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/backend/backendtest"
)

// TestStreamWriterConstRetain pins constRetainBytes: a column not yet proven non-constant attaches
// once it holds a frame of output, so a long constant prefix is not retained, and a column that ends
// constant is aborted and leaves no object.
func TestStreamWriterConstRetain(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	for _, tc := range []struct {
		name    string
		last    []byte
		isConst bool
	}{
		{"ends constant", []byte("same"), true},
		{"differs at the end", []byte("other"), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b := backendtest.NewStreamingMemory()
			w := NewStreamWriterTo(ctx, b, "p", WithGranuleSize(8), WithCompressBlockBytes(64))
			require.NoError(t, w.AddColumn(Column{Name: "b", Kind: KindBytes, Block: true}))
			require.NoError(t, w.AddColumn(Column{Name: "v", Kind: KindFloat64, Block: true}))

			var peak int64

			for range 400 {
				require.NoError(t, w.AppendBytes(0, valuesOf(8, func(int) string { return "same" })))
				require.NoError(t, w.AppendFloat64(1, make([]float64, 8)))
				peak = max(peak, w.ResidentBytes())
			}

			assert.True(t, w.cols[0].streaming, "a constant prefix past one frame attaches")
			assert.True(t, w.cols[1].streaming)

			require.NoError(t, w.AppendBytes(0, [][]byte{tc.last}))
			require.NoError(t, w.AppendFloat64(1, []float64{0}))
			require.NoError(t, WriteStreamPart(ctx, b, "p", w))

			r, err := OpenPart(ctx, b, "p")
			require.NoError(t, err)

			desc, ok := r.ColumnDescByName("b")
			require.True(t, ok)
			assert.Equal(t, tc.isConst, desc.Const)

			_, err = b.Read(ctx, columnKey("p", 0))
			if tc.isConst {
				require.ErrorIs(t, err, backend.ErrNotExist, "an early-attached constant column is aborted")
				assert.Equal(t, []byte("same"), desc.ConstBytes)
			} else {
				require.NoError(t, err)
			}

			vdesc, _ := r.ColumnDescByName("v")
			assert.True(t, vdesc.Const)

			_, err = b.Read(ctx, columnKey("p", 1))
			require.ErrorIs(t, err, backend.ErrNotExist)
		})
	}
}

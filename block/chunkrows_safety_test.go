package block

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/backend/backendtest"
	"github.com/oteldb/storage/encoding/chunk"
)

type chunkBackendWithoutSizer struct{ backend.Backend }

func TestVariablePartsDoNotReadLegacyMarks(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	counter := backendtest.NewByteCounter(backend.Memory())
	w := NewStreamWriterTo(ctx, counter, "p", WithIndependentGroups(), WithGranuleSize(4), WithSortKey("ts"))
	require.NoError(t, w.AddColumn(Column{Name: "ts", Kind: KindInt64, Codec: chunk.CodecDoDScaled, Block: true}))
	require.NoError(t, w.AppendInt64(0, []int64{1, 2}))
	require.NoError(t, w.EndGroup())
	require.NoError(t, w.AppendInt64(0, []int64{3, 4}))
	require.NoError(t, WriteStreamPart(ctx, counter, "p", w))
	r, err := OpenPart(ctx, chunkBackendWithoutSizer{counter}, "p")
	require.NoError(t, err)
	counter.Reset()
	marks, err := r.Marks(ctx)
	require.NoError(t, err)
	require.Empty(t, marks.Granules)
	require.Zero(t, counter.Reads(), counter.Report())
}

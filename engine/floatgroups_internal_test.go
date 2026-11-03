package engine

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/encoding/chunk"
	"github.com/oteldb/storage/internal/watermark"
)

func TestGroupedBufferedColumnsPreserveWeightsAndCoverage(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	e := New(Config{Backend: backend.Memory(), MetricFloatGroups: true, AggregateStats: true})
	s := apiSeries()
	_, err := e.Append(s, 1, 1)
	require.NoError(t, err)
	id := idToU128(s.Hash())
	cols := &flushColumns{series: []chunk.U128{id, id}, ts: []int64{1, 2}, value: []float64{10, 20}, sf: []float64{2, 3}, covered: []int64{100, 200}}
	p, err := e.writeMergedPart(ctx, cols, MergeOptions{}, nil)
	require.NoError(t, err)
	desc, ok := p.reader.ColumnDescByName(colValue)
	require.True(t, ok)
	require.Equal(t, chunk.CodecFloatGroups, desc.Codec)
	c, err := p.reader.Column(ctx, colSF)
	require.NoError(t, err)
	weights, err := c.Float64(nil)
	require.NoError(t, err)
	require.Equal(t, cols.sf, weights)
	data, err := e.cfg.Backend.Read(ctx, watermark.Key(p.prefix))
	require.NoError(t, err)
	marks, err := watermark.Decode(data)
	require.NoError(t, err)
	require.Len(t, marks, 1)
	require.EqualValues(t, 200, marks[0].Max)
}

func TestGroupedWriterKeepsPrecisionPolicy(t *testing.T) {
	t.Parallel()
	e := New(Config{Backend: backend.Memory(), MetricFloatGroups: true})
	w, err := newPartStreamWriter(t.Context(), e, compressProfile{}, 20, false, false, nil)
	require.NoError(t, err)
	defer w.abort()
	require.False(t, w.floatGroups)
}

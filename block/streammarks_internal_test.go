package block

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/backend/backendtest"
)

func TestStreamWriterMarksHandoff(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		name string
		key  string
		ts   []int64
	}{
		{"partial", "ts", []int64{8, 1, 9, 2, 3}},
		{"constant", "ts", []int64{7, 7, 7, 7}},
		{"single", "ts", []int64{3}},
		{"no sort key", "", []int64{1, 2, 3}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := backend.Memory()
			w := NewStreamWriterTo(t.Context(), b, "part", WithGranuleSize(2), WithSortKey(tt.key))
			require.Zero(t, w.Marks())
			require.NoError(t, w.AddColumn(Column{Name: "ts", Kind: KindInt64, Block: true}))
			require.NoError(t, w.AppendInt64(0, tt.ts))
			require.NoError(t, WriteStreamPart(t.Context(), b, "part", w))
			data, err := b.Read(t.Context(), marksKey("part"))
			require.NoError(t, err)
			stored, err := DecodeMarks(data)
			require.NoError(t, err)
			require.Equal(t, stored, w.Marks())
			if len(w.Marks().Granules) > 0 {
				require.Same(t, &w.cols[0].granules[0], &w.Marks().Granules[0], "handoff reuses the writer allocation")
			}
		})
	}
}

func TestFailedStreamPartDoesNotPublishMarks(t *testing.T) {
	t.Parallel()
	b := backendtest.NewCounting(backend.Memory())
	b.FailWriteAt(1)
	w := NewStreamWriterTo(t.Context(), b, "part", WithGranuleSize(2), WithSortKey("ts"))
	require.NoError(t, w.AddColumn(Column{Name: "ts", Kind: KindInt64, Block: true}))
	require.NoError(t, w.AppendInt64(0, []int64{1, 2, 3}))
	require.Error(t, WriteStreamPart(t.Context(), b, "part", w))
	require.Zero(t, w.Marks())
}

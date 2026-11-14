package engine

import (
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/internal/watermark"
)

func TestSharedMergeWatermarkMemoryIsBounded(t *testing.T) {
	t.Parallel()
	for _, count := range []int{1, 32768, 1 << 20} {
		t.Run(strconv.Itoa(count), func(t *testing.T) {
			t.Parallel()
			p := &part{sharedSeries: &seriesBinding{}, index: partIndex{paged: &pagedIndex{n: count}}}
			bound, err := mergeWatermarkBoundWithFallback(t.Context(), p, false)
			require.NoError(t, err)
			resident, open := watermark.MemoryBound()
			require.Equal(t, resident, bound.resident)
			require.Equal(t, open, bound.transient)
			require.Less(t, bound.bytes(), int64(256<<10))
		})
	}
}

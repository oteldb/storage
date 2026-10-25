//go:build !race

package engine_test

import (
	"context"
	"fmt"
	"runtime"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/internal/heaptest"
)

//nolint:paralleltest // allocation volume is process-wide
func TestCountSparseAllocationBound(t *testing.T) {
	for _, cache := range []int64{0, 32 << 20} {
		for _, series := range []int{64, 1024} {
			t.Run(fmt.Sprintf("cache=%d/series=%d", cache, series), func(t *testing.T) {
				e, req := sparseCountEngine(t, series, cache, false)
				run := func() {
					got, err := e.Count(context.Background(), req)
					require.NoError(t, err)
					require.Equal(t, 1, got)
				}
				run()
				runtime.GC()
				allocated := heaptest.Allocated(func() {
					for range 5 {
						run()
					}
				}) / 5
				t.Logf("one matched series, %d total rows: %d bytes/count", series*1024, allocated)
				require.Less(t, allocated, uint64(512<<10), "count scratch must follow selected timestamp blocks, not unrelated part rows")
			})
		}
	}
}

package heaptest_test

import (
	"runtime"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/oteldb/storage/internal/heaptest"
)

// TestPeak: a run that holds several buffers at once, then drops them, peaks at their sum.
//
//nolint:paralleltest // samples the process-wide heap
func TestPeak(t *testing.T) {
	const chunk = 8 << 20

	peak := heaptest.Peak(func() {
		held := make([][]byte, 0, 8)
		for range 8 {
			b := make([]byte, chunk)
			b[len(b)-1] = 1
			held = append(held, b)
		}

		// Allocations after the peak let a collection see it before the buffers are dropped.
		for range 64 {
			_ = make([]byte, 1<<20)
		}

		runtime.KeepAlive(held)
	})

	t.Logf("peak %.1f MiB", float64(peak)/(1<<20))
	assert.GreaterOrEqual(t, peak, uint64(7*chunk))
	assert.Less(t, peak, uint64(10*chunk))
}

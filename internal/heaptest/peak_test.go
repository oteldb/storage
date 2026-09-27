package heaptest_test

import (
	"runtime"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/oteldb/storage/internal/heaptest"
)

// TestPeak: a run that holds several buffers at once and samples while they are held peaks at their
// sum, however much it frees before returning.
//
//nolint:paralleltest // samples the process-wide heap
func TestPeak(t *testing.T) {
	const chunk = 8 << 20

	peak := heaptest.Peak(func(sample func()) {
		held := make([][]byte, 0, 8)
		for range 8 {
			b := make([]byte, chunk)
			b[len(b)-1] = 1
			held = append(held, b)
		}

		sample()
		runtime.KeepAlive(held)
	})

	t.Logf("peak %.1f MiB", float64(peak)/(1<<20))
	assert.Greater(t, peak, uint64(8*chunk-chunk/8))
	assert.Less(t, peak, uint64(9*chunk))
}

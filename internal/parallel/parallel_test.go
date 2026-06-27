package parallel_test

import (
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/internal/parallel"
)

func TestForEachRunsEveryIndexOnce(t *testing.T) {
	t.Parallel()

	for _, n := range []int{0, 1, 2, 7, 64} {
		for _, limit := range []int{-1, 0, 1, 3, 100} {
			seen := make([]int32, n)
			parallel.ForEach(n, limit, func(i int) { atomic.AddInt32(&seen[i], 1) })

			for i := range seen {
				require.Equalf(t, int32(1), seen[i], "n=%d limit=%d index=%d", n, limit, i)
			}
		}
	}
}

// TestForEachRespectsLimit asserts no more than limit calls are ever in flight at once.
func TestForEachRespectsLimit(t *testing.T) {
	t.Parallel()

	const (
		n     = 200
		limit = 4
	)

	var (
		inFlight atomic.Int32
		maxSeen  atomic.Int32
	)

	parallel.ForEach(n, limit, func(int) {
		cur := inFlight.Add(1)
		for { // record the high-water mark
			m := maxSeen.Load()
			if cur <= m || maxSeen.CompareAndSwap(m, cur) {
				break
			}
		}

		for i := 0; i < 1000; i++ { // brief busy work to overlap workers
			_ = i
		}

		inFlight.Add(-1)
	})

	assert.LessOrEqual(t, maxSeen.Load(), int32(limit), "concurrency never exceeds the limit")
	assert.Positive(t, maxSeen.Load(), "work did run concurrently")
}

func TestForEachResultsByIndex(t *testing.T) {
	t.Parallel()

	const n = 50

	out := make([]int, n)
	parallel.ForEach(n, 8, func(i int) { out[i] = i * i })

	for i := range out {
		require.Equal(t, i*i, out[i])
	}
}

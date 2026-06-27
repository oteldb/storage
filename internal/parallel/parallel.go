// Package parallel provides bounded fan-out over an index range — the project's house pattern for
// running independent work (per-tenant maintenance, per-shard scatter-gather, WAL fsyncs) concurrently
// under a cap, without pulling in an errgroup dependency. Callers collect results/errors into
// per-index slots, mirroring the existing query/scale split fan-out.
package parallel

import (
	"runtime"
	"sync"
	"sync/atomic"
)

// DefaultLimit is a sane concurrency cap for I/O-bound fan-out: enough to overlap backend round-trips
// without swamping the object store or the scheduler.
func DefaultLimit() int {
	n := runtime.GOMAXPROCS(0)
	switch {
	case n < 1:
		return 1
	case n > 16:
		return 16
	default:
		return n
	}
}

// ForEach invokes fn(0), …, fn(n-1), running at most limit concurrently (limit ≤ 0 or ≥ n ⇒ all at
// once), and returns once every call has completed. Calls for distinct i run on different goroutines,
// so fn must not mutate shared state without its own synchronization; writing to a per-index slot
// (e.g. results[i], errs[i]) is the intended pattern and needs none. A worker spawns exactly
// min(n, limit) goroutines and distributes indices via an atomic counter.
func ForEach(n, limit int, fn func(i int)) {
	switch {
	case n <= 0:
		return
	case n == 1:
		fn(0)

		return
	}

	if limit <= 0 || limit > n {
		limit = n
	}

	if limit == 1 {
		for i := range n {
			fn(i)
		}

		return
	}

	var (
		wg   sync.WaitGroup
		next atomic.Int64
	)

	wg.Add(limit)

	for range limit {
		go func() {
			defer wg.Done()

			for {
				i := int(next.Add(1)) - 1
				if i >= n {
					return
				}

				fn(i)
			}
		}()
	}

	wg.Wait()
}

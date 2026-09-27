package heaptest

import (
	"runtime"
	"runtime/debug"
	"runtime/metrics"
	"sync"
	"sync/atomic"
)

// Peak runs run with the collector at its most eager and returns the most the heap held live at the
// end of any collection while it ran, above what it held before. A collection starts once the heap
// grows by 1% over the last one's survivors, so the figure falls short of the true peak by at most
// about that much, and by the allocations since the last collection.
func Peak(run func()) uint64 {
	old := debug.SetGCPercent(1)
	defer debug.SetGCPercent(old)

	base := Live()

	var (
		peak atomic.Uint64
		stop atomic.Bool
		wg   sync.WaitGroup
	)

	wg.Go(func() {
		sample := []metrics.Sample{{Name: "/gc/heap/live:bytes"}}

		for !stop.Load() {
			metrics.Read(sample)

			if v := sample[0].Value.Uint64(); v > peak.Load() {
				peak.Store(v)
			}

			runtime.Gosched()
		}
	})

	run()
	runtime.GC()
	stop.Store(true)
	wg.Wait()

	return peak.Load() - min(peak.Load(), base)
}

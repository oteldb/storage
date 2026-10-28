package engine

import (
	"fmt"
	"maps"
	"runtime"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/oteldb/storage/index/series"
	"github.com/oteldb/storage/internal/identitysnapshot"
	"github.com/oteldb/storage/signal"
	"github.com/oteldb/storage/wal"
)

// BenchmarkIdentityCaptureContended measures the two flush reference passes while four writers
// append real 4096-point batches. Use fixed iterations and a disk-backed TMPDIR for the WAL case.
// Allocation counters include the concurrent writers; writer latency includes queueing and WAL I/O.
func BenchmarkIdentityCaptureContended(b *testing.B) {
	for _, cardinality := range []int{1 << 18, 1 << 20} {
		for _, logged := range []bool{false, true} {
			for _, warm := range []bool{false, true} {
				for _, dense := range []bool{false, true} {
					b.Run(fmt.Sprintf("series=%d/wal=%t/warm=%t/dense=%t", cardinality, logged, warm, dense), func(b *testing.B) {
						benchmarkIdentityCapture(b, cardinality, logged, warm, dense, 1)
					})
				}
			}
		}
	}
}

func BenchmarkIdentityCaptureQuarterDensity(b *testing.B) {
	for _, dense := range []bool{false, true} {
		b.Run(fmt.Sprintf("dense=%t", dense), func(b *testing.B) {
			benchmarkIdentityCapture(b, 1<<20, true, true, dense, 4)
		})
	}
}

type captureLock struct {
	sync.Locker

	start time.Time
	wait  time.Duration
	holds []time.Duration
}

func (l *captureLock) Lock() {
	start := time.Now()
	l.Locker.Lock()
	l.start = time.Now()
	l.wait += l.start.Sub(start)
}

func (l *captureLock) Unlock() {
	l.holds = append(l.holds, time.Since(l.start))
	l.Locker.Unlock()
}

func benchmarkIdentityCapture(b *testing.B, cardinality int, logged, warm, dense bool, divisor int) {
	b.Helper()
	b.StopTimer()
	b.ReportAllocs()
	var captureWait, captureElapsed time.Duration
	holds := make([]time.Duration, 0, (2*cardinality/256+2)*b.N)
	var latencies []time.Duration
	var appended int64
	for range b.N {
		e := New(Config{})
		ids := make([]signal.SeriesID, cardinality)
		identity := func(i int) signal.Series {
			return signal.Series{Attributes: signal.NewAttributes(
				signal.KeyValue{Key: []byte("__name__"), Value: signal.StringValue([]byte("metric"))},
				signal.KeyValue{Key: []byte("instance"), Value: signal.IntValue(int64(i))},
				signal.KeyValue{Key: []byte("job"), Value: signal.StringValue([]byte("node"))},
			)}
		}
		for i := range ids {
			ids[i] = identity(i).Hash()
		}
		const batch = 4096
		ts, values := make([]int64, batch), make([]float64, batch)
		for i := range ts {
			ts[i], values[i] = 1, 1
		}
		for offset := 0; offset < len(ids); offset += batch {
			if _, err := e.AppendBatch(ids[offset:offset+batch], ts, values, nil,
				func(i int) signal.Series { return identity(offset + i) }, AppendLimits{}); err != nil {
				b.Fatal(err)
			}
		}
		detached, _, _ := e.head.detach()
		for i := cardinality / divisor; i < cardinality; i++ {
			delete(detached, ids[i])
		}
		if logged {
			w, err := wal.Create(b.TempDir(), 0)
			if err != nil {
				b.Fatal(err)
			}
			w.SetEpoch(1)
			e.cfg.WAL = w
		}
		if warm {
			for offset := 0; offset < len(ids); offset += batch {
				if _, err := e.AppendBatch(ids[offset:offset+batch], ts, values, nil,
					func(i int) signal.Series { return identity(offset + i) }, AppendLimits{}); err != nil {
					b.Fatal(err)
				}
			}
			// Keep cold registration, identity WAL records, and buffer growth out of the capture window.
			for _, buf := range e.head.samples {
				buf.ts = slices.Grow(buf.ts, 31)
				buf.values = slices.Grow(buf.values, 31)
			}
		}
		runtime.GC()
		const writers = 4
		stop := make(chan struct{})
		var ready, done sync.WaitGroup
		var measuring atomic.Bool
		ready.Add(writers)
		writerTimes := make([][]time.Duration, writers)
		errs := make(chan error, writers)
		for worker := range writers {
			done.Go(func() {
				first := true
				for offset := worker * batch; ; offset = (offset + writers*batch) % len(ids) {
					select {
					case <-stop:
						return
					default:
					}
					start := time.Now()
					timed := measuring.Load()
					_, err := e.AppendBatch(ids[offset:offset+batch], ts, values, nil,
						func(i int) signal.Series { return identity(offset + i) }, AppendLimits{})
					if timed && measuring.Load() {
						writerTimes[worker] = append(writerTimes[worker], time.Since(start))
					}
					if first {
						first = false
						ready.Done()
					}
					if err != nil {
						errs <- err
						return
					}
				}
			})
		}
		ready.Wait()
		lock := &captureLock{Locker: e.mu.RLocker(), holds: make([]time.Duration, 0, 2*cardinality/256+2)}
		current := func() *series.Index { return e.head.series }
		capture := func(emit func(signal.SeriesID, series.View, uint32)) {
			if !dense {
				identitysnapshot.Capture(lock, current, maps.Keys(detached), emit)
				return
			}
			identitysnapshot.CaptureMap(lock, current, detached, emit)
		}

		b.StartTimer()
		measuring.Store(true)
		start := time.Now()
		var estimate int64
		capture(func(_ signal.SeriesID, view series.View, ref uint32) {
			estimate += view.EncodingBytes(int(ref)) + view.MaterializationBytes(int(ref))
		})
		refs := make(map[signal.SeriesID]uint32, len(detached))
		capture(func(id signal.SeriesID, _ series.View, ref uint32) {
			refs[id] = ref
		})
		elapsed := time.Since(start)
		measuring.Store(false)
		b.StopTimer()
		close(stop)
		done.Wait()
		close(errs)
		for err := range errs {
			b.Fatal(err)
		}
		if len(refs) != len(detached) || estimate == 0 {
			b.Fatal("incomplete capture")
		}
		if err := e.CloseWAL(); err != nil {
			b.Fatal(err)
		}
		captureWait += lock.wait
		captureElapsed += elapsed
		holds = append(holds, lock.holds...)
		for _, times := range writerTimes {
			latencies = append(latencies, times...)
			appended += int64(len(times) * batch)
		}
	}
	slices.Sort(holds)
	slices.Sort(latencies)
	b.ReportMetric(float64(captureWait.Nanoseconds())/float64(b.N), "capture-wait-ns/op")
	b.ReportMetric(float64(len(holds))/float64(b.N), "capture-locks/op")
	b.ReportMetric(float64(holds[len(holds)*99/100].Nanoseconds()), "capture-hold-p99-ns")
	b.ReportMetric(float64(holds[len(holds)-1].Nanoseconds()), "capture-hold-max-ns")
	b.ReportMetric(float64(latencies[len(latencies)*99/100].Nanoseconds()), "writer-p99-ns")
	b.ReportMetric(float64(appended)/captureElapsed.Seconds(), "writer-samples/s")
}

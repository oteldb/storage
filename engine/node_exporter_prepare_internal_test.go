package engine

import (
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/internal/reproduce"
	"github.com/oteldb/storage/signal"
	"github.com/oteldb/storage/signal/metric"
	"github.com/oteldb/storage/wal"
)

type nodeExporterInput struct {
	series []signal.Series
	ids    []signal.SeriesID
	ts     []int64
	values []float64
}

func nodeExporterInputBatch(count, offset int) nodeExporterInput {
	in := nodeExporterInput{
		series: make([]signal.Series, count), ids: make([]signal.SeriesID, count),
		ts: make([]int64, count), values: make([]float64, count),
	}
	for i := range count {
		s := cpuMetricSeries(offset + i)
		s.Attributes = signal.NewAttributes(
			signal.KeyValue{Key: []byte("instance"), Value: signal.StringValue([]byte(cpuHost((offset + i) / len(cpuFixtureModes))))},
			signal.KeyValue{Key: []byte("cpu"), Value: signal.StringValue([]byte("0"))},
			signal.KeyValue{Key: []byte("mode"), Value: signal.StringValue([]byte(cpuFixtureModes[(offset+i)%len(cpuFixtureModes)]))},
			signal.KeyValue{Key: []byte("job"), Value: signal.StringValue([]byte("node_exporter"))},
		)
		in.series[i] = (metric.Identity{
			Series: s, Name: []byte("node_cpu_seconds_total"), Unit: []byte("s"),
			Kind: metric.KindSum, Temporality: metric.TemporalityCumulative, Monotonic: true,
		}).ToSeries()
		in.ids[i], in.ts[i], in.values[i] = in.series[i].Hash(), cpuFixtureBase, float64(offset+i)
	}
	return in
}

func (in nodeExporterInput) append(e *Engine) parallelAppendResult {
	res, err := e.AppendBatch(in.ids, in.ts, in.values, nil, func(i int) signal.Series { return in.series[i] }, AppendLimits{})
	return parallelAppendResult{result: res, err: err}
}

func TestNodeExporterBatchesPrepareIndependently(t *testing.T) {
	reproduce.Unfixed(t, 661, "4096 node-exporter identities exceed the inflated private-work estimate")
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		e, fs := newFaultWALEngine(t, 0)
		e.cfg.DiskSeries, e.cfg.Backend = true, backend.Memory()
		e.cfg.ResidentHeadBytes = 1 << 30
		e.head.disk = newDiskHeadState()
		firstInput := nodeExporterInputBatch(4096, 0)
		secondInput := nodeExporterInputBatch(4096, 10_000*len(cpuFixtureModes))
		ready, gate, ordered := make(chan struct{}), make(chan struct{}), make(chan struct{}, 2)
		resume := sync.OnceFunc(func() { close(gate) })
		defer resume()
		var preparations atomic.Int32
		e.parallel.hooks = &parallelHeadHooks{
			prepared: func() {
				if preparations.Add(1) == 1 {
					close(ready)
					<-gate
				}
			},
			ordered: func([]signal.SeriesID) { ordered <- struct{}{} },
		}
		first, second := make(chan parallelAppendResult, 1), make(chan parallelAppendResult, 1)
		go func() { first <- firstInput.append(e) }()
		concurrent := false
		select {
		case <-ready:
			go func() { second <- secondInput.append(e) }()
			select {
			case r := <-second:
				require.NoError(t, r.err)
				require.Equal(t, 4096, r.result.Accepted)
				require.Equal(t, 4096, e.HeadSampleCount(), "the paused batch remains unpublished")
				require.Equal(t, []int64{cpuFixtureBase}, headTS(replayedHead(t, fs), secondInput.series[0]))
				concurrent = true
			case <-ordered:
				resume()
				require.NoError(t, (<-second).err)
			}
		case <-ordered:
		}
		resume()
		synctest.Wait()
		r := <-first
		require.NoError(t, r.err)
		require.Equal(t, 4096, r.result.Accepted)
		require.True(t, concurrent, "full node-exporter batches must not take the exclusive fallback")
		require.Zero(t, e.parallel.pending.Load())
		require.Zero(t, e.parallel.pendingSeries.Load())
	})
}

func BenchmarkNodeExporterColdAppend(b *testing.B) {
	in := nodeExporterInputBatch(4096, 0)
	w, err := wal.Create(b.TempDir(), 1<<30)
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = w.Close() })
	e := New(Config{WAL: w, Backend: backend.Memory(), DiskSeries: true, ResidentHeadBytes: 1 << 30})
	b.ReportAllocs()
	b.SetBytes(int64(len(in.ids)) * SampleBytes)
	for b.Loop() {
		b.StopTimer()
		e.head.memory.release()
		e.head = newHead()
		e.head.disk = newDiskHeadState()
		b.StartTimer()
		r := in.append(e)
		if r.err != nil || r.result.Accepted != len(in.ids) {
			b.Fatalf("append accepted=%d: %v", r.result.Accepted, r.err)
		}
	}
	b.ReportMetric(float64(b.N*len(in.ids))/b.Elapsed().Seconds(), "samples/s")
}

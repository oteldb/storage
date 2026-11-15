package engine

import (
	"flag"
	"fmt"
	"math"
	"os"
	"runtime"
	"runtime/pprof"
	"runtime/trace"
	"sync"
	"testing"

	"github.com/go-faster/errors"

	"github.com/oteldb/storage/internal/memlimit"
	"github.com/oteldb/storage/internal/obs/obstest"
	"github.com/oteldb/storage/signal"
)

// BenchmarkIngestionContention replays precomputed CPU series through concurrent native writes.
// See ingestion_bench.md for phase-only mutex/block/CPU/trace capture and fixture limits.
func BenchmarkIngestionContention(b *testing.B) {
	if os.Getenv("OTELDB_BENCH_INGEST_PROFILE") != "" &&
		(flag.Lookup("test.benchtime").Value.String() != "1x" || flag.Lookup("test.count").Value.String() != "1") {
		b.Fatal("phase-only profiles require -benchtime=1x -count=1; select one benchmark leaf")
	}
	hosts := cpuBenchInt(b, "OTELDB_BENCH_INGEST_HOSTS", 10000, 80000)
	rounds := cpuBenchInt(b, "OTELDB_BENCH_INGEST_ROUNDS", 16, 64)
	headMiB := cpuBenchInt(b, "OTELDB_BENCH_INGEST_HEAD_MIB", 8, 1024)
	input := nodeExporterInputBatch(hosts*len(cpuFixtureModes), 0)
	for _, mode := range []string{"resident", "pressure"} {
		for _, writers := range []int{1, 8} {
			b.Run(fmt.Sprintf("%s/writers=%d", mode, writers), func(b *testing.B) {
				if len(input.ids) < writers {
					b.Skip("fixture needs at least one series per writer")
				}
				benchmarkIngestionContention(b, input, rounds, writers, mode, int64(headMiB)<<20)
			})
		}
	}
}

func benchmarkIngestionContention(b *testing.B, input nodeExporterInput, rounds, writers int, mode string, headBytes int64) {
	b.Helper()
	rows := int64(len(input.ids)) * int64(rounds)
	b.ReportAllocs()
	b.SetBytes(rows * SampleBytes)
	b.Logf("hosts=%d series=%d rounds=%d samples/op=%d native files, WAL sync=false, no merges",
		len(input.ids)/len(cpuFixtureModes), len(input.ids), rounds, rows)
	var flushes, parts, remaining, walAppends int64
	var parallelRows, orderedRows, orderedBatches, pressureMisses, snapshotMisses, retried, workLimit int64
	for b.Loop() {
		b.StopTimer()
		f := newCPUFixture(b, b.Context(), cpuFixtureOptions{disk: true, flushMemoryBytes: 32 << 20})
		pool := memlimit.NewPool(128 << 20)
		e := f.engine
		e.cfg.FlushAdmission, e.cfg.TryFlushAdmission = pool.Acquire, pool.TryAcquire
		e.cfg.ResidentHeadBytes = math.MaxInt64
		for off := 0; off < len(input.ids); off += 4096 {
			end := min(off+4096, len(input.ids))
			result, err := e.AppendBatch(input.ids[off:end], input.ts[off:end], input.values[off:end], nil,
				func(i int) signal.Series { return input.series[off+i] }, AppendLimits{})
			if err != nil || result.Accepted != end-off {
				b.Fatalf("seed accepted=%d want=%d: %v", result.Accepted, end-off, err)
			}
		}
		if mode == "pressure" {
			if err := e.Flush(b.Context()); err != nil {
				b.Fatal(err)
			}
			e.cfg.ResidentHeadBytes = headBytes
			e.cfg.AsyncPressureFlush = true
		}
		observability, metrics := obstest.New(b)
		e.cfg.Obs = observability
		e.cfg.WAL.SetObs(observability.WAL)
		initialParts := e.PartCount()
		finishProfile := ingestionContentionProfile(b, mode, writers)
		b.StartTimer()
		var wg sync.WaitGroup
		errs := make(chan error, writers)
		for worker := range writers {
			wg.Go(func() {
				if err := replayIngestionShard(e, input, worker*len(input.ids)/writers,
					(worker+1)*len(input.ids)/writers, rounds); err != nil {
					errs <- err
				}
			})
		}
		wg.Wait()
		if mode == "pressure" {
			if err := e.stopPressurePipeline(b.Context(), false); err != nil {
				b.Error(err)
			}
			if err := e.Flush(b.Context()); err != nil {
				b.Error(err)
			}
		}
		b.StopTimer()
		finishProfile()
		close(errs)
		for err := range errs {
			b.Error(err)
		}
		parallelRows += metrics.Counter("storage.ingest.prepared.samples", "path", "parallel")
		orderedRows += metrics.Counter("storage.ingest.prepared.samples", "path", "ordered")
		orderedBatches += metrics.Counter("storage.ingest.prepared.batches", "path", "ordered")
		pressureMisses += metrics.Counter("storage.ingest.prepared.attempts", "outcome", "pressure")
		snapshotMisses += metrics.Counter("storage.ingest.prepared.attempts", "outcome", "snapshot")
		workLimit += metrics.Counter("storage.ingest.prepared.attempts", "outcome", "work_limit")
		retried += metrics.Counter("storage.ingest.prepared.batches", "path", "parallel", "reason", "retry_pressure")
		retried += metrics.Counter("storage.ingest.prepared.batches", "path", "parallel", "reason", "retry_snapshot")
		walAppends += metrics.Counter("storage.wal.appends")
		flushes += metrics.Counter("storage.flush.total")
		parts += int64(e.PartCount() - initialParts)
		remaining += int64(e.HeadSampleCount())
		if e.SeriesCount() != len(input.ids) {
			b.Errorf("series=%d want=%d", e.SeriesCount(), len(input.ids))
		}
		if err := f.remove(b.Context()); err != nil {
			b.Fatal(err)
		}
		b.StartTimer()
	}
	b.ReportMetric(float64(rows)*float64(b.N)/b.Elapsed().Seconds(), "samples/s")
	b.ReportMetric(float64(rows), "samples/op")
	b.ReportMetric(float64(flushes)/float64(b.N), "flushes/op")
	b.ReportMetric(float64(parts)/float64(b.N), "parts/op")
	b.ReportMetric(float64(remaining)/float64(b.N), "head-samples/op")
	b.ReportMetric(float64(parallelRows)/float64(b.N), "parallel-samples/op")
	b.ReportMetric(float64(orderedRows)/float64(b.N), "ordered-samples/op")
	b.ReportMetric(float64(orderedBatches)/float64(b.N), "ordered-batches/op")
	b.ReportMetric(float64(pressureMisses)/float64(b.N), "pressure-misses/op")
	b.ReportMetric(float64(snapshotMisses)/float64(b.N), "snapshot-misses/op")
	b.ReportMetric(float64(workLimit)/float64(b.N), "work-limit/op")
	b.ReportMetric(float64(retried)/float64(b.N), "retry-success/op")
	b.ReportMetric(float64(walAppends)/float64(b.N), "wal-appends/op")
}

func replayIngestionShard(e *Engine, input nodeExporterInput, start, end, rounds int) error {
	const batch = 4096
	ts := make([]int64, min(batch, end-start))
	for round := range rounds {
		for i := range ts {
			ts[i] = cpuFixtureBase + int64(round+1)*cpuFixtureCadence
		}
		for off := start; off < end; off += batch {
			n := min(batch, end-off)
			result, err := e.AppendBatch(input.ids[off:off+n], ts[:n], input.values[off:off+n], nil,
				func(i int) signal.Series { return input.series[off+i] }, AppendLimits{})
			if err != nil {
				return err
			}
			if result.Accepted != n {
				return errors.Errorf("append accepted=%d want=%d", result.Accepted, n)
			}
		}
	}
	return nil
}

func ingestionContentionProfile(b *testing.B, mode string, writers int) func() {
	b.Helper()
	prefix := os.Getenv("OTELDB_BENCH_INGEST_PROFILE")
	if prefix == "" {
		return func() {}
	}
	prefix = fmt.Sprintf("%s.%s.w%d", prefix, mode, writers)
	finishCPU := cpuPhaseProfile(b, prefix)
	old := runtime.SetMutexProfileFraction(1)
	runtime.SetBlockProfileRate(1)
	f, err := os.Create(prefix + ".trace")
	if err != nil {
		runtime.SetBlockProfileRate(0)
		runtime.SetMutexProfileFraction(old)
		finishCPU()
		b.Fatal(err)
	}
	if err := trace.Start(f); err != nil {
		_ = f.Close()
		runtime.SetBlockProfileRate(0)
		runtime.SetMutexProfileFraction(old)
		finishCPU()
		b.Fatal(err)
	}
	stop := sync.OnceFunc(func() {
		trace.Stop()
		if err := f.Close(); err != nil {
			b.Error(err)
		}
		runtime.SetBlockProfileRate(0)
		runtime.SetMutexProfileFraction(old)
		for _, kind := range []string{"mutex", "block"} {
			out, err := os.Create(prefix + "." + kind)
			if err != nil {
				b.Fatal(err)
			}
			writeErr := pprof.Lookup(kind).WriteTo(out, 0)
			closeErr := out.Close()
			if writeErr != nil {
				b.Fatal(writeErr)
			}
			if closeErr != nil {
				b.Fatal(closeErr)
			}
		}
		finishCPU()
	})
	b.Cleanup(stop)
	return stop
}

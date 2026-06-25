package storage

import (
	"context"
	"strconv"
	"sync/atomic"
	"testing"

	"github.com/oteldb/storage/signal"
	"github.com/oteldb/storage/signal/metric"
)

// benchBatch builds one realistic gauge metric ("http.requests") whose data points span
// seriesCount distinct series (one per `route` value), each carrying pointsPerSeries
// samples. Total points = seriesCount * pointsPerSeries. Built once, outside the timed
// loop, and reused read-only across iterations.
func benchBatch(seriesCount, pointsPerSeries int) metric.Metrics {
	var md metric.Metrics
	rm := md.AddResource()
	rm.Resource = signal.Resource{
		Attributes: signal.NewAttributes(signal.KeyValue{
			Key: []byte("service.name"), Value: signal.StringValue([]byte("bench")),
		}),
	}
	sm := rm.AddScope()
	sm.Scope = signal.Scope{Name: []byte("benchlib")}
	mt := sm.AddMetric()
	mt.Name = []byte("http.requests")
	mt.Unit = []byte("1")
	mt.Kind = metric.KindGauge

	for s := range seriesCount {
		attrs := signal.NewAttributes(signal.KeyValue{
			Key: []byte("route"), Value: signal.StringValue([]byte("/route/" + strconv.Itoa(s))),
		})
		for p := range pointsPerSeries {
			pt := mt.AddPoint()
			pt.Ts = 1_000_000_000 + int64(p)*15_000 // 15ms stride
			pt.Value = float64(p)
			pt.Attributes = attrs
		}
	}

	return md
}

// benchShapes are the cardinality/depth shapes shared by the ingest benchmarks: same total
// point count (1000) distributed from one deep series to ten thousand shallow ones.
var benchShapes = []struct {
	name           string
	series, points int
}{
	{"1series_1000points", 1, 1000},
	{"100series_100points", 100, 100},
	{"1000series_10points", 1000, 10},
	{"10000series_1point", 10000, 1},
}

// reportIngestMetrics adds the point-oriented custom metrics that make an ingest benchmark
// readable: the throughput (million points/sec) and the per-point cost (ns/point). Both are
// derived from b.Elapsed (the timed duration, which excludes the reset work wrapped in
// StopTimer) over the total points ingested across all b.N iterations.
func reportIngestMetrics(b *testing.B, pointsPerOp int) {
	b.Helper()

	totalPoints := float64(pointsPerOp) * float64(b.N)
	secs := b.Elapsed().Seconds()
	if totalPoints == 0 || secs == 0 {
		return
	}

	b.ReportMetric(totalPoints/secs/1e6, "Mpoints/s")
	b.ReportMetric(secs*1e9/totalPoints, "ns/point")
}

// BenchmarkWriteMetrics measures the end-to-end point-ingestion throughput of the storage
// facade — tenant routing, projection, identity hashing, index registration, and head
// append — across a few cardinality/depth shapes. It reports custom metrics (Mpoints/s and
// ns/point) via b.ReportMetric.
func BenchmarkWriteMetrics(b *testing.B) {
	for _, sh := range benchShapes {
		b.Run(sh.name, func(b *testing.B) {
			benchmarkWriteMetrics(b, sh.series, sh.points)
		})
	}
}

func benchmarkWriteMetrics(b *testing.B, seriesCount, pointsPerSeries int) {
	b.Helper()

	ctx := context.Background()
	md := benchBatch(seriesCount, pointsPerSeries)
	total := seriesCount * pointsPerSeries

	// Rebuild the store periodically so the head's per-series buffers don't grow without
	// bound across b.N. ~1M buffered points keeps resident memory modest (~16 MiB of
	// samples) while amortizing the one-time series-registration cost over many appends.
	resetEvery := max((1<<20)/total, 1)

	s, err := InMemory()
	if err != nil {
		b.Fatal(err)
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := range b.N {
		if _, err := s.WriteMetrics(ctx, md); err != nil {
			b.Fatal(err)
		}

		if (i+1)%resetEvery == 0 {
			// Empty the head so its per-series buffers don't grow without bound across b.N.
			// Excluded from the timer.
			b.StopTimer()
			if err := s.Reset(ctx); err != nil {
				b.Fatal(err)
			}
			b.StartTimer()
		}
	}

	reportIngestMetrics(b, total)
}

// BenchmarkIngestAndFlush is the companion to [BenchmarkWriteMetrics] that includes the
// flush-to-part cost: each iteration ingests the batch into the head and then flushes it to
// a new immutable columnar part (column encode + compress + backend write). It measures the
// steady-state ingest+flush cycle — after the first flush the series index is retained, so
// subsequent iterations re-append to known series and flush again. It reports the same
// custom metrics as [BenchmarkWriteMetrics], so the flush overhead shows up directly as a
// lower Mpoints/s.
func BenchmarkIngestAndFlush(b *testing.B) {
	for _, sh := range benchShapes {
		b.Run(sh.name, func(b *testing.B) {
			benchmarkIngestAndFlush(b, sh.series, sh.points)
		})
	}
}

func benchmarkIngestAndFlush(b *testing.B, seriesCount, pointsPerSeries int) {
	b.Helper()

	ctx := context.Background()
	md := benchBatch(seriesCount, pointsPerSeries)
	total := seriesCount * pointsPerSeries

	// Each iteration writes one part; reset the store periodically so flushed parts don't
	// accumulate in the in-memory backend without bound across b.N.
	resetEvery := max((1<<20)/total, 1)

	s, err := InMemory()
	if err != nil {
		b.Fatal(err)
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := range b.N {
		if _, err := s.WriteMetrics(ctx, md); err != nil {
			b.Fatal(err)
		}
		// Flush the default tenant's head to a part (the batch routes to "default"); this is
		// the cost the head-only benchmark omits.
		if err := s.engineFor("default").Flush(ctx); err != nil {
			b.Fatal(err)
		}

		if (i+1)%resetEvery == 0 {
			b.StopTimer()
			if err := s.Reset(ctx); err != nil {
				b.Fatal(err)
			}
			b.StartTimer()
		}
	}

	reportIngestMetrics(b, total)
}

// BenchmarkWriteMetricsConcurrent measures aggregate ingest throughput when many goroutines
// write to one shared store concurrently — the workload the per-metric AppendBatch lock is
// built for. All writers route to the same tenant, so they contend on a single engine lock;
// because a metric's whole run is appended under one critical section (not one lock per
// point), the critical section stays coarse and short. Reported Mpoints/s is the aggregate
// across all goroutines (b.N is the total iteration count under RunParallel).
//
// To keep the shared head bounded across b.N, a writer periodically resets the store; Reset
// takes each engine's lock, so it is safe against the concurrent appends.
func BenchmarkWriteMetricsConcurrent(b *testing.B) {
	const (
		pointsPerBatch = 500     // one metric, 500 series × 1 point — a chunky single push
		resetEvery     = 1 << 12 // ~2M points (~32 MiB of samples) between head resets
	)

	ctx := context.Background()

	s, err := InMemory()
	if err != nil {
		b.Fatal(err)
	}

	var iters atomic.Int64

	b.ReportAllocs()
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		md := benchBatch(pointsPerBatch, 1) // each goroutine owns its batch; all build the same series

		for pb.Next() {
			if _, err := s.WriteMetrics(ctx, md); err != nil {
				b.Error(err)

				return
			}

			if iters.Add(1)%resetEvery == 0 {
				if err := s.Reset(ctx); err != nil {
					b.Error(err)

					return
				}
			}
		}
	})

	reportIngestMetrics(b, pointsPerBatch)
}

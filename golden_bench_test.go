package storage

// Golden benchmarks — the definitive, stable set used to assess overall read+write performance and
// to gate per-PR regressions (see .github/workflows/bench.yml). They are deliberately:
//
//   - Self-contained: no dependency on helpers in other _test.go files, so the CI workflow can copy
//     this one file onto the base commit and run the identical benchmark code against both sides.
//   - Deterministic: one fixed canonical workload (no RNG), so run-to-run variance is the machine,
//     not the data.
//   - Comparable: throughput benchmarks set b.SetBytes to the LOGICAL (uncompressed) size so MB/s is
//     a real ingest/scan speed, and report Mpoints/s; the density benchmark reports B/point.
//
// Keep this set small and stable. Changing the workload resets the historical baseline, so only do
// it deliberately. All names live under BenchmarkGolden/… and the workflow targets `^BenchmarkGolden$`.

import (
	"bytes"
	"context"
	"errors"
	"io"
	"testing"
	"time"

	promengine "github.com/prometheus/prometheus/promql"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/query/fetch"
	promadapter "github.com/oteldb/storage/query/promql"
	"github.com/oteldb/storage/signal"
	"github.com/oteldb/storage/signal/metric"
)

const (
	goldenSeries   = 1000
	goldenPoints   = 100
	goldenTotal    = goldenSeries * goldenPoints // 100k points
	goldenInterval = int64(15_000_000_000)       // 15s, constant ⇒ delta-of-delta ≈ 0
	goldenStartTs  = int64(1_600_000_000_000_000_000)
	// goldenLogicalBytes is the uncompressed footprint of one batch: 8-byte timestamp + 8-byte value
	// per point. b.SetBytes uses it so MB/s reflects logical throughput, not the encoded size.
	goldenLogicalBytes = int64(goldenTotal) * 16
)

var goldenName = []byte("golden.metric")

// goldenCorpus is THE canonical golden workload: one cumulative counter spanning goldenSeries
// series (a "route" label), each with goldenPoints samples at a constant interval. Built once,
// reused read-only. Fully deterministic (no RNG).
func goldenCorpus() metric.Metrics {
	var md metric.Metrics
	rm := md.AddResource()
	rm.Resource = signal.Resource{Attributes: signal.NewAttributes(
		signal.KeyValue{Key: []byte("service.name"), Value: signal.StringValue([]byte("golden"))})}
	mt := rm.AddScope().AddMetric()
	mt.Name = goldenName
	mt.Kind = metric.KindSum
	mt.Temporality = metric.TemporalityCumulative
	mt.Monotonic = true

	for s := range goldenSeries {
		route := append([]byte("/route/"), []byte(itoa(s))...)
		attrs := signal.NewAttributes(signal.KeyValue{Key: []byte("route"), Value: signal.StringValue(route)})
		for p := range goldenPoints {
			pt := mt.AddPoint()
			pt.Ts = goldenStartTs + int64(p)*goldenInterval
			pt.StartTs = goldenStartTs
			pt.Value = float64(p) // monotonic integer ramp
			pt.Attributes = attrs
		}
	}

	return md
}

// itoa is a tiny allocation-light base-10 formatter (avoids importing strconv only for this).
func itoa(n int) string {
	if n == 0 {
		return "0"
	}

	var buf [20]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}

	return string(buf[i:])
}

func goldenMatcher() fetch.Matcher {
	return fetch.Matcher{Name: metric.LabelName, Match: func(v signal.Value) bool { return bytes.Equal(v.Str(), goldenName) }}
}

// goldenFlushedStore builds a store on a memory backend, ingests the canonical corpus, and flushes
// + compacts it to a steady-state part — the read benchmarks' fixture. Returns the store and its
// backend.
func goldenFlushedStore(b *testing.B) (*Storage, backend.Backend) {
	b.Helper()

	ctx := context.Background()
	be := backend.Memory()

	s, err := Open(ctx, Options{}, WithBackend(be))
	if err != nil {
		b.Fatal(err)
	}

	if _, err := s.WriteMetrics(ctx, goldenCorpus()); err != nil {
		b.Fatal(err)
	}

	goldenFlushMerge(b, s)

	return s, be
}

// goldenFlushMerge flushes the default tenant's head to a part and compacts, via the stable
// internal engine handle (no public flush exists).
func goldenFlushMerge(b *testing.B, s *Storage) {
	b.Helper()

	ctx := context.Background()

	eng, err := s.engineFor("default")
	if err != nil {
		b.Fatal(err)
	}

	if err := eng.Flush(ctx); err != nil {
		b.Fatal(err)
	}

	if err := eng.Merge(ctx, 0); err != nil {
		b.Fatal(err)
	}
}

// goldenDrain reads an iterator to completion, returning the total rows seen.
func goldenDrain(b *testing.B, ctx context.Context, it fetch.Iterator) int {
	b.Helper()

	rows := 0
	for {
		batch, err := it.Next(ctx)
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}

			b.Fatal(err)
		}

		rows += len(batch.Timestamps)
	}

	if err := it.Close(); err != nil {
		b.Fatal(err)
	}

	return rows
}

// goldenReportPoints reports Mpoints/s and ns/point from the timed window over the points ingested.
func goldenReportPoints(b *testing.B, pointsPerOp int) {
	b.Helper()

	total := float64(pointsPerOp) * float64(b.N)
	secs := b.Elapsed().Seconds()
	if total == 0 || secs == 0 {
		return
	}

	b.ReportMetric(total/secs/1e6, "Mpoints/s")
}

// goldenIsPartKey reports whether a backend key names a flushed value part (all-digit final
// segment), so the density benchmark can isolate value-part bytes from index overhead.
func goldenIsPartKey(key string) bool {
	last := key
	for i := len(key) - 1; i >= 0; i-- {
		if key[i] == '/' {
			last = key[i+1:]

			break
		}
	}

	if last == "" {
		return false
	}

	for i := range len(last) {
		if last[i] < '0' || last[i] > '9' {
			return false
		}
	}

	return true
}

// BenchmarkGolden is the definitive read+write performance set. Sub-benchmarks:
//
//	write/head        — head ingest throughput (projection + identity + append), no flush
//	write/flush       — ingest + flush to an immutable columnar part (encode + backend write)
//	write/concurrent  — aggregate ingest throughput under many concurrent writers
//	read/fetch_all    — fetch every series over the full range and drain (decode + merge)
//	read/fetch_recent — fetch a narrow recent window (granule/time pruning)
//	density           — bytes/point of the value parts after flush + compaction (codec efficiency)
func BenchmarkGolden(b *testing.B) {
	b.Run("write/head", benchGoldenWriteHead)
	b.Run("write/flush", benchGoldenWriteFlush)
	b.Run("write/concurrent", benchGoldenWriteConcurrent)
	b.Run("read/fetch_all", benchGoldenFetchAll)
	b.Run("read/fetch_all_release", benchGoldenFetchAllRelease)
	b.Run("read/fetch_recent", benchGoldenFetchRecent)
	b.Run("query/promql_count_cpu_cores", benchGoldenPromQLCountCPU)
	b.Run("query/promql_full_scan_count", benchGoldenPromQLFullScan)
	b.Run("density", benchGoldenDensity)
}

// node_cpu_seconds_total benchmark fixture — a node_exporter-shaped counter exercising the realistic
// nested-aggregation query `count_cpu_cores` from /src/oteldb/benchmark
// (count(count(node_cpu_seconds_total{job="node_exporter"}) by (cpu))). The corpus has job+instance
// on the resource and cpu+mode on the data points, so every node_exporter label is matchable.
const (
	nodeInstances = 4
	nodeCPUs      = 16
	nodeModes     = 8
	nodeCPUPoints = 50 // ⇒ nodeInstances*nodeCPUs*nodeModes = 512 series
)

var (
	nodeCPUName  = []byte("node_cpu_seconds_total")
	nodeCPUModes = [nodeModes]string{"user", "system", "idle", "iowait", "irq", "softirq", "nice", "steal"}
	// countCPUCoresExpr is the benchmark query: an inner per-cpu count collapsed by an outer count —
	// the canonical "how many CPU cores" PromQL expression.
	countCPUCoresExpr = `count(count(node_cpu_seconds_total{job="node_exporter"}) by (cpu))`
	// fullScanCountExpr is the worst-case full-series count: a `__name__` regex matches every node_
	// metric, so it cannot be pushed to the postings index — the querier enumerates and filters all
	// series. count_cpu_cores' opposite (an equality matcher prunes; this one scans).
	fullScanCountExpr = `count({__name__=~"node_.+"})`
)

// nodeCPUCorpus builds the deterministic node_cpu_seconds_total workload (no RNG): per instance, a
// cumulative monotonic counter over every (cpu, mode) pair, each a ramp of nodeCPUPoints samples.
func nodeCPUCorpus() metric.Metrics {
	var md metric.Metrics

	for inst := range nodeInstances {
		rm := md.AddResource()
		rm.Resource = signal.Resource{Attributes: signal.NewAttributes(
			signal.KeyValue{Key: []byte("job"), Value: signal.StringValue([]byte("node_exporter"))},
			signal.KeyValue{Key: []byte("instance"), Value: signal.StringValue(append([]byte("host-"), itoa(inst)...))},
		)}

		mt := rm.AddScope().AddMetric()
		mt.Name = nodeCPUName
		mt.Kind = metric.KindSum
		mt.Temporality = metric.TemporalityCumulative
		mt.Monotonic = true

		for cpu := range nodeCPUs {
			for mode := range nodeCPUModes {
				attrs := signal.NewAttributes(
					signal.KeyValue{Key: []byte("cpu"), Value: signal.StringValue([]byte(itoa(cpu)))},
					signal.KeyValue{Key: []byte("mode"), Value: signal.StringValue([]byte(nodeCPUModes[mode]))},
				)

				for p := range nodeCPUPoints {
					pt := mt.AddPoint()
					pt.Ts = goldenStartTs + int64(p)*goldenInterval
					pt.StartTs = goldenStartTs
					pt.Value = float64(p)
					pt.Attributes = attrs
				}
			}
		}
	}

	return md
}

// nodeCPUStore builds, ingests, and flush+compacts the node_cpu_seconds_total corpus into a memory-
// backed store — the shared fixture for the PromQL golden queries.
func nodeCPUStore(b *testing.B) *Storage {
	b.Helper()

	ctx := context.Background()

	s, err := Open(ctx, Options{}, WithBackend(backend.Memory()), WithDecodeCache(64<<20))
	if err != nil {
		b.Fatal(err)
	}

	if _, err := s.WriteMetrics(ctx, nodeCPUCorpus()); err != nil {
		b.Fatal(err)
	}

	goldenFlushMerge(b, s)

	return s
}

// goldenPromQL builds a Prometheus engine + storage adapter over the store and the instant-eval
// timestamp at the corpus's last sample.
func goldenPromQL(s *Storage) (*promengine.Engine, *promadapter.Queryable, time.Time) {
	eng := promengine.NewEngine(promengine.EngineOpts{MaxSamples: 50_000_000, Timeout: time.Minute, LookbackDelta: 5 * time.Minute})
	qa := promadapter.NewQueryable(s.Fetcher("default"), "default")

	return eng, qa, time.Unix(0, goldenStartTs+int64(nodeCPUPoints-1)*goldenInterval)
}

// goldenInstantScalar runs an instant query that must yield a single scalar sample and returns it.
func goldenInstantScalar(b *testing.B, eng *promengine.Engine, qa *promadapter.Queryable, expr string, ts time.Time) float64 {
	b.Helper()

	ctx := context.Background()

	q, err := eng.NewInstantQuery(ctx, qa, nil, expr, ts)
	if err != nil {
		b.Fatal(err)
	}

	defer q.Close()

	res := q.Exec(ctx)
	if res.Err != nil {
		b.Fatal(res.Err)
	}

	vec, err := res.Vector()
	if err != nil {
		b.Fatal(err)
	}

	if len(vec) != 1 {
		b.Fatalf("expected a single scalar result, got %d samples", len(vec))
	}

	return vec[0].F
}

// goldenPromQLBench is the shared body for the instant-query golden benchmarks: it builds the
// fixture, sanity-checks the expression once against want, then times repeated evaluations.
func goldenPromQLBench(b *testing.B, expr string, want float64) {
	b.Helper()

	s := nodeCPUStore(b)
	defer func() { _ = s.Close(context.Background()) }()

	eng, qa, ts := goldenPromQL(s)

	if got := goldenInstantScalar(b, eng, qa, expr, ts); got != want {
		b.Fatalf("%s = %v, want %v", expr, got, want)
	}

	b.ReportAllocs()
	b.ResetTimer()

	for range b.N {
		goldenInstantScalar(b, eng, qa, expr, ts)
	}
}

// benchGoldenPromQLCountCPU measures the end-to-end PromQL path: a Prometheus promql.Engine running
// the count_cpu_cores nested aggregation over the storage fetch contract via the query/promql
// adapter. It covers matcher push-down, series fetch + decode, label projection, and the engine's
// inner+outer count — the realistic embedder query path, not just the fetch seam. The result is the
// number of distinct cpu values (one group per core).
func benchGoldenPromQLCountCPU(b *testing.B) {
	goldenPromQLBench(b, countCPUCoresExpr, float64(nodeCPUs))
}

// benchGoldenPromQLFullScan is the worst case: a `__name__` regex matches every node_ series, so no
// matcher prunes the index and the querier fetches and filters all series before the outer count —
// the antithesis of the equality-pruned count_cpu_cores. The result is the total series count.
func benchGoldenPromQLFullScan(b *testing.B) {
	goldenPromQLBench(b, fullScanCountExpr, float64(nodeInstances*nodeCPUs*nodeModes))
}

// benchGoldenFetchAllRelease is fetch_all where the consumer Releases each batch after use — the
// realistic embedder pattern (copy out, then release). It measures the Batch.Release buffer pooling:
// vs fetch_all, the per-query result allocations drop to near zero.
func benchGoldenFetchAllRelease(b *testing.B) {
	ctx := context.Background()
	s, _ := goldenFlushedStore(b)
	defer func() { _ = s.Close(ctx) }()

	f := s.Fetcher("default")
	req := fetch.Request{Start: 0, End: 1 << 62, Matchers: []fetch.Matcher{goldenMatcher()}, Recycle: true}

	var rows int

	b.SetBytes(goldenLogicalBytes)
	b.ReportAllocs()
	b.ResetTimer()

	for range b.N {
		it, err := f.Fetch(ctx, req)
		if err != nil {
			b.Fatal(err)
		}

		rows = 0

		for {
			batch, err := it.Next(ctx)
			if err != nil {
				if errors.Is(err, io.EOF) {
					break
				}

				b.Fatal(err)
			}

			rows += len(batch.Timestamps)
			batch.Release() // done with the batch — recycle its buffers
		}

		if err := it.Close(); err != nil {
			b.Fatal(err)
		}
	}

	b.StopTimer()
	b.ReportMetric(float64(rows), "rows/op")
}

func benchGoldenWriteHead(b *testing.B) {
	ctx := context.Background()
	md := goldenCorpus()
	resetEvery := max((1<<20)/goldenTotal, 1)

	s, err := InMemory()
	if err != nil {
		b.Fatal(err)
	}
	defer func() { _ = s.Close(ctx) }()

	b.SetBytes(goldenLogicalBytes)
	b.ReportAllocs()
	b.ResetTimer()

	for i := range b.N {
		if _, err := s.WriteMetrics(ctx, md); err != nil {
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

	goldenReportPoints(b, goldenTotal)
}

func benchGoldenWriteFlush(b *testing.B) {
	ctx := context.Background()
	md := goldenCorpus()
	resetEvery := max((1<<20)/goldenTotal, 1)

	be := backend.Memory()
	s, err := Open(ctx, Options{}, WithBackend(be))
	if err != nil {
		b.Fatal(err)
	}
	defer func() { _ = s.Close(ctx) }()

	eng, err := s.engineFor("default")
	if err != nil {
		b.Fatal(err)
	}

	b.SetBytes(goldenLogicalBytes)
	b.ReportAllocs()
	b.ResetTimer()

	for i := range b.N {
		if _, err := s.WriteMetrics(ctx, md); err != nil {
			b.Fatal(err)
		}
		if err := eng.Flush(ctx); err != nil {
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

	goldenReportPoints(b, goldenTotal)
}

func benchGoldenWriteConcurrent(b *testing.B) {
	ctx := context.Background()

	s, err := InMemory()
	if err != nil {
		b.Fatal(err)
	}
	defer func() { _ = s.Close(ctx) }()

	const resetEvery = 1 << 4 // bound the shared head across goroutines

	var iters int
	b.SetBytes(goldenLogicalBytes)
	b.ReportAllocs()
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		md := goldenCorpus() // each goroutine owns its batch; all build the same series set

		for pb.Next() {
			if _, err := s.WriteMetrics(ctx, md); err != nil {
				b.Error(err)

				return
			}

			iters++
			if iters%resetEvery == 0 {
				if err := s.Reset(ctx); err != nil {
					b.Error(err)

					return
				}
			}
		}
	})

	goldenReportPoints(b, goldenTotal)
}

func benchGoldenFetchAll(b *testing.B) {
	ctx := context.Background()
	s, _ := goldenFlushedStore(b)
	defer func() { _ = s.Close(ctx) }()

	f := s.Fetcher("default")
	req := fetch.Request{Start: 0, End: 1 << 62, Matchers: []fetch.Matcher{goldenMatcher()}}

	var rows int
	b.SetBytes(goldenLogicalBytes)
	b.ReportAllocs()
	b.ResetTimer()

	for range b.N {
		it, err := f.Fetch(ctx, req)
		if err != nil {
			b.Fatal(err)
		}

		rows = goldenDrain(b, ctx, it)
	}

	b.StopTimer()
	b.ReportMetric(float64(rows), "rows/op")
}

func benchGoldenFetchRecent(b *testing.B) {
	ctx := context.Background()
	s, _ := goldenFlushedStore(b)
	defer func() { _ = s.Close(ctx) }()

	// The last 10 points' window — exercises time pruning (most granules are skipped).
	start := goldenStartTs + int64(goldenPoints-10)*goldenInterval
	f := s.Fetcher("default")
	req := fetch.Request{Start: start, End: 1 << 62, Matchers: []fetch.Matcher{goldenMatcher()}}

	var rows int
	b.ReportAllocs()
	b.ResetTimer()

	for range b.N {
		it, err := f.Fetch(ctx, req)
		if err != nil {
			b.Fatal(err)
		}

		rows = goldenDrain(b, ctx, it)
	}

	b.StopTimer()
	b.ReportMetric(float64(rows), "rows/op")
}

func benchGoldenDensity(b *testing.B) {
	ctx := context.Background()

	var partBytes int64

	for range b.N {
		be := backend.Memory()
		s, err := Open(ctx, Options{}, WithBackend(be))
		if err != nil {
			b.Fatal(err)
		}

		if _, err := s.WriteMetrics(ctx, goldenCorpus()); err != nil {
			b.Fatal(err)
		}

		goldenFlushMerge(b, s)

		keys, err := be.List(ctx, "")
		if err != nil {
			b.Fatal(err)
		}

		partBytes = 0
		for _, k := range keys {
			if !goldenIsPartKey(k) {
				continue
			}

			obj, err := be.Read(ctx, k)
			if err != nil {
				b.Fatal(err)
			}

			partBytes += int64(len(obj))
		}

		if err := s.Close(ctx); err != nil {
			b.Fatal(err)
		}
	}

	b.ReportMetric(float64(partBytes)/float64(goldenTotal), "B/point")
}

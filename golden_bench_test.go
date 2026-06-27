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

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/query/fetch"
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
	b.Run("read/fetch_recent", benchGoldenFetchRecent)
	b.Run("density", benchGoldenDensity)
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

package storage

// Density & fetch-latency harness — turns the storage tier's headline claims (sub-byte/point
// on disk, low-latency selective fetch) into measured numbers, and gates them against
// regression.
//
// It measures two things on a synthetic-but-realistic OTel metrics corpus:
//
//   - On-disk density: total backend bytes / total points = bytes/point, against the
//     0.4–0.8 B/point target. Measured backend-agnostically by listing every stored object
//     and summing its length, split into part bytes (the value columns) vs identity/index
//     bytes (series.bin + bucket index — the per-series overhead a low-points-per-series
//     corpus pays).
//   - Fetch latency: ns/op and rows/op for a representative selective fetch over flushed parts.
//
// Run:
//
//	go test -run TestDensityReport -v ./        # prints the B/point table
//	go test -bench 'BenchmarkDensity|BenchmarkFetchLatency' -benchmem ./
//
// The corpus profiles below are deliberately a stable, documented contract: the same shapes
// can be replayed into any other engine (push the identical OTLP corpus, flush + compact,
// compare its data-dir size / total points against the B/point reported here, and compare
// query latency on an equivalent matcher) for an apples-to-apples comparison.

import (
	"context"
	"errors"
	"io"
	"math"
	"math/rand"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/engine"
	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/signal"
	"github.com/oteldb/storage/signal/metric"
)

// valuePattern selects the per-point value shape — the dominant factor in how well the value
// column compresses, so density profiles must span the realistic range.
type valuePattern int

const (
	patCounter     valuePattern = iota // monotonic ramp — delta-encodes to near-nothing (the common counter)
	patRandWalk                        // full-precision gauge random walk — near the lossless float entropy floor
	patBoundedWalk                     // gauge random walk at realistic 1-decimal precision (CPU %, rates, ratios)
	patConstant                        // unchanging value — the best case (RLE/const-collapse)
	patNoisy                           // uniform-random floats — the adversarial worst case
)

// corpusProfile is one workload shape. The same shapes must be replayed into any engine the
// numbers are compared against for the comparison to be meaningful (see file header).
type corpusProfile struct {
	name           string
	series, points int
	interval       int64 // ns between consecutive points (constant ⇒ delta-of-delta ≈ 0 bits)
	kind           metric.PointKind
	pattern        valuePattern
}

// densityProfiles are sized so fixed per-part overhead is amortized (≥100k points) while the
// test stays sub-second. They isolate the value-codec axis (counter/randwalk/constant/noisy)
// and the identity-overhead axis (wide_shallow: many series, few points each).
var densityProfiles = []corpusProfile{
	{"counter_200k", 2000, 100, 15_000_000_000, metric.KindSum, patCounter},
	{"gauge_randwalk_200k", 2000, 100, 15_000_000_000, metric.KindGauge, patRandWalk},
	{"gauge_bounded_200k", 2000, 100, 15_000_000_000, metric.KindGauge, patBoundedWalk},
	{"gauge_constant_200k", 2000, 100, 15_000_000_000, metric.KindGauge, patConstant},
	{"gauge_noisy_200k", 2000, 100, 15_000_000_000, metric.KindGauge, patNoisy},
	{"wide_shallow_100k", 50_000, 2, 15_000_000_000, metric.KindGauge, patRandWalk},
}

// buildCorpus materializes one profile into a metric.Metrics batch: a single metric whose
// points span `series` distinct label sets (route + pod), each with `points` samples at a
// constant interval. Deterministic for a given seed so density numbers are reproducible.
func buildCorpus(p corpusProfile, seed int64) metric.Metrics {
	rng := rand.New(rand.NewSource(seed))

	var md metric.Metrics
	rm := md.AddResource()
	rm.Resource = signal.Resource{Attributes: signal.NewAttributes(
		signal.KeyValue{Key: []byte("service.name"), Value: signal.StringValue([]byte("bench"))},
	)}
	sm := rm.AddScope()
	sm.Scope = signal.Scope{Name: []byte("benchlib")}
	mt := sm.AddMetric()
	mt.Name = []byte("bench.metric")
	mt.Unit = []byte("1")
	mt.Kind = p.kind
	if p.kind == metric.KindSum {
		mt.Temporality = metric.TemporalityCumulative
		mt.Monotonic = true
	}

	const startTs = int64(1_600_000_000_000_000_000) // a fixed, realistic nanosecond epoch

	for s := range p.series {
		attrs := signal.NewAttributes(
			signal.KeyValue{Key: []byte("route"), Value: signal.StringValue([]byte("/api/v1/resource/" + strconv.Itoa(s)))},
			signal.KeyValue{Key: []byte("pod"), Value: signal.StringValue([]byte("pod-" + strconv.Itoa(s%64)))},
		)
		val := 1000 * rng.Float64()

		for i := range p.points {
			pt := mt.AddPoint()
			pt.Ts = startTs + int64(i)*p.interval
			pt.Attributes = attrs

			switch p.pattern {
			case patCounter:
				val += rng.Float64() * 10
				pt.Value = math.Floor(val)
				pt.StartTs = startTs
			case patRandWalk:
				val += rng.NormFloat64()
				pt.Value = val
			case patBoundedWalk:
				val += rng.NormFloat64()
				pt.Value = math.Round(val*10) / 10 // 1-decimal precision, like a real gauge
			case patConstant:
				pt.Value = 42
			case patNoisy:
				pt.Value = rng.Float64() * 1e6
			}
		}
	}

	return md
}

// densityResult is one measured profile: the on-disk footprint split into value-part bytes
// vs identity/index bytes, plus the headline bytes/point.
type densityResult struct {
	points             int
	totalBytes         int64
	partBytes          int64 // value columns (numeric-suffixed part objects)
	indexBytes         int64 // series.bin + bucket index — per-series identity overhead
	bppTotal, bppParts float64
}

// isPartKey reports whether a backend key names a flushed value part (`{prefix}/{seq:010d}`),
// as opposed to the identity index (`series.bin`) or the bucket index. Part objects have an
// all-digit final path segment.
func isPartKey(key string) bool {
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
	for i := 0; i < len(last); i++ {
		if last[i] < '0' || last[i] > '9' {
			return false
		}
	}
	return true
}

// measureDensity ingests one profile into a durable in-memory backend, flushes + compacts it
// to steady-state parts, and totals the resulting on-disk bytes. It uses the public backend
// seam (List + Read) so the number is backend-agnostic and would be identical on file/S3.
func measureDensity(tb testing.TB, p corpusProfile) densityResult {
	tb.Helper()

	ctx := context.Background()
	be := backend.Memory()

	s, err := Open(ctx, Options{}, WithBackend(be))
	require.NoError(tb, err)
	defer func() { require.NoError(tb, s.Close(ctx)) }()

	md := buildCorpus(p, 1)
	_, err = s.WriteMetrics(ctx, md)
	require.NoError(tb, err)

	eng := mustEngine(s.engineFor("default"))
	require.NoError(tb, eng.Flush(ctx))    // head → immutable part(s)
	require.NoError(tb, eng.Merge(ctx, 0)) // compact to steady state; retain everything

	keys, err := be.List(ctx, "")
	require.NoError(tb, err)

	res := densityResult{points: p.series * p.points}
	for _, k := range keys {
		data, err := be.Read(ctx, k)
		require.NoError(tb, err)

		n := int64(len(data))
		res.totalBytes += n
		if isPartKey(k) {
			res.partBytes += n
		} else {
			res.indexBytes += n
		}
	}

	if res.points > 0 {
		res.bppTotal = float64(res.totalBytes) / float64(res.points)
		res.bppParts = float64(res.partBytes) / float64(res.points)
	}

	return res
}

// TestDensityReport prints the bytes/point table — the artifact to read against the
// 0.4–0.8 B/point target and to compare with another engine's du-per-point (see file header). It
// also acts as a loose regression gate: the value parts for highly compressible shapes
// (constant value at constant interval; a clean counter ramp) must beat the 8-byte raw
// float64 they'd cost uncompressed.
func TestDensityReport(t *testing.T) {
	if testing.Short() {
		t.Skip("density report ingests sizable corpora; skipped under -short")
	}

	t.Logf("%-22s %10s %12s %12s %12s %12s", "profile", "points", "total_B", "B/pt(all)", "B/pt(part)", "idx_B")
	for _, p := range densityProfiles {
		r := measureDensity(t, p)
		t.Logf("%-22s %10d %12d %12.3f %12.3f %12d",
			p.name, r.points, r.totalBytes, r.bppTotal, r.bppParts, r.indexBytes)

		switch p.pattern {
		case patConstant:
			require.Less(t, r.bppParts, 2.0, "constant value at constant interval must compress hard")
		case patCounter:
			require.Less(t, r.bppParts, 8.0, "a clean counter ramp must beat raw 8-byte float64")
		}
	}
}

// measureLossyDensity is measureDensity with an age-tiered lossy precision budget applied to the
// (cold) part at merge: Bits significant mantissa bits in the value column (0 ⇒ lossless). The
// precision tier's Before is set past every sample so the whole part is treated as cold.
func measureLossyDensity(tb testing.TB, p corpusProfile, bits uint8) densityResult {
	tb.Helper()

	ctx := context.Background()
	be := backend.Memory()

	s, err := Open(ctx, Options{}, WithBackend(be))
	require.NoError(tb, err)
	defer func() { require.NoError(tb, s.Close(ctx)) }()

	_, err = s.WriteMetrics(ctx, buildCorpus(p, 1))
	require.NoError(tb, err)

	eng := mustEngine(s.engineFor("default"))
	require.NoError(tb, eng.Flush(ctx))

	opts := engine.MergeOptions{}
	if bits > 0 {
		opts.Precision = []engine.PrecisionTier{{Before: 1 << 62, Bits: bits}}
	}
	require.NoError(tb, eng.MergeWith(ctx, opts))

	keys, err := be.List(ctx, "")
	require.NoError(tb, err)

	res := densityResult{points: p.series * p.points}
	for _, k := range keys {
		data, err := be.Read(ctx, k)
		require.NoError(tb, err)

		n := int64(len(data))
		res.totalBytes += n
		if isPartKey(k) {
			res.partBytes += n
		} else {
			res.indexBytes += n
		}
	}

	if res.points > 0 {
		res.bppTotal = float64(res.totalBytes) / float64(res.points)
		res.bppParts = float64(res.partBytes) / float64(res.points)
	}

	return res
}

// lossyBudgets is the precision-bits sweep for BenchmarkLossyPrecision: 0 = lossless baseline, then
// decreasing significant-bit budgets (denser, less accurate).
var lossyBudgets = []uint8{0, 28, 20, 16, 12, 10, 8}

// BenchmarkLossyPrecision reports the bytes/point-vs-precision curve for age-tiered lossy float
// compression on a high-entropy gauge (the case where lossy actually helps): each sub-benchmark
// applies one precision budget at merge and reports B/point of the value parts plus the % saved
// vs lossless. On a structureless or already-dense column the adaptive encoder keeps the lossless
// codec, so %saved would read ~0 — the point of using the random-walk profile here.
//
//	go test -bench BenchmarkLossyPrecision ./
func BenchmarkLossyPrecision(b *testing.B) {
	p := corpusProfile{"gauge_randwalk", 2000, 100, 15_000_000_000, metric.KindGauge, patRandWalk}

	lossless := measureLossyDensity(b, p, 0).bppParts

	for _, bits := range lossyBudgets {
		name := "lossless"
		if bits > 0 {
			name = "bits=" + strconv.Itoa(int(bits))
		}

		b.Run(name, func(b *testing.B) {
			var last densityResult
			for range b.N {
				last = measureLossyDensity(b, p, bits)
			}

			b.ReportMetric(last.bppParts, "B/point")

			saved := 0.0
			if lossless > 0 {
				saved = (1 - last.bppParts/lossless) * 100
			}
			b.ReportMetric(saved, "%saved")
		})
	}
}

// BenchmarkDensity reports B/point (value parts and total) as a custom benchmark metric per
// profile, so density tracks in the same benchstat pipeline as throughput. The ingest→flush→
// merge cycle is the timed work; the reported B/point is a property of the produced parts.
func BenchmarkDensity(b *testing.B) {
	for _, p := range densityProfiles {
		b.Run(p.name, func(b *testing.B) {
			var last densityResult
			for range b.N {
				last = measureDensity(b, p)
			}
			b.ReportMetric(last.bppParts, "B/point-parts")
			b.ReportMetric(last.bppTotal, "B/point-total")
		})
	}
}

// fetchProfiles are smaller than densityProfiles: a fetch fully decodes every matched series,
// so the wide/large density shapes are impractical to time. These keep the latency baseline
// usable while still spanning the value-codec axis.
var fetchProfiles = []corpusProfile{
	{"counter_50k", 500, 100, 15_000_000_000, metric.KindSum, patCounter},
	{"gauge_randwalk_50k", 500, 100, 15_000_000_000, metric.KindGauge, patRandWalk},
	{"gauge_constant_50k", 500, 100, 15_000_000_000, metric.KindGauge, patConstant},
}

// BenchmarkFetchLatency measures selective-fetch latency over flushed parts: select the whole
// metric (every series) across its full time range and fully drain the iterator. It reports
// ns/op (the latency headline to compare against another engine's query path) and rows/op. The
// store is built once outside the timed loop; only Fetch + drain is timed.
func BenchmarkFetchLatency(b *testing.B) {
	for _, p := range fetchProfiles {
		b.Run(p.name, func(b *testing.B) {
			ctx := context.Background()
			s, err := Open(ctx, Options{}, WithBackend(backend.Memory()))
			require.NoError(b, err)
			defer func() { require.NoError(b, s.Close(ctx)) }()

			_, err = s.WriteMetrics(ctx, buildCorpus(p, 1))
			require.NoError(b, err)
			eng := mustEngine(s.engineFor("default"))
			require.NoError(b, eng.Flush(ctx))
			require.NoError(b, eng.Merge(ctx, 0))

			req := fetch.Request{
				Start:    0,
				End:      1 << 62,
				Matchers: []fetch.Matcher{nameMatcher("bench.metric")},
			}
			f := s.Fetcher("default")

			var rows int64
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
					rows += int64(len(batch.Timestamps))
				}
				if err := it.Close(); err != nil {
					b.Fatal(err)
				}
			}
			b.StopTimer()
			b.ReportMetric(float64(rows), "rows/op")
		})
	}
}

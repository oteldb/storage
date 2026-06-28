package storage

// Head-resident PromQL query benchmarks — the loop-with-pprof harness for the live fetch path.
//
// BenchmarkGolden's PromQL set flushes + compacts before querying, so it measures the immutable
// columnar-part read path. But the live /src/oteldb/benchmark profile (vmagent remote-writes
// node_exporter continuously; queries hit the last ~120s, still in the head) is dominated by the
// HEAD fetch path — engine.bufBatch → sortedWindow → windowCopy, plus the postings merge — none of
// which a flushed fixture exercises. These benchmarks query the live head (ephemeral store, no
// flush) so that path shows up under pprof, and they're sized to the live full_scan cardinality
// (~2560 series) so the postings merge and per-series window copy are stressed realistically.
//
// Loop one under pprof without the docker harness:
//
//	go test -run=^$ -bench='^BenchmarkHeadQuery$/suite' -benchtime=5s \
//	    -cpuprofile=/tmp/cpu.out -memprofile=/tmp/mem.out .
//	go tool pprof -top /tmp/cpu.out
//	go tool pprof -top -sample_index=alloc_space /tmp/mem.out
//
// The /suite sub-benchmark runs all three query shapes per op, so a single capture covers the whole
// query mix; the individual sub-benchmarks isolate one shape each.

import (
	"context"
	"testing"
	"time"

	"github.com/oteldb/storage/signal"
	"github.com/oteldb/storage/signal/metric"
)

const (
	// 20 instances × 16 cpus × 8 modes = 2560 series — the live node_exporter full_scan cardinality
	// from /src/oteldb/benchmark, so the head postings merge and per-series windowCopy see real width.
	headInstances = 20
	headCPUs      = 16
	// headPoints spans a ~16m window at the 15s golden interval; queries below window the recent tail
	// (instant at the last sample, range over a 1m irate). nodeModes (8) is reused for the mode count.
	headPoints = 64
	headSeries = headInstances * headCPUs * nodeModes // 2560
)

// headCorpus builds the deterministic node_cpu_seconds_total workload at head scale (no RNG): per
// instance, a cumulative monotonic counter over every (cpu, mode) pair, each a ramp of headPoints
// samples. Mirrors nodeCPUCorpus but at headInstances width so the head holds ~2560 series.
func headCorpus() metric.Metrics {
	var md metric.Metrics

	for inst := range headInstances {
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

		for cpu := range headCPUs {
			for mode := range nodeCPUModes {
				attrs := signal.NewAttributes(
					signal.KeyValue{Key: []byte("cpu"), Value: signal.StringValue([]byte(itoa(cpu)))},
					signal.KeyValue{Key: []byte("mode"), Value: signal.StringValue([]byte(nodeCPUModes[mode]))},
				)

				for p := range headPoints {
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

// headCPUStore ingests the head-scale corpus into an ephemeral (head-resident, never flushed) store,
// so queries exercise the live head fetch path. Deliberately NO flush: the corpus stays in the head,
// so Fetch goes through bufBatch / sortedWindow / windowCopy — the live path the production profile
// is dominated by.
func headCPUStore(b *testing.B) *Storage {
	b.Helper()

	ctx := context.Background()

	s, err := InMemory(WithDecodeCache(64 << 20))
	if err != nil {
		b.Fatal(err)
	}

	if _, err := s.WriteMetrics(ctx, headCorpus()); err != nil {
		b.Fatal(err)
	}

	return s
}

// headQueryTs is the instant-eval timestamp at the corpus's last sample.
func headQueryTs() time.Time {
	return time.Unix(0, goldenStartTs+int64(headPoints-1)*goldenInterval)
}

// BenchmarkHeadQuery is the head-resident PromQL query set. Each sub-benchmark queries the live head
// (never flushed) so the bufBatch / sortedWindow / windowCopy fetch path and the 2560-series postings
// merge are exercised — the hotspots of the live /src/oteldb/benchmark CPU profile.
//
//	count_cpu_cores — equality-pruned nested aggregation (index push-down hits)
//	full_scan       — __name__ regex, no pruning: enumerate + window-copy every series
//	cpu_usage_range — range query: irate window iteration + sum-by + vector-matched division
//	suite           — all three per op (one pprof capture covers the whole mix)
func BenchmarkHeadQuery(b *testing.B) {
	b.Run("count_cpu_cores", benchHeadCountCPU)
	b.Run("full_scan", benchHeadFullScan)
	b.Run("cpu_usage_range", benchHeadCPUUsage)
	b.Run("suite", benchHeadSuite)
}

func benchHeadCountCPU(b *testing.B) {
	s := headCPUStore(b)
	defer func() { _ = s.Close(context.Background()) }()

	eng, qa, _ := goldenPromQL(s)
	ts := headQueryTs()

	if got := goldenInstantScalar(b, eng, qa, countCPUCoresExpr, ts); got != float64(headCPUs) {
		b.Fatalf("count_cpu_cores = %v, want %v", got, headCPUs)
	}

	b.ReportAllocs()
	b.ResetTimer()

	for range b.N {
		goldenInstantScalar(b, eng, qa, countCPUCoresExpr, ts)
	}
}

func benchHeadFullScan(b *testing.B) {
	s := headCPUStore(b)
	defer func() { _ = s.Close(context.Background()) }()

	eng, qa, _ := goldenPromQL(s)
	ts := headQueryTs()

	if got := goldenInstantScalar(b, eng, qa, fullScanCountExpr, ts); got != float64(headSeries) {
		b.Fatalf("full_scan = %v, want %v", got, headSeries)
	}

	b.ReportAllocs()
	b.ResetTimer()

	for range b.N {
		goldenInstantScalar(b, eng, qa, fullScanCountExpr, ts)
	}
}

func benchHeadCPUUsage(b *testing.B) {
	s := headCPUStore(b)
	defer func() { _ = s.Close(context.Background()) }()

	eng, qa, _ := goldenPromQL(s)
	start := time.Unix(0, goldenStartTs+int64(time.Minute))
	end := time.Unix(0, goldenStartTs+int64(headPoints-1)*goldenInterval)
	step := time.Minute

	want := 1.0 / float64(nodeModes)
	if m := goldenRangeMatrix(b, eng, qa, cpuUsageExpr, start, end, step); len(m) != headInstances {
		b.Fatalf("cpu_usage returned %d series, want %d", len(m), headInstances)
	} else {
		for _, ser := range m {
			for _, p := range ser.Floats {
				if p.F < want-1e-6 || p.F > want+1e-6 {
					b.Fatalf("cpu_usage ratio = %v, want %v", p.F, want)
				}
			}
		}
	}

	b.ReportAllocs()
	b.ResetTimer()

	for range b.N {
		goldenRangeMatrix(b, eng, qa, cpuUsageExpr, start, end, step)
	}
}

// benchHeadSuite runs all three query shapes per op so a single pprof capture covers the whole mix —
// the convenient target to loop while optimizing the head fetch path.
func benchHeadSuite(b *testing.B) {
	s := headCPUStore(b)
	defer func() { _ = s.Close(context.Background()) }()

	eng, qa, _ := goldenPromQL(s)
	ts := headQueryTs()
	start := time.Unix(0, goldenStartTs+int64(time.Minute))
	end := time.Unix(0, goldenStartTs+int64(headPoints-1)*goldenInterval)
	step := time.Minute

	b.ReportAllocs()
	b.ResetTimer()

	for range b.N {
		goldenInstantScalar(b, eng, qa, countCPUCoresExpr, ts)
		goldenInstantScalar(b, eng, qa, fullScanCountExpr, ts)
		goldenRangeMatrix(b, eng, qa, cpuUsageExpr, start, end, step)
	}
}

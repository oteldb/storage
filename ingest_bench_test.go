package storage

import (
	"context"
	"testing"
)

// Ingest-path profiling benchmark — the write-side analogue of headquery_bench_test.go's read set.
//
// The live /src/oteldb/benchmark profile shows oteldb's at-rest CPU is 100% ingest (vmagent
// remote-writes 2560 node_exporter series every 15s), and the top live-heap holder is
// slices.Clone[uint8] — the per-series label/attribute byte clones taken on the head append path.
// This benchmark drives that exact path with the node_exporter-shaped headCorpus (2560 series,
// 4 labels each: job/instance on the resource, cpu/mode on the points) so the clone + projection +
// head-append costs show up cleanly under pprof, isolated from the docker harness:
//
//	go test -run=^$ -bench=^BenchmarkHeadIngest$ -benchtime=3s \
//	    -cpuprofile=/tmp/cpu.out -memprofile=/tmp/mem.out .
//	go tool pprof -top /tmp/cpu.out
//	go tool pprof -top -sample_index=alloc_space /tmp/mem.out
//
// It periodically Resets the head so per-series buffers don't grow without bound; the first write
// after each reset registers the series (label clone), the rest re-append to known series — the
// steady-state live mix.
func BenchmarkHeadIngest(b *testing.B) {
	ctx := context.Background()

	md := headCorpus()
	total := headSeries * headPoints

	// ~1M buffered points between resets: amortizes one-time series registration over many appends
	// while keeping the head's resident sample buffers modest.
	resetEvery := max((1<<20)/total, 1)

	s, err := InMemory()
	if err != nil {
		b.Fatal(err)
	}
	defer func() { _ = s.Close(ctx) }()

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

	goldenReportPoints(b, total)
}

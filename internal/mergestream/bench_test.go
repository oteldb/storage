package mergestream_test

import (
	"testing"

	"github.com/oteldb/storage/internal/mergestream"
	"github.com/oteldb/storage/internal/mergestream/mergestreamtest"
	"github.com/oteldb/storage/signal"
)

const benchSources, benchPerSource = 8, 1 << 14

func BenchmarkKeys(b *testing.B) {
	src := ascendingSources(benchSources, benchPerSource)
	sources := sourcesOf(src)

	var (
		k    mergestream.Keys
		last signal.SeriesID
	)

	b.ReportAllocs()
	b.SetBytes(int64(benchSources * benchPerSource * 16))
	b.ResetTimer()

	for b.Loop() {
		k.Reset(sources)
		for k.Next() {
			last = k.Key()
		}
	}

	_ = last
}

// BenchmarkUnionNaive is the map-then-sort form the engines used to build per merge, kept as the
// baseline [BenchmarkKeys] is measured against.
func BenchmarkUnionNaive(b *testing.B) {
	src := ascendingSources(benchSources, benchPerSource)

	b.ReportAllocs()
	b.SetBytes(int64(benchSources * benchPerSource * 16))
	b.ResetTimer()

	for b.Loop() {
		_ = mergestreamtest.Union(src)
	}
}

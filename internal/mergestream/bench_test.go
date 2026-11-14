package mergestream_test

import (
	"testing"

	"github.com/oteldb/storage/internal/mergestream"
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

package seriesindex

import (
	"io"
	"slices"
	"strconv"
	"testing"

	"github.com/oteldb/storage/signal"
)

func BenchmarkBuilderWritePrepared(b *testing.B) {
	const count = 4096
	input := make([]signal.Series, count)
	for i := range input {
		input[i] = signal.Series{Attributes: signal.NewAttributes(
			signal.KeyValue{Key: []byte("__name__"), Value: signal.StringValue([]byte("node_cpu_seconds_total"))},
			signal.KeyValue{Key: []byte("instance"), Value: signal.StringValue([]byte(strconv.Itoa(i)))},
			signal.KeyValue{Key: []byte("cpu"), Value: signal.StringValue([]byte("0"))},
			signal.KeyValue{Key: []byte("mode"), Value: signal.StringValue([]byte("idle"))},
		)}
	}
	slices.SortFunc(input, func(a, c signal.Series) int { return a.Hash().Compare(c.Hash()) })
	var builder Builder
	for i := range input {
		if err := builder.Add(input[i].Hash(), input[i], Metadata{Covered: 100, HasStats: true, Stats: Stats{Count: 16}}); err != nil {
			b.Fatal(err)
		}
	}
	b.ReportAllocs()
	for b.Loop() {
		if err := builder.Write(io.Discard); err != nil {
			b.Fatal(err)
		}
	}
}

package metric

import (
	"strconv"
	"testing"

	"github.com/oteldb/storage/signal"
)

func BenchmarkSeriesStaging(b *testing.B) {
	for _, n := range []int{4096, 32768} {
		ids := make([]Identity, n)
		output := make([]signal.Series, n)
		for i := range ids {
			ids[i] = Identity{Name: []byte("node_cpu_seconds_total"), Series: signal.Series{Attributes: signal.NewAttributes(kv("instance", str(strconv.Itoa(i))), kv("cpu", str("0")), kv("mode", str("idle")), kv("job", str("node-exporter")))}}
		}
		for _, mode := range []string{"individual", "arena"} {
			b.Run(strconv.Itoa(n)+"/"+mode, func(b *testing.B) {
				var arena SeriesArena
				b.ReportAllocs()
				for b.Loop() {
					for i := range ids {
						if mode == "arena" {
							output[i] = arena.Materialize(ids[i])
						} else {
							output[i] = ids[i].ToSeries()
						}
					}
					projectedSeriesSink = output[n-1].Hash()
					arena.Reset()
				}
			})
		}
	}
}

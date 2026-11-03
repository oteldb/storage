package engine_test

import (
	"math"
	"strconv"
	"testing"

	backendfile "github.com/oteldb/storage/backend/file"
	"github.com/oteldb/storage/engine"
	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/signal"
)

func BenchmarkMetricFloatGroups(b *testing.B) {
	for _, merged := range []bool{false, true} {
		for _, grouped := range []bool{false, true} {
			layout := "legacy"
			if grouped {
				layout = "grouped"
			}
			state := "flushed"
			if merged {
				state = "merged"
			}
			b.Run(state+"/"+layout, func(b *testing.B) {
				files, err := backendfile.New(b.TempDir())
				if err != nil {
					b.Fatal(err)
				}
				e := engine.New(engine.Config{Backend: files, Prefix: "bench/metrics", MetricFloatGroups: grouped, DecodeCacheBytes: 256 << 10})
				series := make([]signal.Series, 256)
				for i := range series {
					series[i] = mkSeries("instance", "host-"+strconv.Itoa(i))
				}
				for part := range 4 {
					for i := range series {
						for point := range 60 {
							value := float64(i)
							switch i % 3 {
							case 1:
								value = 1e9 + float64(point*17)
							case 2:
								value = math.Sin(float64(point%20)) * 1e-3
							}
							if _, err := e.Append(series[i], int64(part*60+point+1)*15e9, value); err != nil {
								b.Fatal(err)
							}
						}
					}
					if err := e.Flush(b.Context()); err != nil {
						b.Fatal(err)
					}
				}
				if merged {
					if err := e.MergeWith(b.Context(), engine.MergeOptions{Force: true}); err != nil {
						b.Fatal(err)
					}
				}
				parts, err := e.PartsDetailed(b.Context())
				if err != nil {
					b.Fatal(err)
				}
				var stored, rows int64
				for _, p := range parts {
					stored += p.Bytes
					rows += p.Rows
				}
				for _, window := range []struct {
					name       string
					start, end int64
				}{{"narrow", 3500e9, 3600e9}, {"hour", 0, 3600e9}} {
					for _, random := range []bool{false, true} {
						mode := "cached"
						if random {
							mode = "rotating"
						}
						b.Run(window.name+"/"+mode, func(b *testing.B) {
							b.ReportAllocs()
							i := 0
							for b.Loop() {
								host := 17
								if random {
									host = i * 31 % len(series)
								}
								i++
								request := fetch.Request{Start: window.start, End: window.end, Matchers: []fetch.Matcher{eqMatcher("instance", "host-"+strconv.Itoa(host))}}
								it, err := e.Fetch(b.Context(), request)
								if err != nil {
									b.Fatal(err)
								}
								got, err := fetch.Drain(b.Context(), it)
								if err != nil {
									b.Fatal(err)
								}
								if len(got) != 1 {
									b.Fatalf("got %d series", len(got))
								}
							}
							b.ReportMetric(float64(stored)/float64(rows), "stored-B/point")
						})
					}
				}
			})
		}
	}
}

package cluster_test

import (
	"encoding/hex"
	"fmt"
	"math/rand/v2"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/cluster"
	"github.com/oteldb/storage/encoding/compress"
	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/signal"
)

func benchLogBatches(streams, perStream int) []*fetch.Batch {
	rnd := rand.New(rand.NewPCG(1, 2))
	svcs := []string{"checkout", "payments", "frontend", "cartservice", "adservice", "shipping"}
	verbs := []string{"GET", "POST", "PUT", "DELETE"}
	paths := []string{"/api/v1/cart", "/api/v1/checkout", "/healthz", "/api/v1/products/list", "/metrics"}
	msgs := []string{
		"request completed",
		"upstream connection reset, retrying",
		"cache miss for key",
		"failed to resolve product catalog entry",
		"span exported to collector",
	}

	out := make([]*fetch.Batch, 0, streams)

	for s := range streams {
		series := signal.Series{Resource: signal.Resource{Attributes: signal.NewAttributes(
			signal.KeyValue{Key: []byte("service.name"), Value: signal.StringValue([]byte(svcs[s%len(svcs)]))},
			signal.KeyValue{Key: []byte("k8s.pod.name"), Value: signal.StringValue(fmt.Appendf(nil, "%s-7d9f%03d", svcs[s%len(svcs)], s))},
			signal.KeyValue{Key: []byte("deployment.environment"), Value: signal.StringValue([]byte("production"))},
		)}}

		ts := make([]int64, perStream)
		body := make([][]byte, perStream)
		trace := make([][]byte, perStream)
		target := make([][]byte, perStream)
		sev := make([]int64, perStream)
		status := make([]int64, perStream)

		t0 := int64(1_700_000_000_000_000_000)

		for i := range perStream {
			t0 += int64(rnd.IntN(2_000_000)) + 100_000
			ts[i] = t0

			var tid [16]byte
			for j := range tid {
				tid[j] = byte(rnd.UintN(256))
			}

			trace[i] = []byte(hex.EncodeToString(tid[:]))

			p := paths[rnd.IntN(len(paths))]
			target[i] = []byte(p)
			body[i] = fmt.Appendf(nil, "%s %s %s status=%d duration_ms=%d trace_id=%s",
				verbs[rnd.IntN(len(verbs))], p, msgs[rnd.IntN(len(msgs))],
				200+rnd.IntN(4)*100, rnd.IntN(5000), trace[i])
			sev[i] = int64(9 + rnd.IntN(4)*4)
			status[i] = int64(200 + rnd.IntN(4)*100)
		}

		out = append(out, &fetch.Batch{ID: series.Hash(), Series: series, Timestamps: ts, Columns: []fetch.NamedColumn{
			fetch.BytesColumn("body", body),
			fetch.BytesColumn("trace_id", trace),
			fetch.BytesColumn("http.target", target),
			fetch.Int64Column("severity", sev),
			fetch.Int64Column("http.status_code", status),
		}})
	}

	return out
}

// benchMetricBatches builds a metric fan-out payload. gauge picks the value shape: integral
// counter-like values (bucket counts, the compressible end) or full-entropy fractional gauges
// (the incompressible end); a real payload sits between the two.
func benchMetricBatches(series, perSeries int, gauge bool) []*fetch.Batch {
	rnd := rand.New(rand.NewPCG(3, 4))

	out := make([]*fetch.Batch, 0, series)

	for s := range series {
		ser := signal.Series{Attributes: signal.NewAttributes(
			signal.KeyValue{Key: []byte("__name__"), Value: signal.StringValue([]byte("http_server_duration_seconds_bucket"))},
			signal.KeyValue{Key: []byte("job"), Value: signal.StringValue(fmt.Appendf(nil, "api-%d", s%16))},
			signal.KeyValue{Key: []byte("instance"), Value: signal.StringValue(fmt.Appendf(nil, "10.4.%d.%d:9090", s/256, s%256))},
		)}

		ts := make([]int64, perSeries)
		vs := make([]float64, perSeries)
		t0 := int64(1_700_000_000_000)
		v := float64(rnd.IntN(1000))

		for i := range perSeries {
			t0 += 15_000

			if gauge {
				v += rnd.Float64()*20 - 10
			} else {
				v += float64(rnd.IntN(10))
			}

			ts[i], vs[i] = t0, v
		}

		out = append(out, &fetch.Batch{ID: ser.Hash(), Series: ser, Timestamps: ts, Values: vs})
	}

	return out
}

func BenchmarkWireCompress(b *testing.B) {
	logs, err := cluster.EncodeLogBatches(benchLogBatches(200, 500))
	require.NoError(b, err)

	for _, lvl := range []struct {
		name  string
		level compress.Level
	}{{"fast", compress.LevelFast}, {"default", compress.LevelDefault}} {
		b.Run(lvl.name, func(b *testing.B) {
			benchWirePayloads(b, compress.NewCompressor(compress.AlgorithmZSTD, lvl.level), logs)
		})
	}
}

func benchWirePayloads(b *testing.B, comp *compress.Compressor, logs []byte) {
	b.Helper()

	for _, tc := range []struct {
		name string
		raw  []byte
	}{
		{"logs", logs},
		{"metrics_counters", cluster.EncodeBatches(benchMetricBatches(2000, 120, false))},
		{"metrics_gauges", cluster.EncodeBatches(benchMetricBatches(2000, 120, true))},
	} {
		enc := comp.Compress(nil, tc.raw)
		b.Logf("%s: raw=%d compressed=%d ratio=%.2fx", tc.name, len(tc.raw), len(enc), float64(len(tc.raw))/float64(len(enc)))

		b.Run(tc.name+"/compress", func(b *testing.B) {
			b.SetBytes(int64(len(tc.raw)))
			b.ReportAllocs()

			var dst []byte
			for b.Loop() {
				dst = comp.Compress(dst[:0], tc.raw)
			}
		})

		b.Run(tc.name+"/decompress", func(b *testing.B) {
			b.SetBytes(int64(len(tc.raw))) // logical, uncompressed size on both sides
			b.ReportAllocs()

			var dst []byte
			for b.Loop() {
				var derr error
				if dst, derr = comp.Decompress(dst[:0], enc); derr != nil {
					b.Fatal(derr)
				}
			}
		})
	}
}

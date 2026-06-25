package storage

import (
	"context"
	"strconv"
	"testing"

	"github.com/oteldb/storage/signal"
	"github.com/oteldb/storage/signal/metric"
)

// benchBatch builds one realistic gauge metric ("http.requests") whose data points span
// seriesCount distinct series (one per `route` value), each carrying pointsPerSeries
// samples. Total points = seriesCount * pointsPerSeries. Built once, outside the timed
// loop, and reused read-only across iterations.
func benchBatch(seriesCount, pointsPerSeries int) metric.Metrics {
	var md metric.Metrics
	rm := md.AddResource()
	rm.Resource = signal.Resource{
		Attributes: signal.NewAttributes(signal.KeyValue{
			Key: []byte("service.name"), Value: signal.StringValue([]byte("bench")),
		}),
	}
	sm := rm.AddScope()
	sm.Scope = signal.Scope{Name: []byte("benchlib")}
	mt := sm.AddMetric()
	mt.Name = []byte("http.requests")
	mt.Unit = []byte("1")
	mt.Kind = metric.KindGauge

	for s := range seriesCount {
		attrs := signal.NewAttributes(signal.KeyValue{
			Key: []byte("route"), Value: signal.StringValue([]byte("/route/" + strconv.Itoa(s))),
		})
		for p := range pointsPerSeries {
			pt := mt.AddPoint()
			pt.Ts = 1_000_000_000 + int64(p)*15_000 // 15ms stride
			pt.Value = float64(p)
			pt.Attributes = attrs
		}
	}

	return md
}

// BenchmarkWriteMetrics measures the end-to-end point-ingestion throughput of the storage
// facade — tenant routing, projection, identity hashing, index registration, and head
// append — across a few cardinality/depth shapes. Throughput (via b.SetBytes) is reported
// in logical sample bytes: 16 per point (an int64 timestamp + a float64 value), sized by
// the uncompressed data so it is a real ingest speed.
func BenchmarkWriteMetrics(b *testing.B) {
	shapes := []struct {
		name           string
		series, points int
	}{
		{"1series_1000points", 1, 1000},
		{"100series_100points", 100, 100},
		{"1000series_10points", 1000, 10},
		{"10000series_1point", 10000, 1},
	}

	for _, sh := range shapes {
		b.Run(sh.name, func(b *testing.B) {
			benchmarkWriteMetrics(b, sh.series, sh.points)
		})
	}
}

func benchmarkWriteMetrics(b *testing.B, seriesCount, pointsPerSeries int) {
	b.Helper()

	ctx := context.Background()
	md := benchBatch(seriesCount, pointsPerSeries)
	total := seriesCount * pointsPerSeries

	// Rebuild the store periodically so the head's per-series buffers don't grow without
	// bound across b.N. ~1M buffered points keeps resident memory modest (~16 MiB of
	// samples) while amortizing the one-time series-registration cost over many appends.
	resetEvery := max((1<<20)/total, 1)

	s, err := InMemory()
	if err != nil {
		b.Fatal(err)
	}

	b.SetBytes(int64(total) * 16) // logical (timestamp, value) bytes ingested/sec
	b.ReportAllocs()
	b.ResetTimer()

	for i := range b.N {
		if _, err := s.WriteMetrics(ctx, md); err != nil {
			b.Fatal(err)
		}

		if (i+1)%resetEvery == 0 {
			// Drop the store (and its in-memory backend) for a fresh head; the old one is
			// unreferenced and reclaimed. No Close: that would flush samples into the
			// backend and accumulate parts across resets. Excluded from the timer.
			b.StopTimer()
			s, err = InMemory()
			if err != nil {
				b.Fatal(err)
			}
			b.StartTimer()
		}
	}
}

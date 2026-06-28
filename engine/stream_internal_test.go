package engine

import (
	"context"
	"runtime"
	"strconv"
	"testing"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/signal"
)

// BenchmarkDecodeResident measures the peak in-use heap of decoding one part's columns two ways:
// the one-shot full decode (the pre-streaming merge path, which held the whole decoded column
// resident) vs the streaming path (one series range at a time into reused scratch). This is the
// streaming k-way merge's resident-memory win per source part (issue #25, item 1), measured as peak
// HeapInuse — invisible to alloc-byte benchmarks because it is residency, not cumulative allocation.
func BenchmarkDecodeResident(b *testing.B) {
	const series, samples = 500, 400

	ctx := context.Background()
	e := New(Config{Backend: backend.Memory(), Prefix: "x", MaxPartBytes: 0})

	sids := make([]signal.SeriesID, series)
	ser := make([]signal.Series, series)
	for i := range series {
		ser[i] = signal.Series{Attributes: signal.NewAttributes(
			signal.KeyValue{Key: []byte("h"), Value: signal.StringValue([]byte(strconv.Itoa(i)))},
		)}
		sids[i] = ser[i].Hash()
	}

	n := series * samples
	batchIDs := make([]signal.SeriesID, n)
	ts := make([]int64, n)
	vals := make([]float64, n)

	k := 0
	for i := range series {
		for s := range samples {
			batchIDs[k] = sids[i]
			ts[k] = int64(s) * 15
			vals[k] = float64(i)
			k++
		}
	}

	if _, err := e.AppendBatch(batchIDs, ts, vals, nil, func(i int) signal.Series { return ser[i/samples] }, AppendLimits{}); err != nil {
		b.Fatal(err)
	}

	if err := e.Flush(ctx); err != nil {
		b.Fatal(err)
	}

	p := e.parts[0]

	b.Run("full", func(b *testing.B) {
		b.ReportAllocs()

		var peak uint64

		for range b.N {
			runtime.GC()

			var before runtime.MemStats
			runtime.ReadMemStats(&before)

			d, err := p.decode(ctx)
			if err != nil {
				b.Fatal(err)
			}

			var after runtime.MemStats
			runtime.ReadMemStats(&after)

			decodeResidentSink ^= d.ts[0] // keep the decoded columns resident through the measurement

			if delta := after.HeapInuse - before.HeapInuse; delta > peak {
				peak = delta
			}
		}

		b.ReportMetric(float64(peak)/(1<<20), "peakMB")
	})

	b.Run("stream", func(b *testing.B) {
		b.ReportAllocs()

		var peak uint64

		for range b.N {
			runtime.GC()

			var before runtime.MemStats
			runtime.ReadMemStats(&before)

			s, err := newPartStream(ctx, p)
			if err != nil {
				b.Fatal(err)
			}

			var dst rangeBuf

			for _, id := range p.index.ids {
				rng, ok := p.index.lookup(id)
				if !ok {
					continue
				}

				tsv, _, _, err := s.decodeRange(rng, &dst)
				if err != nil {
					b.Fatal(err)
				}

				if len(tsv) > 0 {
					decodeResidentSink ^= tsv[0]
				}
			}

			var after runtime.MemStats
			runtime.ReadMemStats(&after)

			if delta := after.HeapInuse - before.HeapInuse; delta > peak {
				peak = delta
			}
		}

		b.ReportMetric(float64(peak)/(1<<20), "peakMB")
	})
}

// decodeResidentSink prevents the decoder from being dead-code-eliminated in the resident-memory
// benchmark (the values are read after the HeapInuse measurement so the decode stays resident).
var decodeResidentSink int64

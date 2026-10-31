package engine

import (
	"strconv"
	"testing"

	"github.com/oteldb/storage/encoding/chunk"
	"github.com/oteldb/storage/signal"
)

func BenchmarkPlanIndexReuse(b *testing.B) {
	for _, count := range []int{1 << 16, 1 << 20} {
		b.Run(strconv.Itoa(count), func(b *testing.B) {
			col := make([]chunk.U128, count)
			for i := range col {
				col[i] = chunk.U128{Hi: uint64(i+1) * 2, Lo: uint64(i)}
			}
			entries, n, total, err := parseSeriesIndex(encodeSeriesIndex(col), true)
			if err != nil {
				b.Fatal(err)
			}
			index := &pagedIndex{n: n, total: total}
			index.view.Store(&entries)
			ids := make([]signal.SeriesID, 512)
			for i := range ids {
				ids[i] = signal.SeriesID(col[i*count/len(ids)])
				if i%16 != 0 {
					ids[i].Hi++
				}
			}
			parts := make([]*part, 16)
			readers := make(map[*part]*seriesBlockReader, len(parts))
			for i := range parts {
				parts[i] = &part{index: partIndex{paged: index}}
				readers[parts[i]] = &seriesBlockReader{part: parts[i], blockRows: 1024}
			}
			ctx := b.Context()
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				plan := enginePlan{ids: ids, liveParts: parts, blockReaders: readers}
				if _, err := plan.decodeEstimate(ctx, colNeed{values: true}); err != nil {
					b.Fatal(err)
				}
				found := 0
				for _, pt := range parts {
					for _, id := range ids {
						_, ok, err := plan.lookupRange(ctx, pt, id)
						if err != nil {
							b.Fatal(err)
						}
						if ok {
							found++
						}
					}
				}
				if found != len(parts)*32 {
					b.Fatalf("matched=%d", found)
				}
			}
		})
	}
}

package engine

import (
	"strconv"
	"testing"

	"github.com/oteldb/storage/encoding/chunk"
	"github.com/oteldb/storage/signal"
)

func BenchmarkResidentIndexLookup(b *testing.B) {
	for _, count := range []int{4096, 1 << 16, 1 << 20} {
		b.Run(strconv.Itoa(count), func(b *testing.B) {
			ids := make([]chunk.U128, count)
			for i := range ids {
				ids[i] = chunk.U128{Hi: uint64(i+1) << 40, Lo: uint64(i) * 0x9e3779b97f4a7c15}
			}
			entries, n, total, err := parseSeriesIndex(encodeSeriesIndex(ids), true)
			if err != nil {
				b.Fatal(err)
			}
			p := pagedIndex{n: n, total: total, fences: make([]signal.SeriesID, (n+indexPageEntries-1)/indexPageEntries)}
			for i := range p.fences {
				p.fences[i] = sidxEntryID(entries, i*indexPageEntries)
			}
			p.view.Store(&entries)
			state := uint64(1)
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				state = state*6364136223846793005 + 1442695040888963407
				k := int(state>>32) & (count - 1)
				id := signal.SeriesID(ids[k])
				if state&1 != 0 {
					id.Hi++
				}
				r, found, err := p.lookup(b.Context(), id)
				if err != nil || found != (state&1 == 0) || found && r != (rowRange{start: k, end: k + 1}) {
					b.Fatalf("lookup=%v,%v,%v for index %d", r, found, err, k)
				}
			}
		})
	}
}

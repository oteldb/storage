package engine

import (
	"strconv"
	"testing"

	"github.com/oteldb/storage/encoding/chunk"
	"github.com/oteldb/storage/internal/mergestream"
	"github.com/oteldb/storage/signal"
)

func BenchmarkMergeSeriesRanges(b *testing.B) {
	const count, inputs = 32768, 8
	for _, overlap := range []bool{false, true} {
		for _, paged := range []bool{false, true} {
			b.Run("overlap="+strconv.FormatBool(overlap)+"/paged="+strconv.FormatBool(paged), func(b *testing.B) {
				parts := make([]*part, inputs)
				sources := make([]mergestream.Source, inputs)
				for i := range parts {
					ids := make([]signal.SeriesID, count)
					starts := make([]int32, count+1)
					runs := make([]chunk.U128Run, count)
					for j := range ids {
						id := uint64(j*inputs + i)
						if overlap {
							id = uint64(j)
						}
						ids[j], starts[j] = signal.SeriesID{Lo: id}, int32(j*3)
						runs[j] = chunk.U128Run{Value: chunk.U128(ids[j]), Count: 3}
					}
					starts[count] = count * 3
					idx := partIndex{ids: ids, starts: starts}
					if paged {
						entries, n, total, err := parseSeriesIndex(encodeSeriesIndexRuns(runs), true)
						if err != nil {
							b.Fatal(err)
						}
						p := &pagedIndex{n: n, total: total}
						p.view.Store(&entries)
						idx = partIndex{paged: p}
					}
					parts[i] = &part{index: idx}
					source, err := idx.idSource(b.Context())
					if err != nil {
						b.Fatal(err)
					}
					sources[i] = source
				}
				for _, mode := range []string{"lookup", "cursor"} {
					b.Run(mode, func(b *testing.B) {
						ctx := b.Context()
						var plain mergestream.Keys
						var cursor seriesMerge
						if err := cursor.init(b.Context(), parts); err != nil {
							b.Fatal(err)
						}
						b.ReportAllocs()
						b.SetBytes(count * inputs * 3 * SampleBytes)
						for b.Loop() {
							rows := 0
							if mode == "lookup" {
								plain.Reset(sources)
								for plain.Next() {
									for _, p := range parts {
										rng, ok, err := p.index.lookup(ctx, plain.Key())
										if err != nil {
											b.Fatal(err)
										}
										if ok {
											rows += rng.end - rng.start
										}
									}
								}
							} else {
								cursor.keys.Reset(sources)
								for cursor.Next() {
									for _, r := range cursor.ranges {
										rows += r.end - r.start
									}
								}
							}
							if rows != count*inputs*3 {
								b.Fatal("incomplete traversal", rows)
							}
						}
					})
				}
			})
		}
	}
}

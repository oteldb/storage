package fetch_test

import (
	"context"
	"strconv"
	"testing"

	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/signal"
)

// BenchmarkMergeFanOutWidth isolates the merge's own bookkeeping: children are in-memory slice
// iterators (no I/O at all) holding a fixed total of disjoint, id-sorted series, split across k
// children. Emitted batches are constant, so ns/batch moves only with the merge's per-emit
// ordering cost — which is why the ordering is a heap and not a scan of the children.
func BenchmarkMergeFanOutWidth(b *testing.B) {
	ctx := context.Background()

	const total = 8192

	for _, k := range []int{2, 8, 32, 128, 512} {
		b.Run("children="+strconv.Itoa(k), func(b *testing.B) {
			per := total / k
			groups := make([][]*fetch.Batch, k)

			for c := range groups {
				g := make([]*fetch.Batch, 0, per)
				for i := range per { // interleave so every child stays live to the end
					id := uint64(i*k + c)
					g = append(g, &fetch.Batch{
						ID:         signal.SeriesID{Lo: id},
						Timestamps: []int64{int64(id)},
						Values:     []float64{float64(id)},
					})
				}

				groups[c] = g
			}

			fetchers := make([]fetch.Fetcher, k)
			for c := range fetchers {
				fetchers[c] = sliceFetcher{groups[c]}
			}

			m := fetch.Merge(fetchers...)

			b.ReportAllocs()
			b.ResetTimer()

			for range b.N {
				it, err := m.Fetch(ctx, fetch.Request{})
				if err != nil {
					b.Fatal(err)
				}

				n := 0

				for {
					if _, err := it.Next(ctx); err != nil {
						break
					}

					n++
				}

				if err := it.Close(); err != nil {
					b.Fatal(err)
				}

				if n != total {
					b.Fatalf("got %d batches, want %d", n, total)
				}
			}

			b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*total), "ns/batch")
		})
	}
}

// sliceFetcher re-serves the same batches on every Fetch (the merge never mutates single-source
// batches, so replaying them across iterations is sound).
type sliceFetcher struct{ batches []*fetch.Batch }

func (f sliceFetcher) Fetch(context.Context, fetch.Request) (fetch.Iterator, error) {
	return fetch.NewSliceIterator(f.batches), nil
}

package storage

import (
	"context"
	"fmt"
	"math/rand/v2"
	"slices"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/signal"
	"github.com/oteldb/storage/signal/profile"
)

// symbolWorkload is a producer that resends its whole dictionary on every scrape, the way a pprof
// exporter does: every flushed part then carries the full symbol working set.
type symbolWorkload struct {
	functions, locations, stacks, depth int
}

func (w symbolWorkload) batch(service string, ts int64, seed uint64) profile.Profiles {
	var pd profile.Profiles

	d := &pd.Dictionary
	rng := rand.New(rand.NewPCG(seed, seed))

	fns := make([]int32, w.functions)
	for i := range fns {
		fns[i] = d.AddFunction(profile.Function{
			NameStrindex:     d.InternString(fmt.Appendf(nil, "%s.pkg.Function%d", service, i)),
			FilenameStrindex: d.InternString(fmt.Appendf(nil, "%s/pkg/file%d.go", service, i%64)),
		})
	}

	locs := make([]int32, w.locations)
	for i := range locs {
		locs[i] = d.AddLocation(profile.Location{
			Address: uint64(i) * 16,
			Lines:   []profile.Line{{FunctionIndex: fns[rng.IntN(len(fns))], Line: int64(i)}},
		})
	}

	rp := pd.AddResource()
	rp.Resource = signal.Resource{Attributes: signal.NewAttributes(
		signal.KeyValue{Key: []byte("service.name"), Value: signal.StringValue([]byte(service))},
	)}
	pr := rp.AddScope().AddProfile()
	pr.SampleType = profile.ValueType{TypeStrindex: d.InternString([]byte("cpu")), UnitStrindex: d.InternString([]byte("nanoseconds"))}
	pr.TimeNanos = ts

	for range w.stacks {
		frames := make([]int32, w.depth)
		for j := range frames {
			frames[j] = locs[rng.IntN(len(locs))]
		}

		sm := pr.AddSample()
		sm.StackIndex, sm.Values = d.AddStack(frames...), []int64{int64(rng.IntN(1000) + 1)}
	}

	return pd
}

// writeParts writes one scrape per part and flushes it, so the store holds n parts that each repeat
// the working set. Part i holds samples at ts i+1.
func (w symbolWorkload) writeParts(tb testing.TB, s *Storage, n int) {
	tb.Helper()

	ctx := context.Background()

	for i := range n {
		_, err := s.WriteProfiles(ctx, w.batch("api", int64(i+1), 1))
		require.NoError(tb, err)
		require.NoError(tb, mustEngine(s.profileEngineFor("default")).Flush(ctx))
	}
}

// profileStackIDs fetches the distinct stack ids the tenant's samples in [start, end] reference.
func profileStackIDs(tb testing.TB, s *Storage, start, end int64) [][]byte {
	tb.Helper()

	batches := mustDrainTB(tb, s.ProfileFetcher("default"), fetch.Request{
		Signal: signal.Profile, Start: start, End: end, Projection: []string{profile.ColStackID},
	})

	seen := map[string]bool{}

	var ids [][]byte

	for _, b := range batches {
		col, _ := b.Column(profile.ColStackID)
		for _, id := range col.Bytes {
			if !seen[string(id)] {
				seen[string(id)] = true
				ids = append(ids, slices.Clone(id))
			}
		}
	}

	return ids
}

func mustDrainTB(tb testing.TB, f fetch.Fetcher, r fetch.Request) []*fetch.Batch {
	tb.Helper()

	out, err := fetch.Drain(context.Background(), must(f.Fetch(context.Background(), r)))
	require.NoError(tb, err)

	return out
}

// BenchmarkProfileResolver is repeated flamegraph queries over a store of many flushed parts, each
// carrying the whole symbol working set. Build is the resolver alone; BuildResolve also resolves
// every stack the samples reference; Newest is a window holding only the latest part. Uncached turns
// the symbol cache off: the cost of a query whose parts are not resident.
func BenchmarkProfileResolver(b *testing.B) {
	w := symbolWorkload{functions: 500, locations: 2000, stacks: 2000, depth: 24}
	ctx := context.Background()

	for _, parts := range []int{8, 32} {
		for _, cache := range []struct {
			name  string
			bytes int64
		}{{"", 0}, {"Uncached/", -1}} {
			s, err := InMemory(WithProfileSymbolCache(cache.bytes))
			require.NoError(b, err)
			b.Cleanup(func() { _ = s.Close(ctx) })

			w.writeParts(b, s, parts)
			stacks := profileStackIDs(b, s, 0, 1<<60)
			newest := int64(parts)

			for _, bc := range []struct {
				name       string
				start, end int64
				resolve    bool
			}{
				{"All/Build", 0, 0, false},
				{"All/BuildResolve", 0, 0, true},
				{"Newest/Build", newest, newest, false},
			} {
				b.Run(fmt.Sprintf("parts=%d/%s%s", parts, cache.name, bc.name), func(b *testing.B) {
					_, err := s.ProfileResolver(ctx, "default", bc.start, bc.end)
					require.NoError(b, err)

					b.ReportAllocs()

					for b.Loop() {
						r, err := s.ProfileResolver(ctx, "default", bc.start, bc.end)
						if err != nil {
							b.Fatal(err)
						}

						if !bc.resolve {
							continue
						}

						for _, id := range stacks {
							if len(r.Resolve(id)) == 0 {
								b.Fatal("unresolved stack")
							}
						}
					}
				})
			}
		}
	}
}

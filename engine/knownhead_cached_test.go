package engine

import (
	"bytes"
	"context"
	"fmt"
	"slices"
	"testing"

	"github.com/go-faster/errors"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/backend/bucketindex"
	backendfile "github.com/oteldb/storage/backend/file"
	"github.com/oteldb/storage/index/series"
	"github.com/oteldb/storage/internal/seriestable"
)

type knownProofFixture struct {
	view      series.View
	refs      []uint32
	turn      int
	snapshot  *seriesSnapshot
	batch     *walBatch
	positions []int
	files     *cpuFixtureFile
	cached    backend.Backend
	ids       *seriestable.IDCache
}

func newKnownProofFixture(tb testing.TB, perRoot int, cacheBytes int64) *knownProofFixture {
	tb.Helper()
	files, err := backendfile.New(tb.TempDir())
	if err != nil {
		tb.Fatal(err)
	}
	counter := &cpuFixtureFile{File: files}
	cached := backend.Cached(counter, cacheBytes)
	ids := seriestable.NewIDCache(4 << 10)
	registry := series.New()
	refs := make([]uint32, 3*perRoot)
	for i := range refs {
		id := registry.Add(cpuMetricSeries(i))
		refs[i], _ = registry.Ref(id)
	}
	view := registry.View()
	slices.SortFunc(refs, func(i, j uint32) int { return view.ID(int(i)).Compare(view.ID(int(j))) })
	roots := make([]*seriesRootHandle, 0, 3)
	for root := range 3 {
		var builder seriestable.Builder
		for i := root; i < len(refs); i += 3 {
			if err := builder.AddView(view, int(refs[i])); err != nil {
				tb.Fatal(err)
			}
		}
		prefix := fmt.Sprintf("proof-%d", root)
		run, err := seriestable.WriteWithIDCache(tb.Context(), cached, prefix, &builder, ids)
		if err != nil {
			tb.Fatal(err)
		}
		roots = append(roots, &seriesRootHandle{binding: bucketindex.SeriesRoot{Prefix: prefix, ManifestDigest: run.Digest()}, run: run})
	}
	snapshot := newSeriesSnapshot(roots)
	tb.Cleanup(snapshot.release)
	batch := newWALBatch()
	tb.Cleanup(batch.reset)
	const probes = 256
	positions := make([]int, probes)
	for i := range positions {
		ref := refs[(i*7919)%len(refs)]
		batch.order = append(batch.order, view.ID(int(ref)))
		batch.accs = append(batch.accs, walSeriesAcc{series: view.At(int(ref), nil)})
		positions[i] = i
	}
	slices.SortFunc(positions, func(i, j int) int { return batch.order[i].Compare(batch.order[j]) })
	return &knownProofFixture{view: view, refs: refs, snapshot: snapshot, batch: batch, positions: positions, files: counter, cached: cached, ids: ids}
}

func (f *knownProofFixture) rotate() {
	f.turn++
	for i := range f.positions {
		ref := f.refs[(i*7919+f.turn*4099)%len(f.refs)]
		f.batch.order[i] = f.view.ID(int(ref))
		f.batch.accs[i].series = f.view.At(int(ref), f.batch.accs[i].series.Attributes)
		f.positions[i] = i
	}
	slices.SortFunc(f.positions, func(i, j int) int { return f.batch.order[i].Compare(f.batch.order[j]) })
}

// verifyReferenceProof keeps the two unmodified page policies as benchmark controls.
func verifyReferenceProof(root *seriesRootHandle, b *walBatch, positions []int, canonical *[]byte, streamed bool) (err error) {
	r, err := root.run.NewReader(context.Background())
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, r.Close()) }()
	if streamed {
		r.StreamPages()
	}
	for _, i := range positions {
		raw, found, err := r.LookupRaw(b.order[i])
		if err != nil {
			return err
		}
		if !found {
			continue
		}
		*canonical = b.accs[i].series.AppendHashInput((*canonical)[:0])
		if !bytes.Equal(raw, *canonical) {
			return errors.New("canonical conflict")
		}
		b.accs[i].compact = true
	}
	return nil
}

func (f *knownProofFixture) verify(mode string) error {
	for i := range f.batch.accs {
		f.batch.accs[i].compact = false
	}
	pending := slices.Clone(f.positions)
	var canonical []byte
	for _, root := range f.snapshot.roots {
		var err error
		if mode != "id-pages" {
			err = verifyReferenceProof(root, f.batch, pending, &canonical, mode == "streamed")
		} else {
			err = verifyKnownRoot(root, f.batch, pending, &canonical)
		}
		if err != nil {
			return err
		}
		next := pending[:0]
		for _, i := range pending {
			if !f.batch.accs[i].compact {
				next = append(next, i)
			}
		}
		pending = next
	}
	if len(pending) != 0 {
		return errors.New("missing identity")
	}
	return nil
}

func TestKnownHeadCachedCanonicalProof(t *testing.T) {
	t.Parallel()
	const cacheBytes = 2 << 20
	f := newKnownProofFixture(t, 1024, cacheBytes)
	require.Less(t, f.ids.SizeBytes(), int64(1024*16), "no root's full ID array fits the cache")
	for range 3 {
		require.NoError(t, f.verify("id-pages"))
	}
	stats := f.cached.(interface{ Stats() backend.CacheStats }).Stats()
	require.Positive(t, stats.Hits)
	require.LessOrEqual(t, stats.Bytes, int64(cacheBytes))
	original := f.batch.accs[0].series
	f.batch.accs[0].series = benchSeries(999999)
	require.ErrorContains(t, f.verify("id-pages"), "canonical identity conflict")
	f.batch.accs[0].series = original
	require.NoError(t, f.verify("id-pages"))
}

func BenchmarkKnownHeadCanonicalPages(b *testing.B) {
	for _, cacheMiB := range []int64{2, 32} {
		b.Run(fmt.Sprintf("cache=%dMiB", cacheMiB), func(b *testing.B) {
			for _, mode := range []string{"streamed", "full-cached", "id-pages"} {
				b.Run(mode, func(b *testing.B) {
					f := newKnownProofFixture(b, 32768, cacheMiB<<20)
					for range 32 {
						f.rotate()
						if err := f.verify(mode); err != nil {
							b.Fatal(err)
						}
					}
					f.files.Reset()
					b.ReportAllocs()
					b.SetBytes(int64(len(f.positions)) * 16)
					for b.Loop() {
						b.StopTimer()
						f.rotate()
						b.StartTimer()
						if err := f.verify(mode); err != nil {
							b.Fatal(err)
						}
					}
					stats := f.cached.(interface{ Stats() backend.CacheStats }).Stats()
					b.ReportMetric(float64(f.files.Bytes())/float64(b.N), "backend-read-B/op")
					b.ReportMetric(float64(stats.Bytes), "cache-B")
					b.ReportMetric(float64(f.ids.SizeBytes()), "id-cache-B")
				})
			}
		})
	}
}

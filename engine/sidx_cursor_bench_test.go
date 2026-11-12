package engine

import (
	"testing"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/backend/file"
	"github.com/oteldb/storage/encoding/chunk"
	"github.com/oteldb/storage/internal/obs"
	"github.com/oteldb/storage/internal/readerpool"
	"github.com/oteldb/storage/signal"
)

func BenchmarkSIDXBatchPages(b *testing.B) {
	f, err := file.New(b.TempDir())
	if err != nil {
		b.Fatal(err)
	}
	const count = 8192
	col := make([]chunk.U128, count)
	for i := range col {
		col[i] = chunk.U128{Lo: uint64((i + 1) * 2)}
	}
	if err := f.Write(b.Context(), sidxKey("p"), encodeSeriesIndex(col)); err != nil {
		b.Fatal(err)
	}
	scope := readerpool.NewScope()
	scope.Acquire()
	defer scope.Release()
	defer scope.Close()
	ctx := scope.Context(b.Context())
	for _, cache := range []struct {
		name  string
		bytes int64
	}{{"none", 0}, {"tiny", 4096}, {"warm", 2 << 20}} {
		be := backend.Cached(f, cache.bytes)
		p, ok := openPagedIndex(ctx, be, "p", count, obs.NewNop().Corruption)
		if !ok {
			b.Fatal("paged index")
		}
		idx := partIndex{paged: p}
		for _, mode := range []string{"point", "cursor"} {
			b.Run(cache.name+"/"+mode, func(b *testing.B) {
				b.ReportAllocs()
				for b.Loop() {
					var cursor sidxPositionCursor
					for i := range 4096 {
						id := signal.SeriesID{Lo: uint64((i + 1) * 2)}
						var pos int
						var found bool
						var err error
						if mode == "cursor" {
							pos, found, err = cursor.position(ctx, idx, id)
						} else {
							pos, found, err = idx.position(ctx, id)
						}
						if err != nil || !found || pos != i {
							b.Fatalf("position=%d found=%v err=%v", pos, found, err)
						}
					}
				}
			})
		}
	}
}

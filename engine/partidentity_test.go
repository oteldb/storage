package engine_test

import (
	"context"
	"strconv"
	"strings"
	"testing"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/backend/faultbackend"
	"github.com/oteldb/storage/engine"
)

func identityWrite(op faultbackend.Op) bool {
	return op.Kind == faultbackend.Write && strings.HasSuffix(op.Key, "/identity")
}

// BenchmarkLoadPartsIdentity measures recovery: the resident index is rebuilt by reading one
// identity object per live part instead of a single whole-set object, which is the cost #273 trades
// for retention that cleans itself. The part count is what varies — merge bounds it, so this is the
// working range.
func BenchmarkLoadPartsIdentity(b *testing.B) {
	const seriesPerPart = 20_000

	for _, parts := range []int{4, 16} {
		b.Run(strconv.Itoa(parts)+"parts", func(b *testing.B) {
			ctx := context.Background()
			be := backend.Memory()
			cfg := engine.Config{Backend: be, Prefix: "bench/identity"}

			e := engine.New(cfg)
			for p := range parts {
				for i := range seriesPerPart {
					if _, err := e.Append(mkSeries("job", "api", "inst", strconv.Itoa(p*seriesPerPart+i)), int64(100+p), 1); err != nil {
						b.Fatal(err)
					}
				}

				if err := e.Flush(ctx); err != nil {
					b.Fatal(err)
				}
			}

			b.ReportAllocs()

			for b.Loop() {
				r := engine.New(cfg)
				if err := r.LoadParts(ctx); err != nil {
					b.Fatal(err)
				}

				if got := r.SeriesCount(); got != parts*seriesPerPart {
					b.Fatalf("loaded %d identities, want %d", got, parts*seriesPerPart)
				}
			}
		})
	}
}

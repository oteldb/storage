package file_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/backend/file"
)

// benchTree writes a deployment-shaped tree: tenants × signals × parts, each part holding a
// manifest and a column object under "c/".
func benchTree(b *testing.B, tenants, parts int) backend.Backend {
	b.Helper()
	ctx := context.Background()

	bk, err := file.New(b.TempDir())
	if err != nil {
		b.Fatal(err)
	}

	for t := range tenants {
		for _, sig := range []string{"metrics", "logs", "traces"} {
			for p := range parts {
				pfx := fmt.Sprintf("t%d/%s/%010d", t, sig, p)
				if err := bk.Write(ctx, pfx+"/manifest", []byte("m")); err != nil {
					b.Fatal(err)
				}

				if err := bk.Write(ctx, pfx+"/c/col", []byte("c")); err != nil {
					b.Fatal(err)
				}
			}
		}
	}

	return bk
}

func BenchmarkListPrefix(b *testing.B) {
	ctx := context.Background()
	bk := benchTree(b, 4, 64)

	b.ReportAllocs()

	for b.Loop() {
		keys, err := bk.List(ctx, "t0/metrics/")
		if err != nil {
			b.Fatal(err)
		}

		if len(keys) != 128 {
			b.Fatalf("got %d keys", len(keys))
		}
	}
}

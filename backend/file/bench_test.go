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

// BenchmarkWrite publishes a 4 KiB object into a directory that already exists — the shape a part's
// column objects take, and where the fsync durability costs shows up as write latency.
func BenchmarkWrite(b *testing.B) {
	ctx := context.Background()

	bk, err := file.New(b.TempDir())
	if err != nil {
		b.Fatal(err)
	}

	data := make([]byte, 4<<10)

	if err := bk.Write(ctx, "t0/metrics/0000/c/warm", data); err != nil {
		b.Fatal(err)
	}

	b.SetBytes(int64(len(data)))
	b.ReportAllocs()

	i := 0

	for b.Loop() {
		if err := bk.Write(ctx, fmt.Sprintf("t0/metrics/0000/c/col%d", i), data); err != nil {
			b.Fatal(err)
		}

		i++
	}
}

// BenchmarkPutIfAbsent measures the link publish path, whose directory sync is the same one
// [BenchmarkWrite] pays for the rename.
func BenchmarkPutIfAbsent(b *testing.B) {
	ctx := context.Background()

	bk, err := file.New(b.TempDir())
	if err != nil {
		b.Fatal(err)
	}

	data := make([]byte, 4<<10)

	if err := bk.Write(ctx, "t0/metrics/0000/c/warm", data); err != nil {
		b.Fatal(err)
	}

	b.SetBytes(int64(len(data)))
	b.ReportAllocs()

	i := 0

	for b.Loop() {
		if _, err := bk.PutIfAbsent(ctx, fmt.Sprintf("t0/metrics/0000/c/obj%d", i), data); err != nil {
			b.Fatal(err)
		}

		i++
	}
}

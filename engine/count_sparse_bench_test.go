package engine_test

import (
	"context"
	"fmt"
	"testing"
)

func BenchmarkCountSparsePart(b *testing.B) {
	for _, series := range []int{64, 1024} {
		b.Run(fmt.Sprintf("series=%d", series), func(b *testing.B) {
			e, req := sparseCountEngine(b, series, 32<<20, false)
			ctx := context.Background()
			if _, err := e.Count(ctx, req); err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			b.SetBytes(101 * 8)
			for b.Loop() {
				n, err := e.Count(ctx, req)
				if err != nil {
					b.Fatal(err)
				}
				if n != 1 {
					b.Fatalf("count=%d", n)
				}
			}
		})
	}
}

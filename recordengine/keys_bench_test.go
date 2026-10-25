package recordengine_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/recordengine"
)

func BenchmarkKeysWarmParallel(b *testing.B) {
	e := recordengine.New(recordengine.Config{Schema: testSchema, Backend: backend.Memory(), Prefix: "bench/keys"})
	b.Cleanup(func() {
		if err := e.Close(context.Background()); err != nil {
			b.Fatal(err)
		}
	})
	rows := make([]rrec, 100)
	for i := range rows {
		rows[i] = rrec{ts: int64(i + 1), attr: [2]string{fmt.Sprintf("key.%02d", i%16), "x"}}
	}
	for i := range 100 {
		if _, err := e.AppendBatch(mkScopedBatch(fmt.Sprintf("service.%03d", i), rows...), recordengine.AppendLimits{}); err != nil {
			b.Fatal(err)
		}
	}
	if got := len(e.Keys(0, 0)); got != 19 {
		b.Fatalf("got %d keys, want19", got)
	}
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if got := len(e.Keys(0, 0)); got != 19 {
				b.Errorf("got %d keys, want19", got)
				return
			}
		}
	})
}

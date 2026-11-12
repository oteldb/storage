package readerpool

import (
	"bytes"
	"fmt"
	"strconv"
	"testing"
)

// BenchmarkPinnedPoolDecline isolates directory contention when all retained handles are pinned.
func BenchmarkPinnedPoolDecline(b *testing.B) {
	for _, batch := range []bool{false, true} {
		b.Run(fmt.Sprintf("batch=%t", batch), func(b *testing.B) { benchmarkPinnedPoolDecline(b, batch) })
	}
}

func benchmarkPinnedPoolDecline(b *testing.B, retain bool) {
	b.Helper()
	p := newPool(256)
	owner := testScope()
	owner.Acquire()
	open := func() (Reader, error) { return &testReader{Reader: bytes.NewReader([]byte("data"))}, nil }
	for i := range 256 {
		lease, ok, err := p.acquire(owner, 1, strconv.Itoa(i), open)
		if err != nil || !ok {
			b.Fatal(ok, err)
		}
		if err := lease.Close(); err != nil {
			b.Fatal(err)
		}
	}
	b.Cleanup(func() { owner.Release(); _ = p.closeScope(owner.id) })
	const reads = 64
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		scope := testScope()
		binding := binding{scope: scope, generation: scope.generation.Load()}
		var batch *Batch
		if retain {
			batch = newBatch(p)
		}
		defer func() {
			if batch != nil {
				_ = batch.Close()
			}
			_ = p.closeScope(scope.id)
		}()
		for pb.Next() {
			for range reads {
				var ok bool
				var err error
				if batch != nil {
					_, ok, err = batch.append(binding, 1, "missing", nil, 0, 4, open)
				} else {
					_, ok, err = p.appendGeneration(binding, 1, "missing", nil, 0, 4, open)
				}
				if err != nil || ok {
					b.Error("pinned pool must decline", ok, err)
					return
				}
			}
		}
	})
	b.ReportMetric(reads, "declines/op")
}

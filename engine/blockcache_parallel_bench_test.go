package engine

import (
	"fmt"
	"sync/atomic"
	"testing"
)

func BenchmarkDecodedCacheParallelHit(b *testing.B) {
	const entries = 1024
	c := newBlockCache(64 << 20)
	keys := make([]blockKey, entries)
	for i := range keys {
		keys[i] = blockKey{prefix: fmt.Sprintf("part-%d", i/16), col: colTsID, blk: i % 16}
		e := c.insert(&blockEntry{key: keys[i], i64: []int64{int64(i)}, bytes: 8})
		c.release(e)
	}
	var sequence atomic.Uint64
	b.SetParallelism(2)
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		state := sequence.Add(1)
		for pb.Next() {
			state = state*6364136223846793005 + 1442695040888963407
			index := int(state>>32) & (entries - 1)
			e, ok := c.get(keys[index])
			if !ok || e.i64[0] != int64(index) {
				b.Error("cached block missing or changed")
				return
			}
			c.release(e)
		}
	})
}

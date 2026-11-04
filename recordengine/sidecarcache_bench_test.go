package recordengine

import (
	"fmt"
	"testing"

	"github.com/oteldb/storage/encoding/chunk"
	"github.com/oteldb/storage/signal"
)

func BenchmarkSidecarDictionary(b *testing.B) {
	for _, mode := range []BloomMode{BloomFullText, BloomAttrs, BloomEquality} {
		for _, distinct := range []int{64, 4096, 65536} {
			b.Run(fmt.Sprintf("mode=%d/distinct=%d", mode, distinct), func(b *testing.B) {
				const rows = 128 << 10
				entries := make([][]byte, distinct)
				for i := range entries {
					if mode == BloomAttrs {
						entries[i] = attrsCell(b, signal.KeyValue{Key: []byte("host"), Value: signal.StringValue(fmt.Appendf(nil, "node-%d", i))})
					} else {
						entries[i] = fmt.Appendf(nil, "handled request path=/api/items/%d status=200 duration_ms=123", i)
					}
				}
				dc := chunk.DictColumn{Entries: entries, IDWidth: 2, IDs: make([]byte, 2*rows)}
				var logical int64
				for row := range rows {
					id := row % distinct
					dc.IDs[2*row], dc.IDs[2*row+1] = byte(id>>8), byte(id)
					logical += int64(len(entries[id]))
				}
				b.SetBytes(logical)
				b.ReportAllocs()
				for b.Loop() {
					s := &columnSink{bloom: &bloomAccum{mode: mode}, seen: make(map[uint64]int32)}
					if mode == BloomAttrs {
						s.keys = make(recordKeySet)
					}
					var cache sidecarCache
					for lo := 0; lo < rows; lo += 1024 {
						cache.add(s, &dc, lo, min(lo+1024, rows))
					}
					if s.bloom.bytes != logical {
						b.Fatal("wrong source byte count")
					}
				}
			})
		}
	}
}

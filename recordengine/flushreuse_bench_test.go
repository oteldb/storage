package recordengine

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/backend/file"
	"github.com/oteldb/storage/signal"
)

func BenchmarkHeadFlushCycles(b *testing.B) {
	for _, tc := range []struct {
		name    string
		streams int
		rows    int
		body    int
	}{
		{name: "tiny", streams: 16, rows: 4, body: 64},
		{name: "streams256", streams: 256, rows: 4, body: 64},
		{name: "streams1024", streams: 1024, rows: 4, body: 64},
		{name: "steady", streams: 16, rows: 64, body: 256},
		{name: "large_then_tiny", streams: 16, rows: 1024, body: 1024},
	} {
		b.Run(tc.name, func(b *testing.B) {
			schema := NewSchema(Column{Name: "body", Kind: KindBytes})
			h := newHead(schema)
			payload := bytes.Repeat([]byte("x"), tc.body)
			r := rec{bytes: [][]byte{payload}}
			cycle := func(rows int) {
				for stream := range tc.streams {
					id := signal.SeriesID{Lo: uint64(stream)}
					for i := range rows {
						r.ts = int64(i)
						h.appendRecord(id, r, 0, 0)
					}
				}
				detached, _ := h.detach()
				h.releaseDetached()
				if pool, ok := any(h).(interface {
					recycleRecordCols(records map[signal.SeriesID]*recordCols)
				}); ok {
					pool.recycleRecordCols(detached)
				}
			}
			if tc.name == "large_then_tiny" {
				cycle(tc.rows)
				tc.rows = 4
				r.bytes[0] = payload[:64]
			}
			b.SetBytes(int64(tc.streams * tc.rows * (8 + len(r.bytes[0]))))
			b.ReportAllocs()
			for b.Loop() {
				cycle(tc.rows)
			}
			if pool, ok := any(h).(interface{ reusableSize() int64 }); ok {
				b.ReportMetric(float64(pool.reusableSize()), "retained-B")
			} else {
				b.ReportMetric(0, "retained-B")
			}
		})
	}
}

func BenchmarkAppendFlushCycles(b *testing.B) {
	schema := NewSchema(Column{Name: "body", Kind: KindBytes})
	for _, medium := range []string{"memory", "file"} {
		for _, streams := range []int{16, 1024} {
			b.Run(fmt.Sprintf("%s/streams%d", medium, streams), func(b *testing.B) {
				be := backend.Memory()
				if medium == "file" {
					disk, err := file.New(b.TempDir())
					if err != nil {
						b.Fatal(err)
					}
					be = disk
				}
				e := New(Config{Schema: schema, Backend: be, Prefix: "bench"})
				batches := make([]Batch, streams)
				for i := range batches {
					identity := signal.Series{Resource: signal.Resource{Attributes: signal.NewAttributes(signal.KeyValue{Key: []byte("service"), Value: signal.IntValue(int64(i))})}}
					batches[i] = Batch{Stream: identity.Hash(), Identity: func() signal.Series { return identity }, Ts: []int64{1, 2, 3, 4}, Bytes: [][][]byte{{[]byte("body"), []byte("body"), []byte("body"), []byte("body")}}}
				}
				b.ReportAllocs()
				b.SetBytes(int64(streams * 4 * 12))
				cycle := func() {
					for i := range batches {
						if _, err := e.AppendBatch(&batches[i], AppendLimits{}); err != nil {
							b.Fatal(err)
						}
					}
					if err := e.Flush(b.Context()); err != nil {
						b.Fatal(err)
					}
				}
				retire := func() {
					// Keep ingest scratch, but bound both published parts and catalog tombstones.
					previous := e
					head := e.head
					if err := e.Reset(b.Context()); err != nil {
						b.Fatal(err)
					}
					e = New(previous.cfg)
					e.head, e.flushBuf, e.bloomBuf = head, previous.flushBuf, previous.bloomBuf
					e.readCompressors = previous.readCompressors
				}
				cycle()
				retire()
				for b.Loop() {
					cycle()
					b.StopTimer()
					retire()
					b.StartTimer()
				}
				if pool, ok := any(e.head).(interface{ reusableSize() int64 }); ok {
					b.ReportMetric(float64(pool.reusableSize()), "retained-B")
				} else {
					b.ReportMetric(0, "retained-B")
				}
			})
		}
	}
}

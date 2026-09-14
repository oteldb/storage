package recordengine_test

import (
	"testing"

	"github.com/oteldb/storage/recordengine"
	"github.com/oteldb/storage/wal"
)

// BenchmarkAppendBatchWAL measures the logged append: admission, encoding the accepted records into
// the WAL payload, the log write, and applying the records to the head.
func BenchmarkAppendBatchWAL(b *testing.B) {
	const rows = 1000

	w, err := wal.Create(b.TempDir(), 0)
	if err != nil {
		b.Fatal(err)
	}

	b.Cleanup(func() { _ = w.Close() })

	e := recordengine.New(recordengine.Config{Schema: testSchema, WAL: w})

	recs := make([]rrec, rows)
	for i := range recs {
		recs[i] = rrec{ts: int64(i), sev: 9, body: "GET /api/v1/thing status=200 handler=list done", id: "0123456789abcdef"}
	}

	batch := mkBatch("api", recs...)

	var logical int64
	for i := range batch.Ts {
		logical += 16
		for k := range batch.Bytes {
			logical += int64(len(batch.Bytes[k][i]))
		}
	}

	b.SetBytes(logical)
	b.ReportAllocs()

	for i := 0; b.Loop(); i++ {
		if i%256 == 0 {
			b.StopTimer()
			_ = e.Reset(b.Context())
			b.StartTimer()
		}

		for j := range batch.Ts {
			batch.Ts[j] = int64(i*rows + j)
		}

		if _, err := e.AppendBatch(batch, recordengine.AppendLimits{}); err != nil {
			b.Fatal(err)
		}
	}
}

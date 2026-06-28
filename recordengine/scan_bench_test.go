package recordengine_test

import (
	"bytes"
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/recordengine"
	"github.com/oteldb/storage/signal"
)

// Head byte-column profiling benchmarks — the read/scan analog of the ingest bench.
//
// They drive the head buffer's byte columns (body/id/attrs of the log schema) through the two paths
// the offsets+blob layout targets:
//
//   - BenchmarkHeadByteScan: a per-record Condition on a byte column, which scans every buffered row
//     via recordCols.colValue/rowMatches — one cell read per row. With the [][]byte cell layout this
//     pointer-chases the heap; with offsets+blob it is a sequential walk of one contiguous blob.
//
//   - BenchmarkHeadByteIngest: the append side — every record clones its byte cells into the head
//     buffer (the slices.Clone[uint8] live-heap holder), with a periodic Reset for steady state.
//
//     go test -run=^$ -bench=^BenchmarkHeadByte -benchmem ./recordengine
const (
	scanStream = "api"
	scanRows   = 20000
)

// scanCorpus is one stream's worth of log records with a full-text body, an equality id, and a
// per-record attribute, ts strictly increasing so none are rejected out-of-order.
func scanCorpus(rows int) []rrec {
	recs := make([]rrec, rows)
	for i := range recs {
		method := "GET"
		if i%2 == 1 {
			method = "POST"
		}

		recs[i] = rrec{
			ts:   int64(i + 1),
			sev:  int64(i%24 + 1),
			body: fmt.Sprintf("%s /api/v1/resource/%d status=200 latency=%dms", method, i%256, i%1000),
			id:   fmt.Sprintf("%016x", i),
			attr: [2]string{"http.method", method},
		}
	}

	return recs
}

// BenchmarkHeadByteScan fetches the head with a byte-column Condition that matches ~half the rows,
// forcing a full per-record scan of a byte column (colValue → cell read).
func BenchmarkHeadByteScan(b *testing.B) {
	ctx := context.Background()
	e := recordengine.New(recordengine.Config{Schema: testSchema, Prefix: "t/recs"})

	batch := mkBatch(scanStream, scanCorpus(scanRows)...)
	if _, err := e.AppendBatch(batch, recordengine.AppendLimits{}); err != nil {
		b.Fatal(err)
	}

	want := []byte("GET ")
	cond := fetch.Condition{
		Column: "body",
		Match:  func(v signal.Value) bool { return bytes.HasPrefix(v.Str(), want) },
	}
	r := fetch.Request{
		Signal: signal.Log, Start: 0, End: 1 << 60,
		Matchers:      []fetch.Matcher{svcMatcher(scanStream)},
		Conditions:    []fetch.Condition{cond},
		AllConditions: true,
	}

	b.ReportAllocs()
	b.ResetTimer()

	var rows int
	for range b.N {
		it, err := e.Fetch(ctx, r)
		if err != nil {
			b.Fatal(err)
		}

		got, err := fetch.Drain(ctx, it)
		if err != nil {
			b.Fatal(err)
		}

		rows = 0
		for _, bt := range got {
			rows += len(bt.Timestamps)
		}
	}

	require.Equal(b, scanRows/2, rows)
}

// BenchmarkHeadByteScanProjected is the realistic log-query shape: it projects a single byte column
// and recycles the result buffers (Recycle), so the per-fetch accumulator and its materialized views
// are pooled across iterations rather than re-allocated — the path the offsets+blob layout is tuned
// for. Each returned batch is released so the next fetch reuses its buffers.
func BenchmarkHeadByteScanProjected(b *testing.B) {
	ctx := context.Background()
	e := recordengine.New(recordengine.Config{Schema: testSchema, Prefix: "t/recs"})

	batch := mkBatch(scanStream, scanCorpus(scanRows)...)
	if _, err := e.AppendBatch(batch, recordengine.AppendLimits{}); err != nil {
		b.Fatal(err)
	}

	want := []byte("GET ")
	r := fetch.Request{
		Signal: signal.Log, Start: 0, End: 1 << 60,
		Matchers: []fetch.Matcher{svcMatcher(scanStream)},
		Conditions: []fetch.Condition{{
			Column: "body",
			Match:  func(v signal.Value) bool { return bytes.HasPrefix(v.Str(), want) },
		}},
		AllConditions: true,
		Projection:    []string{"body"},
		Recycle:       true,
	}

	b.ReportAllocs()
	b.ResetTimer()

	for range b.N {
		it, err := e.Fetch(ctx, r)
		if err != nil {
			b.Fatal(err)
		}

		for {
			bt, err := it.Next(ctx)
			if err != nil {
				break
			}

			bt.Release()
		}

		if err := it.Close(); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkHeadByteIngest drives the head append path for byte columns: each record clones its
// body/id/attrs cells into the per-stream buffer. It Resets periodically so the buffers do not grow
// without bound, mirroring the steady-state head.
func BenchmarkHeadByteIngest(b *testing.B) {
	ctx := context.Background()
	s := recordengine.New(recordengine.Config{Schema: testSchema, Prefix: "t/recs"})

	const perBatch = 1000
	batch := mkBatch(scanStream, scanCorpus(perBatch)...)
	resetEvery := max((1<<20)/perBatch, 1)

	b.ReportAllocs()
	b.ResetTimer()

	for i := range b.N {
		if _, err := s.AppendBatch(batch, recordengine.AppendLimits{}); err != nil {
			b.Fatal(err)
		}

		if (i+1)%resetEvery == 0 {
			b.StopTimer()
			if err := s.Reset(ctx); err != nil {
				b.Fatal(err)
			}
			b.StartTimer()
		}
	}

	goldenReportRecords(b, perBatch)
}

// goldenReportRecords sets b.Bytes to the per-op logical record count so throughput is comparable
// across runs (records/s rather than an opaque ns/op).
func goldenReportRecords(b *testing.B, records int) {
	b.Helper()
	b.SetBytes(int64(records))
}

package storage

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"strconv"
	"testing"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/index/bloom"
	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/signal"
	"github.com/oteldb/storage/signal/trace"
)

// traceID16 builds the fixed 16-byte trace id for (round, svc, tr) and spanID8 the fixed 8-byte span
// id for (svc, tr, sp) — the real OTLP id widths, so the trace_id/span_id columns exercise the
// fixed-width (flagFixed) encoding path rather than falling to the variable-length flat fallback.
func traceID16(round, svc, tr int) []byte {
	id := make([]byte, 16)
	binary.BigEndian.PutUint64(id[0:8], uint64(round))
	binary.BigEndian.PutUint32(id[8:12], uint32(svc))
	binary.BigEndian.PutUint32(id[12:16], uint32(tr))

	return id
}

func spanID8(svc, tr, sp int) []byte {
	id := make([]byte, 8)
	binary.BigEndian.PutUint16(id[0:2], uint16(svc))
	binary.BigEndian.PutUint16(id[2:4], uint16(tr))
	binary.BigEndian.PutUint32(id[4:8], uint32(sp))

	return id
}

// genTraceRound builds one round's spans: services × tracesPerSvc traces, each a root + children.
// Each round stamps a round-specific token (evt{round}) into the span name and a round-specific
// region attribute, so a query for a rare token matches one part (the others bloom-prune) while a
// common token (e.g. "GET") and a common status scan everything. The known trace id traceID16(round,0,0)
// in each round feeds the by-id benchmark. It accumulates the logical (uncompressed) searched bytes
// into total, sized by the columns a span search actually reads.
func genTraceRound(round, services, tracesPerSvc, spansPerTrace int, total *int64) trace.Traces {
	var td trace.Traces

	for svc := range services {
		rs := td.AddResource()
		rs.Resource = signal.Resource{Attributes: signal.NewAttributes(
			signal.KeyValue{Key: []byte("service.name"), Value: signal.StringValue(fmt.Appendf(nil, "svc-%d", svc))},
		)}
		ss := rs.AddScope()

		for tr := range tracesPerSvc {
			traceID := traceID16(round, svc, tr)
			region := fmt.Sprintf("r%d", round)
			rootID := spanID8(svc, tr, 0)

			for sp := range spansPerTrace {
				name := fmt.Sprintf("GET /api/v1/op%d evt%d", sp%20, round)

				s := ss.AddSpan()
				s.TraceID = traceID
				s.Name = []byte(name)
				s.Start = int64(round*1_000_000 + tr*100 + sp)
				s.End = s.Start + int64(50+sp)
				s.StatusCode = int32(sp % 3)

				if sp == 0 {
					s.SpanID = rootID
				} else {
					s.SpanID = spanID8(svc, tr, sp)
					s.ParentSpanID = rootID
				}

				s.Attributes = signal.NewAttributes(
					signal.KeyValue{Key: []byte("region"), Value: signal.StringValue([]byte(region))},
					signal.KeyValue{Key: []byte("http.method"), Value: signal.StringValue([]byte("GET"))},
				)

				*total += int64(len(name)) // duration + status are derived; name is the searched bytes
			}
		}
	}

	return td
}

func durationAtLeastCond(d int64) fetch.Condition {
	return fetch.Condition{Column: trace.ColDuration, Match: func(v signal.Value) bool { return v.Int() >= d }}
}

func statusEqualCond(code int64) fetch.Condition {
	return fetch.Condition{Column: trace.ColStatusCode, Match: func(v signal.Value) bool { return v.Int() == code }}
}

func nameContainsCond(sub string) fetch.Condition {
	want := []byte(sub)

	return fetch.Condition{
		Column: trace.ColName,
		Match:  func(v signal.Value) bool { return bytes.Contains(v.Str(), want) },
		Tokens: bloom.Tokenize(nil, want),
	}
}

// loadTraceStore ingests a multi-part trace store, one flushed part per round, and returns the
// store, the per-round token base, and the logical searched-byte count for b.SetBytes.
func loadTraceStore(b *testing.B, services, tracesPerSvc, spansPerTrace, rounds int) (*Storage, int64) {
	b.Helper()
	ctx := context.Background()

	s, err := InMemory()
	if err != nil {
		b.Fatal(err)
	}

	b.Cleanup(func() { _ = s.Close(ctx) })

	var logical int64

	for round := range rounds {
		if _, err := s.WriteTraces(ctx, genTraceRound(round, services, tracesPerSvc, spansPerTrace, &logical)); err != nil {
			b.Fatal(err)
		}

		// Flush each round to its own part (no merge) so cross-part pruning is exercised.
		if eng, ok := s.lookupTraceEngine("default"); ok {
			if err := eng.Flush(ctx); err != nil {
				b.Fatal(err)
			}
		}
	}

	return s, logical
}

// traceIDStoredBytes sums the on-backend size of the trace_id column object ({part}/c/{i}) and its
// equality bloom across the default tenant's flushed trace parts — the direct storage cost of the
// trace_id encoding, reported by the by-id benchmark so a codec change shows as a size delta.
func traceIDStoredBytes(b *testing.B, s *Storage) int64 {
	b.Helper()
	ctx := context.Background()

	eng, ok := s.lookupTraceEngine("default")
	if !ok {
		return 0
	}

	details, err := eng.PartsDetailed(ctx)
	if err != nil {
		b.Fatal(err)
	}

	var total int64

	for _, p := range details {
		col := -1
		for i := range p.Columns {
			if p.Columns[i].Name == trace.ColTraceID {
				col = i

				break
			}
		}

		if col >= 0 {
			if n, err := backend.SizeOf(ctx, s.backend, p.ID+"/c/"+strconv.Itoa(col)); err == nil {
				total += n
			}
		}

		if n, err := backend.SizeOf(ctx, s.backend, p.ID+"/bloom-"+trace.ColTraceID+".bin"); err == nil {
			total += n
		}
	}

	return total
}

// BenchmarkTraceSearch is the end-to-end span-search benchmark: resource matchers resolve streams,
// span-column and attribute Conditions filter spans, and name/attr blooms prune parts. Throughput
// (b.SetBytes) is sized by the full logical dataset, so a pruned query reports a much higher
// effective search rate than a scanning one — that gap is the bloom pruning at work.
func BenchmarkTraceSearch(b *testing.B) {
	ctx := context.Background()

	const (
		services      = 8
		tracesPerSvc  = 40
		spansPerTrace = 8
		rounds        = 10 // ⇒ 10 flushed parts, 25.6k spans
		rareRound     = 5
	)

	s, logical := loadTraceStore(b, services, tracesPerSvc, spansPerTrace, rounds)

	req := func(c fetch.Condition, projection ...string) fetch.Request {
		return fetch.Request{
			Signal: signal.Trace, Start: 0, End: 1 << 60,
			Conditions: []fetch.Condition{c}, AllConditions: true, Projection: projection,
		}
	}

	rare := fmt.Sprintf("evt%d", rareRound)

	cases := []struct {
		name string
		req  fetch.Request
	}{
		// Full scan (common token/column in every part): all-columns vs name-only projection shows
		// the lazy-decode win — only referenced columns are decoded, copied, and returned.
		{"Name/CommonToken_scanAllCols", req(nameContainsCond("GET"))},
		{"Name/CommonToken_scanProjName", req(nameContainsCond("GET"), trace.ColName)},
		{"Name/RareToken_prune", req(nameContainsCond(rare))},         // one part; nine pruned
		{"Name/AbsentToken_prune", req(nameContainsCond("zzzznone"))}, // all pruned
		{"Duration/Scan", req(durationAtLeastCond(54))},               // numeric range: no prune, full scan
		{"Status/Scan", req(statusEqualCond(1))},                      // status filter: full scan
	}

	for i := range cases {
		tc := &cases[i]
		b.Run(tc.name, func(b *testing.B) {
			b.SetBytes(logical)
			b.ReportAllocs()

			for b.Loop() {
				it, err := s.TraceFetcher("default").Fetch(ctx, tc.req)
				if err != nil {
					b.Fatal(err)
				}

				if _, err := fetch.Drain(ctx, it); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkTraceByID measures the trace-by-id fast path: an equality Condition on trace_id pruned
// by that column's Equality bloom. A present id touches the one part holding it (the rest prune);
// an absent id prunes every part. Throughput is sized by the full logical dataset so the pruning
// shows as a high effective rate.
//
// The per-part trace_id cardinality (services × tracesPerSvc) is deliberately kept above the
// dictionary codec's 65536-distinct cap, matching real trace stores where a part holds hundreds of
// thousands of distinct ids. Below that cap a synthetic run would let CodecDict dictionary-encode
// trace_id and mis-rank it against the fixed-width CodecBytesRaw; above it (as here, and in
// production) CodecDict degrades to its flat length-prefixed fallback, which the traceid_bytes
// metric reflects.
func BenchmarkTraceByID(b *testing.B) {
	ctx := context.Background()

	const (
		services      = 8
		tracesPerSvc  = 9000 // 72k distinct trace ids per round part (> the 65536 dict cap)
		spansPerTrace = 2
		rounds        = 4
		hitRound      = 2
	)

	s, logical := loadTraceStore(b, services, tracesPerSvc, spansPerTrace, rounds)
	idBytes := traceIDStoredBytes(b, s)

	cases := []struct {
		name string
		id   []byte
	}{
		{"Present_prune", traceID16(hitRound, 0, 0)},     // one part holds it
		{"Absent_prune", bytes.Repeat([]byte{0xFF}, 16)}, // all parts prune
	}

	for i := range cases {
		tc := &cases[i]
		b.Run(tc.name, func(b *testing.B) {
			b.SetBytes(logical)
			b.ReportAllocs()

			for b.Loop() {
				got, err := s.Trace(ctx, "default", tc.id)
				if err != nil {
					b.Fatal(err)
				}

				_ = got
			}

			b.ReportMetric(float64(idBytes), "traceid_bytes")
		})
	}
}

// BenchmarkTraceByService measures the by-service span lookup: a resource matcher on service.name
// resolves one service's stream over the postings index, and the fetch returns that stream's spans
// across every part (its per-part row range). Unlike the by-id path (a column Condition scanned per
// row), this exercises stream resolution + the per-stream row-range index, and the lazy column
// decode: the all-columns case decodes every span column, the name-only projection just the name.
func BenchmarkTraceByService(b *testing.B) {
	ctx := context.Background()

	const (
		services      = 8
		tracesPerSvc  = 200
		spansPerTrace = 8
		rounds        = 10 // ⇒ 10 parts; each service's spans span all parts
	)

	s, logical := loadTraceStore(b, services, tracesPerSvc, spansPerTrace, rounds)

	req := func(projection ...string) fetch.Request {
		return fetch.Request{
			Signal: signal.Trace, Start: 0, End: 1 << 60,
			Matchers: []fetch.Matcher{nameMatcherSvc("svc-3")}, Projection: projection,
		}
	}

	cases := []struct {
		name string
		req  fetch.Request
	}{
		{"AllCols", req()},
		{"ProjName", req(trace.ColName)},
	}

	for i := range cases {
		tc := &cases[i]
		b.Run(tc.name, func(b *testing.B) {
			b.SetBytes(logical)
			b.ReportAllocs()

			for b.Loop() {
				it, err := s.TraceFetcher("default").Fetch(ctx, tc.req)
				if err != nil {
					b.Fatal(err)
				}

				if _, err := fetch.Drain(ctx, it); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkWriteTraces measures the trace ingest write path: projecting the span model (nested-set
// ids, event/link serialization), deriving the tenant, indexing streams, and buffering records into
// the head. The Flush variant adds the flush-to-part cost (column encode + bloom build + backend
// write). Reported Mspans/s isolates the per-span ingest cost; the store is reset periodically so
// the head does not grow unbounded across b.N.
func BenchmarkWriteTraces(b *testing.B) {
	shapes := []struct {
		name                                  string
		services, tracesPerSvc, spansPerTrace int
	}{
		{"8svc_50traces_8spans", 8, 50, 8},
		{"32svc_100traces_4spans", 32, 100, 4},
	}

	for _, sh := range shapes {
		b.Run(sh.name, func(b *testing.B) {
			benchmarkWriteTraces(b, sh.services, sh.tracesPerSvc, sh.spansPerTrace, false)
		})
		b.Run(sh.name+"/Flush", func(b *testing.B) {
			benchmarkWriteTraces(b, sh.services, sh.tracesPerSvc, sh.spansPerTrace, true)
		})
	}
}

func benchmarkWriteTraces(b *testing.B, services, tracesPerSvc, spansPerTrace int, flush bool) {
	b.Helper()

	ctx := context.Background()

	var logical int64

	td := genTraceRound(0, services, tracesPerSvc, spansPerTrace, &logical)
	totalSpans := services * tracesPerSvc * spansPerTrace
	resetEvery := max((1<<20)/totalSpans, 1)

	s, err := InMemory()
	if err != nil {
		b.Fatal(err)
	}

	b.Cleanup(func() { _ = s.Close(ctx) })
	b.SetBytes(logical)
	b.ReportAllocs()
	b.ResetTimer()

	for i := range b.N {
		if _, err := s.WriteTraces(ctx, td); err != nil {
			b.Fatal(err)
		}

		if flush {
			if eng, ok := s.lookupTraceEngine("default"); ok {
				if err := eng.Flush(ctx); err != nil {
					b.Fatal(err)
				}
			}
		}

		if (i+1)%resetEvery == 0 {
			b.StopTimer()

			if err := s.Reset(ctx); err != nil {
				b.Fatal(err)
			}

			b.StartTimer()
		}
	}

	if secs := b.Elapsed().Seconds(); secs > 0 {
		b.ReportMetric(float64(totalSpans)*float64(b.N)/secs/1e6, "Mspans/s")
	}
}

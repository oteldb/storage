package storage

import (
	"bytes"
	"context"
	"fmt"
	"testing"

	"github.com/oteldb/storage/index/bloom"
	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/signal"
	"github.com/oteldb/storage/signal/trace"
)

// genTraceRound builds one round's spans: services × tracesPerSvc traces, each a root + children.
// Each round stamps a round-specific token (evt{round}) into the span name and a round-specific
// region attribute, so a query for a rare token matches one part (the others bloom-prune) while a
// common token (e.g. "GET") and a common status scan everything. The known trace id "trace-{round}-0"
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
			traceID := fmt.Sprintf("trace-%d-%d-%d", round, svc, tr)
			region := fmt.Sprintf("r%d", round)
			rootID := traceID + "-root"

			for sp := range spansPerTrace {
				name := fmt.Sprintf("GET /api/v1/op%d evt%d", sp%20, round)

				s := ss.AddSpan()
				s.TraceID = []byte(traceID)
				s.Name = []byte(name)
				s.Start = int64(round*1_000_000 + tr*100 + sp)
				s.End = s.Start + int64(50+sp)
				s.StatusCode = int32(sp % 3)

				if sp == 0 {
					s.SpanID = []byte(rootID)
				} else {
					s.SpanID = fmt.Appendf(nil, "%s-%d", traceID, sp)
					s.ParentSpanID = []byte(rootID)
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
func BenchmarkTraceByID(b *testing.B) {
	ctx := context.Background()

	const (
		services      = 8
		tracesPerSvc  = 40
		spansPerTrace = 8
		rounds        = 10
		hitRound      = 5
	)

	s, logical := loadTraceStore(b, services, tracesPerSvc, spansPerTrace, rounds)

	cases := []struct {
		name string
		id   []byte
	}{
		{"Present_prune", fmt.Appendf(nil, "trace-%d-0-0", hitRound)}, // one part holds it
		{"Absent_prune", []byte("trace-no-such-id")},                  // all parts prune
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
		})
	}
}

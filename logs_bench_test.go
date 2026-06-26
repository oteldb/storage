package storage

import (
	"bytes"
	"context"
	"fmt"
	"testing"

	"github.com/oteldb/storage/index/bloom"
	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/signal"
	"github.com/oteldb/storage/signal/log"
)

// genLogRound builds one round's logs: services × perService records. Each round stamps a
// round-specific token (evt{round}) into the body and a round-specific region/msg, so a query for
// a rare token matches one part and the others are bloom-pruned; common tokens (e.g. "status") are
// in every record so a query for them scans everything. It accumulates the logical (uncompressed)
// searched-bytes into total.
func genLogRound(round, services, perService int, total *int64) log.Logs {
	var ld log.Logs

	for svc := range services {
		rl := ld.AddResource()
		rl.Resource = signal.Resource{Attributes: signal.NewAttributes(
			signal.KeyValue{Key: []byte("service.name"), Value: signal.StringValue(fmt.Appendf(nil, "svc-%d", svc))},
		)}
		sl := rl.AddScope()

		for i := range perService {
			region := fmt.Sprintf("r%d", round)
			body := fmt.Sprintf("GET /api/v1/users status=200 latency=%dms evt%d trace done", i%50, round)
			msg := fmt.Sprintf("request processed region=%s evt%d ok", region, round)

			r := sl.AddRecord()
			r.Timestamp = int64(round*1_000_000 + i)
			r.SeverityNumber = int32(9 + i%9)
			r.Body = []byte(body)
			r.Attributes = signal.NewAttributes(
				signal.KeyValue{Key: []byte("region"), Value: signal.StringValue([]byte(region))},
				signal.KeyValue{Key: []byte("user"), Value: signal.StringValue(fmt.Appendf(nil, "user-%d", i%1000))},
				signal.KeyValue{Key: []byte("msg"), Value: signal.StringValue([]byte(msg))},
			)

			*total += int64(len(body) + len(region) + len(msg))
		}
	}

	return ld
}

// bodyContainsCond / attrEqualCond / attrContainsCond build the same conditions a LogQL line filter
// would compile to, carrying the serializable bloom hints (Tokens / Equal) for part pruning.
func bodyContainsCond(sub string) fetch.Condition {
	want := []byte(sub)

	return fetch.Condition{
		Column: "body",
		Match:  func(v signal.Value) bool { return bytes.Contains(v.Str(), want) },
		Tokens: bloom.Tokenize(nil, want),
	}
}

func attrEqualCond(key, val string) fetch.Condition {
	want := []byte(val)

	return fetch.Condition{
		Column: key,
		Match:  func(v signal.Value) bool { return bytes.Equal(v.Str(), want) },
		Equal:  &fetch.EqualMatcher{Name: key, Value: val},
	}
}

func attrContainsCond(key, sub string) fetch.Condition {
	want := []byte(sub)

	return fetch.Condition{
		Column: key,
		Match:  func(v signal.Value) bool { return bytes.Contains(v.Str(), want) },
		Tokens: bloom.Tokenize(nil, want),
	}
}

// BenchmarkLogTextSearch is the end-to-end text-search benchmark over body and attributes: it
// ingests a multi-part log store, then measures LogFetcher queries that either scan (a common
// token, present in every part) or prune (a rare token in one part, or an absent token in none).
// Throughput (b.SetBytes) is sized by the full logical dataset, so a pruned query reports a much
// higher effective search rate than a scanning one — that gap is the bloom pruning at work.
func BenchmarkLogTextSearch(b *testing.B) {
	ctx := context.Background()

	s, err := InMemory()
	if err != nil {
		b.Fatal(err)
	}

	b.Cleanup(func() { _ = s.Close(ctx) })

	const (
		services   = 8
		perService = 600
		rounds     = 10 // ⇒ 10 flushed parts, 48k records
		rareRound  = 5
	)

	var logical int64

	for round := range rounds {
		if _, err := s.WriteLogs(ctx, genLogRound(round, services, perService, &logical)); err != nil {
			b.Fatal(err)
		}

		// Flush this round to its own part (no merge), so the store holds many parts and
		// cross-part pruning is exercised.
		if eng, ok := s.lookupLogEngine("default"); ok {
			if err := eng.Flush(ctx); err != nil {
				b.Fatal(err)
			}
		}
	}

	req := func(c fetch.Condition) fetch.Request {
		return fetch.Request{Signal: signal.Log, Start: 0, End: 1 << 60, Conditions: []fetch.Condition{c}, AllConditions: true}
	}

	rare := fmt.Sprintf("evt%d", rareRound)

	cases := []struct {
		name string
		req  fetch.Request
	}{
		{"Body/CommonToken_scan", req(bodyContainsCond("status"))},    // present in every part
		{"Body/RareToken_prune", req(bodyContainsCond(rare))},         // one part; nine pruned
		{"Body/AbsentToken_prune", req(bodyContainsCond("zzzznone"))}, // all pruned
		{"Attr/EqualRare_prune", req(attrEqualCond("region", fmt.Sprintf("r%d", rareRound)))},
		{"Attr/EqualAbsent_prune", req(attrEqualCond("region", "mars"))},
		{"Attr/ContainsRare_prune", req(attrContainsCond("msg", rare))},
		{"Attr/ContainsAbsent_prune", req(attrContainsCond("msg", "zzzznone"))},
	}

	for i := range cases {
		tc := &cases[i]
		b.Run(tc.name, func(b *testing.B) {
			b.SetBytes(logical)
			b.ReportAllocs()

			for b.Loop() {
				it, err := s.LogFetcher("default").Fetch(ctx, tc.req)
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

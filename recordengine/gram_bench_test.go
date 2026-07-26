package recordengine

import (
	"strconv"
	"testing"

	"github.com/oteldb/storage/query/fetch"
)

// benchBodies is a column of templated log lines with a per-row identifier — the shape the gram
// index is built for (a distinctive token glued into a larger one) and the shape whose repetition
// the row dedup exploits.
func benchBodies(rows int) *byteCol {
	var c byteCol

	for i := range rows {
		c.appendCell([]byte("2026-07-23T09:45:37Z INFO checkout-service handler=CreateOrder " +
			"trace[deadbeefcafebabe" + strconv.Itoa(i) + "] user=" + strconv.Itoa(i%37) +
			" latency_ms=42 status=OK Region=EU-Central-1"))
	}

	return &c
}

// BenchmarkGramBuild sizes throughput by the column's logical bytes, so it is directly comparable to
// BenchmarkBloomBuild beside it: the gram build is the more expensive of the two, and it runs on
// every flush and every merge of a [Column.Grams] column.
func BenchmarkGramBuild(b *testing.B) {
	for _, rows := range []int{1000, 10000} {
		col := benchBodies(rows)

		b.Run(strconv.Itoa(rows)+"rows", func(b *testing.B) {
			b.SetBytes(int64(len(col.data)))
			b.ReportAllocs()

			var out int

			for b.Loop() {
				var bb bloomBuilder

				out = len(bb.buildGrams(col))
			}

			b.ReportMetric(float64(out), "filter_B")
		})
	}
}

// BenchmarkBloomBuild is the token build over the same column, as the baseline the gram build's cost
// is judged against.
func BenchmarkBloomBuild(b *testing.B) {
	for _, rows := range []int{1000, 10000} {
		col := benchBodies(rows)

		b.Run(strconv.Itoa(rows)+"rows", func(b *testing.B) {
			b.SetBytes(int64(len(col.data)))
			b.ReportAllocs()

			var out int

			for b.Loop() {
				var bb bloomBuilder

				out = len(bb.build(BloomFullText, col))
			}

			b.ReportMetric(float64(out), "filter_B")
		})
	}
}

// BenchmarkGramHints times the per-fetch hint extraction, which happens once per request regardless
// of part count — it must stay far below the cost of the part reads it saves.
func BenchmarkGramHints(b *testing.B) {
	conds := []fetch.Condition{{
		Column:     "body",
		Substrings: [][]byte{[]byte("deadbeefcafebabe0123456789abcdef")},
	}}

	b.ReportAllocs()

	for b.Loop() {
		gramHintsSink = buildGramHints(conds)
	}
}

var gramHintsSink gramHints

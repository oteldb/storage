package engine_test

import (
	"context"
	"strconv"
	"testing"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/engine"
	"github.com/oteldb/storage/query/fetch"
)

// The question this measures: with in-part granule pruning wired, does splitting the same data into
// more (narrower) parts make a narrow query cheaper, or only add per-part fixed cost?
//
// It decides how wide a time partition should be. If cost is flat in part count, wide partitions are
// free and the width should be chosen for retention granularity and write amplification instead. If
// cost rises with part count, the slope is the per-part fixed cost — index lookup per matched series,
// sidecar loads, and one extra run in the k-way merge — and it argues for wider parts.
//
// The controls that make it honest: total rows, total series, and the queried window are identical
// across every sub-benchmark. Only the number of parts the rows are divided into changes, so the
// per-part cost is the only thing that can move.
const (
	pcSeries    = 5000 // distinct series in the store
	pcTotalRows = 1440 // samples per series, held constant across all splits
	pcStepSec   = 60
	pcBlockRows = 256
)

// BenchmarkPartCountNarrowQuery queries a single series over a window covering ~1/24th of the store
// (the issue's one-hour-out-of-a-day shape), against stores holding identical data in 1..48 parts.
func BenchmarkPartCountNarrowQuery(b *testing.B) {
	ctx := context.Background()

	// Each split must divide pcTotalRows exactly, so every store holds the same rows.
	for _, parts := range []int{1, 2, 4, 8, 16, 48} {
		samples := pcTotalRows / parts

		b.Run("parts"+strconv.Itoa(parts), func(b *testing.B) {
			ser, ids := buildNamedSeries(pcSeries, "node_cpu_seconds_total")
			e := engine.New(engine.Config{
				Backend: backend.Memory(), Prefix: "bench/partcount",
				MaxPartBytes: 0, MetricBlockRows: pcBlockRows, DecodeCacheBytes: 1 << 30,
			})
			flushParts(b, ctx, e, ser, ids, samples, pcStepSec, parts)

			if got := e.PartCount(); got != parts {
				b.Fatalf("want %d parts, got %d", parts, got)
			}

			// One hour out of the store's full span, anchored in the middle so it falls inside a
			// part for every split rather than on an edge. flushParts stamps timestamps in units of
			// stepSec (plus a per-series offset), not nanoseconds, so the window is in those units.
			span := int64(pcTotalRows) * pcStepSec
			mid := span / 2
			start := mid - 1800
			end := mid + 1800

			req := fetch.Request{
				Start: start, End: end,
				Matchers: []fetch.Matcher{eqMatcher("instance", "host-2001")},
			}

			if n := len(fetchAll2(b, ctx, e, req)); n != 1 {
				b.Fatalf("want 1 matched series, got %d", n)
			}

			b.ReportAllocs()

			for b.Loop() {
				fetchAll2(b, ctx, e, req)
			}
		})
	}
}

// BenchmarkPartCountWideQuery is the control: the same stores queried over their whole span. Here
// every part is genuinely needed, so this isolates the cost of merging more runs from the cost of
// opening parts a narrow query did not need.
func BenchmarkPartCountWideQuery(b *testing.B) {
	ctx := context.Background()

	for _, parts := range []int{1, 2, 4, 8, 16, 48} {
		samples := pcTotalRows / parts

		b.Run("parts"+strconv.Itoa(parts), func(b *testing.B) {
			ser, ids := buildNamedSeries(pcSeries, "node_cpu_seconds_total")
			e := engine.New(engine.Config{
				Backend: backend.Memory(), Prefix: "bench/partcountwide",
				MaxPartBytes: 0, MetricBlockRows: pcBlockRows, DecodeCacheBytes: 1 << 30,
			})
			flushParts(b, ctx, e, ser, ids, samples, pcStepSec, parts)

			req := fetch.Request{
				Start: 0, End: 1 << 62,
				Matchers: []fetch.Matcher{eqMatcher("instance", "host-2001")},
			}

			if n := len(fetchAll2(b, ctx, e, req)); n != 1 {
				b.Fatalf("want 1 matched series, got %d", n)
			}

			b.ReportAllocs()

			for b.Loop() {
				fetchAll2(b, ctx, e, req)
			}
		})
	}
}

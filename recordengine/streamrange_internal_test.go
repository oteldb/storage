package recordengine

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/signal"
)

func TestStreamInRangePrunesBeforeLookup(t *testing.T) {
	t.Parallel()
	id := signal.SeriesID{Lo: 1}
	for _, tt := range []struct {
		name                 string
		start, end           int64
		held, empty, foreign bool
		want                 bool
		lookups              int
		head, flushing       bool
	}{
		{name: "unbounded", want: true},
		{name: "before", start: 1, end: 99, held: true},
		{name: "after", start: 201, end: 300, held: true},
		{name: "touch lower", start: 1, end: 100, held: true, want: true, lookups: 1},
		{name: "touch upper", start: 200, end: 300, held: true, want: true, lookups: 1},
		{name: "contained", start: 125, end: 175, held: true, want: true, lookups: 1},
		{name: "containing", start: 50, end: 250, held: true, want: true, lookups: 1},
		{name: "not held", start: 125, end: 175, lookups: 1},
		{name: "empty range", start: 125, end: 175, held: true, empty: true, lookups: 1},
		{name: "foreign before", start: 1, end: 99, held: true, foreign: true},
		{name: "foreign inclusive", start: 200, end: 300, held: true, foreign: true, want: true, lookups: 1},
		{name: "head", start: 150, end: 150, head: true, want: true},
		{name: "flushing", start: 150, end: 150, flushing: true, want: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			p := &part{minTime: 100, maxTime: 200}
			if tt.held {
				end := 1
				if tt.empty {
					end = 0
				}
				p.ranges = []streamRange{{id: id, rowRange: rowRange{end: end}}}
			}
			e := New(Config{Schema: headTestSchema})
			if tt.head {
				e.head.records[id] = &recordCols{ts: []int64{150}, tsMin: 150, tsMax: 150}
			}
			if tt.flushing {
				e.flushing = map[signal.SeriesID]*recordCols{id: {ts: []int64{150}, tsMin: 150, tsMax: 150}}
			}
			if tt.foreign {
				e.foreignParts = map[string]*part{"p": p}
			} else {
				e.parts = []*part{p}
			}
			got, lookups := e.streamInRangeLookupsLocked(id, tt.start, tt.end)
			require.Equal(t, tt.want, got)
			require.Equal(t, tt.lookups, lookups)
			require.Equal(t, membershipFirst(e, id, tt.start, tt.end), got)
		})
	}
}

func membershipFirst(e *Engine, id signal.SeriesID, start, end int64) bool {
	if start == 0 && end == 0 {
		return true
	}
	if bufInRange(e.head.records[id], start, end) || bufInRange(e.flushing[id], start, end) {
		return true
	}
	for _, p := range e.readablePartsLocked() {
		if rng, ok := p.lookup(id); ok && rng.start < rng.end && p.maxTime >= start && p.minTime <= end {
			return true
		}
	}
	return false
}

func BenchmarkStreamInRangePruning(b *testing.B) {
	ranges := make([]streamRange, 1024)
	for i := range ranges {
		ranges[i] = streamRange{id: signal.SeriesID{Lo: uint64(i + 1)}, rowRange: rowRange{end: 1}}
	}
	id := signal.SeriesID{Lo: 1025}
	for _, live := range []struct {
		name  string
		count int
	}{{name: "narrow", count: 100}, {name: "all", count: 2289}} {
		b.Run(live.name, func(b *testing.B) {
			e := New(Config{Schema: headTestSchema})
			for i := range 2289 {
				p := &part{minTime: 1, maxTime: 99, ranges: ranges}
				if i < live.count {
					p.minTime, p.maxTime = 100, 200
				}
				e.parts = append(e.parts, p)
			}
			for _, variant := range []struct {
				name string
				fn   func(*Engine, signal.SeriesID, int64, int64) bool
			}{
				{name: "before", fn: membershipFirst},
				{name: "after", fn: (*Engine).streamInRangeLocked},
			} {
				b.Run(variant.name, func(b *testing.B) {
					b.ReportAllocs()
					for b.Loop() {
						if variant.fn(e, id, 100, 200) {
							b.Fatal("absent stream matched")
						}
					}
				})
			}
		})
	}
}

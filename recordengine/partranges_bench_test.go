package recordengine

import (
	"math/rand/v2"
	"slices"
	"testing"

	"github.com/oteldb/storage/signal"
)

// Stream resolution is what planning a fetch costs on a fragmented store: every (part, requested
// stream) pair. The shape here is the real log corpus's — hundreds of parts, a service with tens of
// thousands of streams — and the two sub-benchmarks are the two ways to resolve it: one hash probe
// per pair over a 16-byte key ("map", the shape this replaced) against a merge-join of two sorted
// lists ("mergejoin", [fetchPlan.sizeStreams]).
const (
	benchParts    = 664
	benchUniverse = 50_000
	benchPerPart  = 5_000
	benchAsked    = 16_728
)

type benchStreams struct {
	parts []*part
	maps  []map[signal.SeriesID]rowRange
	asked []signal.SeriesID
}

// newBenchStreams builds the part set. disjoint gives each part a slice of the id space of its own,
// with the request landing outside it — the fragmented store where most parts hold none of the
// queried service's streams, and what the min/max early-out in [fetchPlan.sizeStreams] is for.
func newBenchStreams(disjoint bool) *benchStreams {
	rng := rand.New(rand.NewPCG(1, 2))

	universe := make([]signal.SeriesID, benchUniverse)
	for i := range universe {
		universe[i] = signal.SeriesID{Hi: rng.Uint64(), Lo: rng.Uint64()}
	}

	b := &benchStreams{}

	slices.SortFunc(universe, signal.SeriesID.Compare)

	for pi := range benchParts {
		held := make([]signal.SeriesID, benchPerPart)
		for i := range held {
			if disjoint {
				// The high half of the id space, sliced per part; the request takes the low half.
				lo := benchUniverse/2 + pi*(benchUniverse/2/benchParts)
				held[i] = universe[min(lo+i%(benchUniverse/2/benchParts), benchUniverse-1)]
				continue
			}

			held[i] = universe[rng.IntN(len(universe))]
		}

		slices.SortFunc(held, signal.SeriesID.Compare)
		held = slices.Compact(held)

		ranges := make([]streamRange, 0, len(held))
		m := make(map[signal.SeriesID]rowRange, len(held))

		for i, id := range held {
			r := rowRange{start: i * 10, end: i*10 + 10}
			ranges = append(ranges, streamRange{id: id, rowRange: r})
			m[id] = r
		}

		b.parts = append(b.parts, &part{ranges: ranges})
		b.maps = append(b.maps, m)
	}

	asked := make([]signal.SeriesID, 0, benchAsked)
	for i := range benchAsked {
		if disjoint {
			asked = append(asked, universe[i%(benchUniverse/2)])
			continue
		}

		asked = append(asked, universe[i%len(universe)])
	}

	slices.SortFunc(asked, signal.SeriesID.Compare)
	b.asked = asked

	return b
}

// resolveMap is the pre-merge-join resolution: [part.holdsAny] over the requested ids to select the
// part, then one probe per (live part, id) pair to size each stream's accumulator.
func (b *benchStreams) resolveMap(rows []int) int {
	var live []int

	for pi := range b.parts {
		for _, id := range b.asked {
			if _, ok := b.maps[pi][id]; ok {
				live = append(live, pi)

				break
			}
		}
	}

	total := 0

	for k, id := range b.asked {
		for _, pi := range live {
			if rng, ok := b.maps[pi][id]; ok {
				rows[k] += rng.end - rng.start
				total += rng.end - rng.start
			}
		}
	}

	return total
}

func (b *benchStreams) resolveJoin(rows []int) int {
	p := &fetchPlan{sortedIDs: b.asked}

	for _, part := range b.parts {
		p.sizeStreams(part, rows)
	}

	total := 0
	for _, n := range rows {
		total += n
	}

	return total
}

func BenchmarkStreamResolution(b *testing.B) {
	for _, shape := range []struct {
		name     string
		disjoint bool
	}{
		{"shared", false},
		{"disjoint", true},
	} {
		b.Run(shape.name, func(b *testing.B) { benchStreamResolution(b, shape.disjoint) })
	}
}

func benchStreamResolution(b *testing.B, disjoint bool) {
	b.Helper()

	fx := newBenchStreams(disjoint)
	rows := make([]int, len(fx.asked))

	// Both resolutions must account for the same rows, or the comparison measures nothing.
	want := fx.resolveMap(rows)
	clear(rows)

	if got := fx.resolveJoin(rows); got != want {
		b.Fatalf("resolutions disagree: map %d, merge-join %d", want, got)
	}

	b.Run("map", func(b *testing.B) {
		b.ReportAllocs()

		for b.Loop() {
			clear(rows)
			fx.resolveMap(rows)
		}
	})

	b.Run("mergejoin", func(b *testing.B) {
		b.ReportAllocs()

		for b.Loop() {
			clear(rows)
			fx.resolveJoin(rows)
		}
	})
}

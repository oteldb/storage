package recordengine

import (
	"slices"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/oteldb/storage/internal/heaptest"
	"github.com/oteldb/storage/internal/mergestream"
	"github.com/oteldb/storage/signal"
)

// legacyIDSetOf is the map-then-sort union [mergeKeys] replaced. It stays as the reference the
// rewire is held to: the heap must yield exactly this sequence.
func legacyIDSetOf(parts []*part) []signal.SeriesID {
	set := make(map[signal.SeriesID]struct{})
	for _, p := range parts {
		for _, sr := range p.ranges {
			set[sr.id] = struct{}{}
		}
	}

	ids := make([]signal.SeriesID, 0, len(set))
	for id := range set {
		ids = append(ids, id)
	}

	slices.SortFunc(ids, signal.SeriesID.Compare)

	return ids
}

// idPart builds a part holding one row per id, with ranges ordered the way [buildRanges] leaves
// them: ascending by id, repeats adjacent.
func idPart(ids ...uint64) *part {
	p := &part{}
	slices.Sort(ids)

	for i, v := range ids {
		p.ranges = append(p.ranges, streamRange{
			id:       signal.SeriesID{Lo: v},
			rowRange: rowRange{start: i, end: i + 1},
		})
	}

	return p
}

func drainKeys(t *testing.T, src []*part) []signal.SeriesID {
	t.Helper()

	var k mergestream.Keys

	mergeKeys(src, &k)

	return k.Append(make([]signal.SeriesID, 0))
}

// TestMergeKeysMatchesTheSetItReplaced is the rewire's acceptance criterion. It includes a part
// holding the same id twice, which an unsorted stream column leaves behind ([buildRanges] groups
// runs, then sorts) and which the map deduplicated silently.
func TestMergeKeysMatchesTheSetItReplaced(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct {
		name string
		src  []*part
	}{
		{"none", nil},
		{"one part", []*part{idPart(1, 2, 3)}},
		{"identical parts", []*part{idPart(1, 2), idPart(1, 2)}},
		{"disjoint", []*part{idPart(1, 3, 5), idPart(2, 4, 6)}},
		{"nested", []*part{idPart(1, 2, 3, 4), idPart(2, 3)}},
		{"empty parts among full ones", []*part{idPart(), idPart(7), idPart(), idPart(1, 7)}},
		{"all empty", []*part{idPart(), idPart()}},
		{"repeated id within a part", []*part{idPart(4, 4, 4, 9), idPart(9, 9)}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, legacyIDSetOf(tt.src), drainKeys(t, tt.src))
		})
	}
}

// TestMergeKeysHoldsNoSet is what the rewire is for: the union no longer allocates a map and a
// sorted slice of every distinct stream, only the per-part cursors — O(parts), not O(streams).
//
//nolint:paralleltest // measures allocations; a parallel test's own allocations add noise.
func TestMergeKeysHoldsNoSet(t *testing.T) {
	const streams = 2048

	src := make([]*part, 4)
	for i := range src {
		ids := make([]uint64, streams)
		for j := range ids {
			ids[j] = uint64(j*len(src) + i)
		}

		src[i] = idPart(ids...)
	}

	var k mergestream.Keys

	heap := heaptest.BytesPerOp(func() {
		mergeKeys(src, &k)

		for k.Next() {
			_ = k.Key()
		}
	})

	assert.Less(t, heap, int64(1<<10))
	assert.Greater(t, heaptest.BytesPerOp(func() { _ = legacyIDSetOf(src) }), int64(streams*16), "the reference allocated per stream")
}

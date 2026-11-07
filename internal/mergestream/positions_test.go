package mergestream_test

import (
	"slices"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/internal/heaptest"
	"github.com/oteldb/storage/internal/mergestream"
	"github.com/oteldb/storage/signal"
)

func TestKeysPositions(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name string
		src  [][]signal.SeriesID
	}{
		{name: "empty"},
		{name: "empty sources", src: [][]signal.SeriesID{nil, ids(1, 2), nil}},
		{name: "overlap", src: [][]signal.SeriesID{ids(1, 3), ids(2, 3), ids(1, 2, 3)}},
		{name: "repeats", src: [][]signal.SeriesID{ids(1, 1, 3, 3), ids(1, 2, 2, 3)}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var k mergestream.Keys
			k.Reset(sourcesOf(tc.src))
			var positions []mergestream.Position
			var got []signal.SeriesID
			for {
				var ok bool
				positions, ok = k.NextPositions(positions)
				if !ok {
					break
				}
				key := k.Key()
				got = append(got, key)
				want := make(map[int]int)
				for i, source := range tc.src {
					if first := slices.Index(source, key); first >= 0 {
						want[i] = first
					}
				}
				actual := make(map[int]int)
				for _, pos := range positions {
					require.NotContains(t, actual, pos.Source)
					actual[pos.Source] = pos.Index
				}
				require.Equal(t, want, actual)
			}
			var reference mergestream.Keys
			reference.Reset(sourcesOf(tc.src))
			require.Equal(t, reference.Append(nil), got)
			require.Empty(t, positions)
		})
	}
}

//nolint:paralleltest // Measures steady-state allocations.
func TestKeysPositionsReuse(t *testing.T) {
	heaptest.SkipUnsupported(t)
	var k mergestream.Keys
	sources := sourcesOf([][]signal.SeriesID{ids(1, 3), ids(2, 3), nil})
	positions := make([]mergestream.Position, 0, len(sources))
	drain := func() {
		k.Reset(sources)
		for {
			var ok bool
			positions, ok = k.NextPositions(positions)
			if !ok {
				return
			}
		}
	}
	drain()
	require.Zero(t, testing.AllocsPerRun(10, drain))
}

package mergestream_test

import (
	"math/rand/v2"
	"slices"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/internal/mergestream"
	"github.com/oteldb/storage/internal/mergestream/mergestreamtest"
	"github.com/oteldb/storage/signal"
)

func ids(vs ...uint64) []signal.SeriesID {
	out := make([]signal.SeriesID, len(vs))
	for i, v := range vs {
		out[i] = signal.SeriesID{Lo: v}
	}

	return out
}

func sourcesOf(src [][]signal.SeriesID) []mergestream.Source {
	out := make([]mergestream.Source, len(src))
	for i, s := range src {
		out[i] = mergestream.SeriesIDs(s)
	}

	return out
}

func TestKeys(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct {
		name string
		src  [][]signal.SeriesID
		want []signal.SeriesID
	}{
		{"none", nil, nil},
		{"all empty", [][]signal.SeriesID{{}, {}}, nil},
		{"single", [][]signal.SeriesID{ids(1, 2, 3)}, ids(1, 2, 3)},
		{"disjoint", [][]signal.SeriesID{ids(1, 3), ids(2, 4)}, ids(1, 2, 3, 4)},
		{"identical", [][]signal.SeriesID{ids(1, 2), ids(1, 2), ids(1, 2)}, ids(1, 2)},
		{"repeats within a source", [][]signal.SeriesID{ids(1, 1, 1, 2), ids(2, 2)}, ids(1, 2)},
		{"empty source among full ones", [][]signal.SeriesID{{}, ids(5), {}, ids(1, 5)}, ids(1, 5)},
		{"nested", [][]signal.SeriesID{ids(1, 2, 3, 4), ids(2, 3)}, ids(1, 2, 3, 4)},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var k mergestream.Keys

			k.Reset(sourcesOf(tt.src))
			assert.Equal(t, tt.want, k.Append(nil))
			assert.False(t, k.Next(), "exhausted")
		})
	}
}

// TestKeysOrdersByHighWordFirst pins the union to [signal.SeriesID.Compare]'s order rather than to
// the low word, which a 128-bit key sorted as two independent numbers would get wrong.
func TestKeysOrdersByHighWordFirst(t *testing.T) {
	t.Parallel()

	src := [][]signal.SeriesID{
		{{Hi: 0, Lo: 9}, {Hi: 2, Lo: 1}},
		{{Hi: 1, Lo: 7}, {Hi: 2, Lo: 0}},
	}

	var k mergestream.Keys

	k.Reset(sourcesOf(src))
	assert.Equal(t, []signal.SeriesID{{Hi: 0, Lo: 9}, {Hi: 1, Lo: 7}, {Hi: 2, Lo: 0}, {Hi: 2, Lo: 1}}, k.Append(nil))
}

func TestKeysResetReuse(t *testing.T) {
	t.Parallel()

	var k mergestream.Keys

	k.Reset(sourcesOf([][]signal.SeriesID{ids(1, 2, 3)}))
	require.True(t, k.Next())
	assert.Equal(t, signal.SeriesID{Lo: 1}, k.Key())

	// Re-arming mid-traversal drops the rest rather than resuming it.
	k.Reset(sourcesOf([][]signal.SeriesID{ids(7), ids(8)}))
	assert.Equal(t, ids(7, 8), k.Append(nil))
}

func TestKeysMatchesNaiveUnion(t *testing.T) {
	t.Parallel()

	rng := rand.New(rand.NewPCG(1, 2))
	for range 200 {
		src := randomSources(rng, 1+rng.IntN(6), rng.IntN(40))
		mergestreamtest.CheckKeys(t, src)
	}
}

// TestKeysHoldsNoSet is the point of the type: a reused Keys allocates nothing per traversal, and
// nothing that grows with the distinct key count — the map and sorted slice it replaces were both
// O(distinct keys).
//
//nolint:paralleltest // measures allocations; a parallel test's own allocations add noise.
func TestKeysHoldsNoSet(t *testing.T) {
	for _, n := range []int{64, 4096} {
		src := ascendingSources(4, n)
		sources := sourcesOf(src)

		var (
			k    mergestream.Keys
			last signal.SeriesID
		)

		drain := func() {
			k.Reset(sources)
			for k.Next() {
				last = k.Key()
			}
		}

		drain()
		require.Equal(t, signal.SeriesID{Lo: uint64(4*n - 1)}, last)
		assert.Zero(t, testing.AllocsPerRun(10, drain), "%d keys per source", n)
	}
}

func ascendingSources(n, per int) [][]signal.SeriesID {
	src := make([][]signal.SeriesID, n)
	for i := range src {
		src[i] = make([]signal.SeriesID, per)
		for j := range src[i] {
			src[i][j] = signal.SeriesID{Lo: uint64(j*n + i)}
		}
	}

	return src
}

func randomSources(rng *rand.Rand, n, per int) [][]signal.SeriesID {
	src := make([][]signal.SeriesID, n)
	for i := range src {
		for range per {
			src[i] = append(src[i], signal.SeriesID{Hi: uint64(rng.IntN(3)), Lo: uint64(rng.IntN(16))})
		}

		slices.SortFunc(src[i], signal.SeriesID.Compare)
	}

	return src
}

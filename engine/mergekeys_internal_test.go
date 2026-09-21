package engine

import (
	"context"
	"slices"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/internal/mergestream"
	"github.com/oteldb/storage/internal/obs"
	"github.com/oteldb/storage/signal"
)

// legacySortedSeriesIDs is the map-then-sort union [mergeKeys] replaced. It stays as the reference
// the rewire is held to: the heap must yield exactly this sequence.
func legacySortedSeriesIDs(ctx context.Context, src []*part) ([]signal.SeriesID, error) {
	idSet := make(map[signal.SeriesID]struct{})

	for _, p := range src {
		if err := p.index.forEachID(ctx, func(id signal.SeriesID) { idSet[id] = struct{}{} }); err != nil {
			return nil, err
		}
	}

	ids := make([]signal.SeriesID, 0, len(idSet))
	for id := range idSet {
		ids = append(ids, id)
	}

	slices.SortFunc(ids, signal.SeriesID.Compare)

	return ids, nil
}

func residentPart(t *testing.T, runLens []int) *part {
	t.Helper()

	return &part{index: buildPartIndex(sidxColumn(runLens))}
}

func pagedPart(t *testing.T, prefix string, runLens []int) (*part, backend.Backend) {
	t.Helper()

	ctx := context.Background()
	col := sidxColumn(runLens)
	be := backend.Memory()
	require.NoError(t, be.Write(ctx, sidxKey(prefix), encodeSeriesIndex(col)))

	paged, ok := openPagedIndex(ctx, be, prefix, len(col), obs.NewNop().Corruption)
	require.True(t, ok)

	return &part{index: partIndex{paged: paged}}, be
}

func drainKeys(ctx context.Context, t *testing.T, src []*part) []signal.SeriesID {
	t.Helper()

	var k mergestream.Keys

	require.NoError(t, mergeKeys(ctx, src, &k))

	return k.Append(make([]signal.SeriesID, 0))
}

// TestMergeKeysMatchesTheSetItReplaced is the rewire's acceptance criterion: over every mix of
// overlapping, nested, disjoint and empty parts the heap yields the same ids in the same order as
// the map-and-sort union it replaced.
func TestMergeKeysMatchesTheSetItReplaced(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	for _, tt := range []struct {
		name string
		runs [][]int
	}{
		{"none", nil},
		{"one part", [][]int{{1, 2, 3}}},
		{"identical parts", [][]int{{1, 1, 1}, {1, 1, 1}}},
		{"nested", [][]int{{1, 1, 1, 1, 1}, {1, 1}}},
		{"empty parts among full ones", [][]int{{}, {1, 1, 1}, {}, {1, 1, 1, 1}}},
		{"all empty", [][]int{{}, {}}},
		{"long runs", [][]int{{9, 1, 4}, {1, 9}, {2, 2, 2, 2}}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			src := make([]*part, len(tt.runs))
			for i, runs := range tt.runs {
				src[i] = residentPart(t, runs)
			}

			want, err := legacySortedSeriesIDs(ctx, src)
			require.NoError(t, err)
			assert.Equal(t, want, drainKeys(ctx, t, src))
		})
	}
}

// TestMergeKeysReadsAPagedIndexInPlace covers the second index form: a merge over a part whose
// index lives in the sidecar must see the same ids without materializing them.
func TestMergeKeysReadsAPagedIndexInPlace(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	pagedA, _ := pagedPart(t, "a", []int{3, 1, 2, 7})
	pagedB, _ := pagedPart(t, "b", []int{1, 1})
	src := []*part{pagedA, residentPart(t, []int{2, 2, 2}), pagedB}

	want, err := legacySortedSeriesIDs(ctx, src)
	require.NoError(t, err)
	assert.Equal(t, want, drainKeys(ctx, t, src))
}

func TestMergeKeysPropagatesASidecarFailure(t *testing.T) {
	t.Parallel()

	p, be := pagedPart(t, "a", []int{1, 2})
	p.index.dropView()
	require.NoError(t, be.Delete(context.Background(), sidxKey("a")))

	var k mergestream.Keys

	require.ErrorIs(t, mergeKeys(context.Background(), []*part{p}, &k), backend.ErrNotExist)
}

// TestMergeKeysHoldsNoSet is what the rewire is for: the union no longer allocates a map and a
// sorted slice of every distinct series, only the per-part cursors — O(parts), not O(series).
//
//nolint:paralleltest // measures allocations; a parallel test's own allocations add noise.
func TestMergeKeysHoldsNoSet(t *testing.T) {
	ctx := context.Background()

	const series = 2048

	src := make([]*part, 4)
	for i := range src {
		src[i] = residentPart(t, slices.Repeat([]int{1}, series))
	}

	var k mergestream.Keys

	heap := allocBytes(func() {
		require.NoError(t, mergeKeys(ctx, src, &k))

		for k.Next() {
			_ = k.Key()
		}
	})

	set := allocBytes(func() {
		_, err := legacySortedSeriesIDs(ctx, src)
		require.NoError(t, err)
	})

	assert.Less(t, heap, int64(1<<10))
	assert.Greater(t, set, int64(series*16), "the reference allocated per series")
}

func allocBytes(fn func()) int64 {
	return testing.Benchmark(func(b *testing.B) {
		b.Helper()
		b.ReportAllocs()

		for b.Loop() {
			fn()
		}
	}).AllocedBytesPerOp()
}

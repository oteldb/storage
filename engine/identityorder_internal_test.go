package engine

import (
	"context"
	"slices"
	"strconv"
	"testing"

	"github.com/go-faster/errors"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/index/series"
	"github.com/oteldb/storage/signal"
)

func orderSeries(i int) signal.Series {
	return signal.Series{Attributes: signal.NewAttributes(signal.KeyValue{
		Key: []byte("order"), Value: signal.IntValue(int64(i)),
	})}
}

func orderIDs(t *testing.T, view series.View, runs [][]uint32) []signal.SeriesID {
	t.Helper()
	out := make([]signal.SeriesID, 0, view.Len())
	require.NoError(t, walkIdentityOrder(t.Context(), view, runs, func(id signal.SeriesID, ref uint32) error {
		require.Equal(t, id, view.ID(int(ref)))
		out = append(out, id)
		return nil
	}))
	return out
}

func TestIdentityOrderIncrementalAndReplacement(t *testing.T) {
	t.Parallel()
	index := series.New()
	var order identityOrder
	reserve := func(int64) error { return nil }
	var want []signal.SeriesID
	for _, count := range []int{17, 31, 113, 257} {
		for index.Len() < count {
			want = append(want, index.Add(orderSeries(index.Len())))
		}
		require.NoError(t, order.update(t.Context(), index, index.View(), reserve))
		sorted := slices.Clone(want)
		slices.SortFunc(sorted, signal.SeriesID.Compare)
		require.Equal(t, sorted, orderIDs(t, index.View(), order.runs))
		require.Equal(t, int64(count)*4, order.bytes.Load())
	}
	replacement := series.New()
	for i := range slices.Backward(want) {
		replacement.Add(orderSeries(i))
	}
	require.NoError(t, order.update(t.Context(), replacement, replacement.View(), reserve))
	slices.SortFunc(want, signal.SeriesID.Compare)
	require.Equal(t, want, orderIDs(t, replacement.View(), order.runs))
	order.reset()
	require.Zero(t, order.bytes.Load())
	require.Nil(t, order.index)
	require.Empty(t, order.runs)
}

func TestIdentityOrderBudgetFailureCanResume(t *testing.T) {
	t.Parallel()
	index := series.New()
	for i := range 2 * identityOrderBatch {
		index.Add(orderSeries(i))
	}
	var order identityOrder
	budgetErr := errors.New("order reservation")
	err := order.update(t.Context(), index, index.View(), func(bytes int64) error {
		if bytes > 4*identityOrderBatch {
			return budgetErr
		}
		return nil
	})
	require.ErrorIs(t, err, budgetErr)
	require.NoError(t, order.update(t.Context(), index, index.View(), func(int64) error { return nil }))
	got := orderIDs(t, index.View(), order.runs)
	require.Len(t, got, index.Len())
	require.True(t, slices.IsSortedFunc(got, signal.SeriesID.Compare))
	require.Len(t, slices.Compact(got), index.Len())
}

func TestPrepareDetachedOrderAllowsRegistration(t *testing.T) {
	t.Parallel()
	for _, selectedCount := range []int{1, 31, 128} {
		t.Run(strconv.Itoa(selectedCount), func(t *testing.T) {
			t.Parallel()
			e := New(Config{})
			selected := make(map[signal.SeriesID]*sampleBuf)
			for i := range 128 {
				s := orderSeries(i)
				e.head.registerSeries(s)
				if i < selectedCount {
					selected[s.Hash()] = &sampleBuf{}
				}
			}
			order, err := e.prepareDetachedOrder(t.Context(), selected, func(int64) error {
				e.mu.LockWriter()
				e.head.registerSeries(orderSeries(e.head.series.Len()))
				e.mu.UnlockWriter()
				return nil
			})
			require.NoError(t, err)
			got := orderIDs(t, order.view, order.runs)
			var matched []signal.SeriesID
			for _, id := range got {
				if _, ok := selected[id]; ok {
					matched = append(matched, id)
				}
			}
			require.Len(t, matched, selectedCount)
			require.True(t, slices.IsSortedFunc(matched, signal.SeriesID.Compare))
		})
	}
}

func TestStreamedHeadPartsFollowGlobalIDOrder(t *testing.T) {
	t.Parallel()
	e := New(Config{Backend: backend.Memory(), Prefix: "ordered", MaxPartBytes: 17 * partRowBytes,
		FlushMemoryBytes: 9 << 20})
	for i := range 128 {
		_, err := e.Append(orderSeries(i), 1, float64(i))
		require.NoError(t, err)
	}
	require.NoError(t, e.Flush(t.Context()))
	require.Greater(t, len(e.parts), 1)
	var previous signal.SeriesID
	for i, part := range e.parts {
		p := part.index.paged
		require.NotNil(t, p)
		require.True(t, p.bounded)
		if i > 0 {
			require.LessOrEqual(t, previous.Compare(p.first), 0)
		}
		previous = p.last
	}
	require.NoError(t, e.Reset(t.Context()))
	require.Zero(t, e.identityOrder.bytes.Load())
}

func TestIdentityOrderCancellation(t *testing.T) {
	t.Parallel()
	index := series.New()
	index.Add(orderSeries(1))
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	var order identityOrder
	require.ErrorIs(t, order.update(ctx, index, index.View(), func(int64) error { return nil }), context.Canceled)
	require.Zero(t, order.known)
}

func FuzzIdentityOrderMerge(f *testing.F) {
	for _, count := range []uint16{0, 1, 17, 64, 257, 1023} {
		f.Add(count, uint16(7))
	}
	f.Fuzz(func(t *testing.T, count, width uint16) {
		index := series.New()
		want := make([]signal.SeriesID, 0, int(count%1024))
		for i := range int(count % 1024) {
			want = append(want, index.Add(orderSeries(i)))
		}
		view := index.View()
		var runs [][]uint32
		step := 1 + int(width%127)
		for i := 0; i < view.Len(); i += step {
			refs := make([]uint32, min(step, view.Len()-i))
			for j := range refs {
				refs[j] = uint32(i + j)
			}
			sortIdentityRefs(view, refs)
			runs = append(runs, refs)
		}
		slices.SortFunc(want, signal.SeriesID.Compare)
		require.Equal(t, want, orderIDs(t, view, runs))
	})
}

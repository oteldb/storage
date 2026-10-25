package engine

import (
	"context"
	"fmt"
	"slices"
	"testing"

	"github.com/oteldb/storage/backend"

	"github.com/stretchr/testify/require"
)

func TestSelectWidePartsInCurrentBucket(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name   string
		lo, hi int64
	}{
		{name: "six hours", lo: hour + hour/2, hi: 3 * hour},
		{name: "day", lo: 16 * hour, hi: 20 * hour},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			lo, hi := tc.lo, tc.hi
			a, b := partAt(0, 4096, lo, hi), partAt(1, 4096, lo+1, hi+hour/60)
			fine := partAt(2, 4096, hi+2*hour/60, hi+3*hour/60)
			src := []*part{a, fine, b}
			require.ElementsMatch(t, []*part{a, b}, selectMergeParts(src, MergeOptions{}, 1<<30, 0), "already-wide parts must not wait for another bucket's data")
			require.ElementsMatch(t, []*part{a, b}, selectMergeParts(src, MergeOptions{Force: true}, 1<<30, mergeIdleRounds), "forcing retains the same time-locality rule")
		})
	}
}

func TestCurrentBucketSelectionBounds(t *testing.T) {
	t.Parallel()
	src := make([]*part, 40)
	for i := range src {
		src[i] = partAt(i, 4096, 16*hour, 20*hour+int64(i))
	}
	for _, tc := range []struct {
		name string
		cap  int64
		want int
	}{
		{name: "unlimited bytes retains count cap", cap: 0, want: 16},
		{name: "roomy disk retains count cap", cap: 1 << 30, want: 16},
		{name: "disk bounds group", cap: 16 << 10, want: 4},
		{name: "disk bounds pair", cap: 8 << 10, want: 2},
		{name: "sealed parts excluded", cap: 4 << 10, want: 0},
	} {
		for _, force := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/force=%t", tc.name, force), func(t *testing.T) {
				t.Parallel()
				idle := 0
				if force {
					idle = mergeIdleRounds
				}
				got := selectMergeParts(src, MergeOptions{Force: force}, tc.cap, idle)
				require.Len(t, got, tc.want)
				var total int64
				for _, p := range got {
					total += p.sizeBytes()
				}
				if tc.cap > 0 {
					require.LessOrEqual(t, total, tc.cap)
				}
			})
		}
	}
}

func TestCurrentBucketMergeConverges(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	admitted := false
	admissions := 0
	e := New(Config{
		Backend: backend.Memory(), Prefix: "current/metrics", MergeCeilingBytes: 1 << 20, MergeMemoryBytes: 16 << 20,
		MergeAdmission: func(_ context.Context, _ int64, wait bool) (func(), bool, error) {
			require.False(t, wait)
			admissions++
			return func() {}, admitted, nil
		},
	})
	want := make([]int64, 0, 70)
	flush := func(lo, hi int64) { t.Helper(); flushEvery(t, e, lo, hi+1, hi-lo, 1); want = append(want, lo, hi) }
	const parts = 33
	for i := range parts {
		flush(day+16*hour+int64(i), day+20*hour+int64(i))
	}
	flush(day+20*hour+2*hour/60, day+20*hour+3*hour/60)
	fine := e.parts[len(e.parts)-1].prefix
	before := e.PartPrefixes()
	changed, err := e.MergeStep(ctx, MergeOptions{Background: true})
	require.NoError(t, err)
	require.False(t, changed)
	require.True(t, e.MergeDeferred())
	require.Equal(t, before, e.PartPrefixes())
	admitted = true
	for range 8 {
		require.LessOrEqual(t, e.MergeShape().Candidates, maxMergeParts)
		changed, err = e.MergeStep(ctx, MergeOptions{Background: true})
		require.NoError(t, err)
		if e.PartCount() == 2 {
			break
		}
		require.True(t, changed, "wide parts must continue compacting")
	}
	require.Equal(t, 2, e.PartCount(), "wide parts collapse without promoting the fine singleton")
	require.Contains(t, e.PartPrefixes(), fine)
	stable := e.PartPrefixes()
	for range mergeIdleRounds + 1 {
		changed, err = e.MergeStep(ctx, MergeOptions{Background: true})
		require.NoError(t, err)
		require.False(t, changed)
		require.Equal(t, stable, e.PartPrefixes(), "settled maintenance must not rewrite its fixed point")
	}
	flush(2*day+hour, 2*day+hour+1)
	changed, err = e.MergeStep(ctx, MergeOptions{Background: true})
	require.NoError(t, err)
	require.True(t, changed)
	require.NotContains(t, e.PartPrefixes(), fine, "newer data allows ordinary promotion of the old fine part")
	require.Equal(t, 2, e.PartCount())
	ts, values := rollupSamples(t, e)
	slices.Sort(want)
	require.Equal(t, want, ts)
	for _, v := range values {
		require.InDelta(t, 1, v, 0)
	}
	require.Greater(t, admissions, 1, "wide-part compaction must use normal admission")
}

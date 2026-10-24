package engine

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/backend/bucketindex"
	"github.com/oteldb/storage/signal"
)

func TestMergeShapeIncludesUnopenedLayouts(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	e := New(Config{Backend: backend.Memory(), Prefix: "t/metrics", MergeMemoryBytes: -1})
	flushEvery(t, e, 100, 105, 1, 1)
	opts := MergeOptions{Downsample: []DownsampleTier{{Before: 10000, Interval: 1000, Agg: signal.AggSum}}}
	changed, err := e.MergeStep(ctx, opts)
	require.NoError(t, err)
	require.True(t, changed)
	e.mu.Lock()
	e.foreign = append(e.foreign, bucketindex.Entry{
		Prefix: "t/metrics/unopened", MinTime: 0, MaxTime: 1000,
		Rollup: &bucketindex.Rollup{Tiers: []bucketindex.RollupTier{{Before: 10000, Interval: 1000, Agg: uint8(signal.AggCount)}}},
	})
	e.mu.Unlock()

	opts.Downsample[0].Interval = 2000
	before := e.PartPrefixes()
	changed, err = e.MergeStep(ctx, opts)
	require.NoError(t, err)
	assert.False(t, changed, "a successful no-op must not reschedule catch-up")
	assert.Equal(t, before, e.PartPrefixes(), "the unreadable layout prevents widening the rollup")
	assert.Zero(t, e.MergeShapeWith(opts).Candidates, "candidate reporting must use the same layout guard")
}

func TestMergeStepProgress(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name              string
		parts             int
		retain            int64
		refuse            bool
		failure           error
		changed, deferred bool
	}{
		{name: "empty"},
		{name: "compaction", parts: 2, changed: true},
		{name: "admission deferred", parts: 2, refuse: true, deferred: true},
		{name: "retention before deferral", parts: 3, retain: 50, refuse: true, changed: true, deferred: true},
		{name: "admission error", parts: 2, failure: context.Canceled},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			e := New(Config{Backend: backend.Memory(), Prefix: "t/metrics", MergeMemoryBytes: -1})
			for i := range tc.parts {
				from := int64(i*100 + 1)
				flushEvery(t, e, from, from+2, 1, 1)
			}
			e.cfg.MergeAdmission = func(context.Context, int64, bool) (func(), bool, error) {
				return func() {}, !tc.refuse, tc.failure
			}
			before := e.PartPrefixes()
			changed, err := e.MergeStep(ctx, MergeOptions{RetainFrom: tc.retain, Background: true})
			if tc.failure != nil {
				require.ErrorIs(t, err, tc.failure)
			} else {
				require.NoError(t, err)
			}
			assert.Equal(t, tc.changed, changed)
			assert.Equal(t, tc.deferred, e.MergeDeferred())
			if !changed {
				assert.Equal(t, before, e.PartPrefixes())
			}
		})
	}
}

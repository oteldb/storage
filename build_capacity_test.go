package storage

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestIndependentBuildCapacity(t *testing.T) {
	t.Parallel()
	const mib = 1 << 20
	for _, tc := range []struct {
		name         string
		build, flush int64
		parallel     bool
	}{
		{name: "legacy"},
		{name: "larger pool", build: 2048 * mib, parallel: true},
		{name: "explicit job target", build: 2048 * mib, flush: 400 * mib, parallel: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			s, err := Open(t.Context(), Options{MergeMemoryBytes: 1792 * mib,
				BuildMemoryBytes: tc.build, MetricFlushMemoryBytes: tc.flush})
			require.NoError(t, err)
			defer func() { require.NoError(t, s.Close(context.Background())) }()
			require.EqualValues(t, 1344*mib, s.mergeMemoryBytes())
			wantFlush := int64(448 * mib)
			if tc.flush != 0 {
				wantFlush = tc.flush
			}
			require.Equal(t, wantFlush, s.metricFlushMemoryBytes())
			merge, ok, err := s.admitMerge(t.Context(), 1344*mib, false)
			require.NoError(t, err)
			require.True(t, ok)
			defer merge()
			flush, ok := s.tryFlushAdmission()(506 * mib)
			require.Equal(t, tc.parallel, ok)
			if ok {
				flush()
			}
			merge()
			release, ok := s.buildPool.TryAcquire(s.buildPool.Total())
			require.True(t, ok)
			release()
		})
	}
}

func TestInvalidBuildCapacity(t *testing.T) {
	t.Parallel()
	for _, opts := range []Options{
		{BuildMemoryBytes: -1}, {MetricFlushMemoryBytes: -1},
		{BuildMemoryBytes: 16, MergeMemoryBytes: 128},
		{BuildMemoryBytes: 128, MergeMemoryBytes: -1},
		{BuildMemoryBytes: 128, MergeMemoryBytes: 128, MetricFlushMemoryBytes: 256},
	} {
		_, err := Open(t.Context(), opts)
		require.Error(t, err)
	}
}

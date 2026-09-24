package storage

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend/bucketindex"
	"github.com/oteldb/storage/internal/partid"
)

// TestMaintainRetriesFencedLoad: an owner reloads its index only after a backfill, so the
// maintenance cycle itself must retry a failed load, or one failure refuses its commits for good.
func TestMaintainRetriesFencedLoad(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s, err := InMemory()
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close(ctx) })

	write := func() {
		t.Helper()

		_, err := s.WriteMetrics(ctx, gaugeBatch("api", "m", []int64{time.Now().UnixNano()}, []float64{1}))
		require.NoError(t, err)
	}

	write()
	s.maintain(ctx)

	eng, ok := s.lookupEngine(defaultTenant)
	require.True(t, ok)

	be := s.backendFor(defaultTenant)
	prefix := string(defaultTenant) + metricsPrefix
	key := prefix + "/" + bucketindex.Object
	bad := prefix + "/" + partid.New().String()

	require.NoError(t, be.Write(ctx, bad+"/manifest", []byte("not a manifest")))

	ix, version, err := bucketindex.LoadVersioned(ctx, be, key)
	require.NoError(t, err)
	ix.Add(bucketindex.Entry{Prefix: bad, MinTime: 1, MaxTime: 2})
	_, err = ix.Save(ctx, be, key, version)
	require.NoError(t, err)

	require.Error(t, eng.LoadParts(ctx))
	require.True(t, metricStat(t, s).IndexFenced)

	write()
	s.maintain(ctx)

	st := metricStat(t, s)
	assert.True(t, st.IndexFenced, "a part that stays unreadable keeps the engine fenced")
	assert.Positive(t, st.HeadItems, "and its head unflushed")

	require.NoError(t, be.Delete(ctx, bad+"/manifest"))
	s.maintain(ctx)

	st = metricStat(t, s)
	assert.False(t, st.IndexFenced, "the cycle's retry lifted the fence")
	assert.Zero(t, st.HeadItems, "and the flush it held back landed")
}

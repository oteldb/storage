package storage

import (
	"context"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/signal"
	"github.com/oteldb/storage/tenant"
)

// TestFacadeMaxPartSizeSplits proves tenant.Limits.MaxPartSize is wired into the engine: a flush of
// many series produces several parts, each bounded by the cap, observable via Inspect.
func TestFacadeMaxPartSizeSplits(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s, err := InMemory(WithTenancy(tenant.ResolverFunc(func(signal.TenantID) tenant.Policy {
		return tenant.Policy{Limits: tenant.Limits{MaxPartSize: 160}} // ~5 rows per part
	})))
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close(ctx) })

	// 20 distinct metric series (one sample each).
	for i := range 20 {
		_, werr := s.WriteMetrics(ctx, gaugeBatch("svc", "m"+strconv.Itoa(i), []int64{int64(i + 1)}, []float64{1}))
		require.NoError(t, werr)
	}

	require.NoError(t, s.Admin().Flush(ctx, "default", signal.Metric))

	st, _ := findSignal(mustTenant(t, s.Inspect(), "default"), signal.Metric)
	assert.Greater(t, st.Parts, 1, "MaxPartSize split the flush into multiple parts")
	assert.Equal(t, int64(20), st.Series, "all series retained across the split parts")
}

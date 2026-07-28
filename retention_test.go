package storage

import (
	"context"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/signal"
	"github.com/oteldb/storage/tenant"
)

func TestSizeRetentionCutoff(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		parts    []sizedPart
		maxBytes int64
		want     int64
	}{
		{"no budget", []sizedPart{{maxTime: 10, bytes: 100}}, 0, 0},
		{"negative budget", []sizedPart{{maxTime: 10, bytes: 100}}, -1, 0},
		{"no parts", nil, 100, 0},
		{"under budget", []sizedPart{{maxTime: 10, bytes: 40}, {maxTime: 20, bytes: 50}}, 100, 0},
		{"exactly at budget", []sizedPart{{maxTime: 10, bytes: 50}, {maxTime: 20, bytes: 50}}, 100, 0},
		{
			"drops the oldest part",
			[]sizedPart{{maxTime: 10, bytes: 50}, {maxTime: 20, bytes: 50}},
			50,
			11,
		},
		{
			"drops as many oldest parts as needed",
			[]sizedPart{{maxTime: 10, bytes: 50}, {maxTime: 20, bytes: 50}, {maxTime: 30, bytes: 50}},
			50,
			21,
		},
		{
			// The input order is arbitrary (engine maps): the cutoff is by time, not by position.
			"orders by time, not input order",
			[]sizedPart{{maxTime: 30, bytes: 50}, {maxTime: 10, bytes: 50}, {maxTime: 20, bytes: 50}},
			120,
			11,
		},
		{
			// The newest part is never dropped: an impossible budget degrades to keeping it.
			"budget below one part keeps the newest",
			[]sizedPart{{maxTime: 10, bytes: 50}, {maxTime: 20, bytes: 50}},
			10,
			11,
		},
		{"single part over budget is kept", []sizedPart{{maxTime: 10, bytes: 100}}, 10, 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.want, sizeRetentionCutoff(tt.parts, tt.maxBytes))
		})
	}
}

func TestMaintainAppliesSizeRetention(t *testing.T) {
	t.Parallel()

	// The budget is set after the first parts are written, so it is sized from real parts rather
	// than guessed (part bytes depend on the codecs chosen at flush). MaxPartSize keeps the parts
	// bounded, which is what makes a byte budget meaningful: without it a merge can fold the whole
	// history into one (undroppable) part.
	var budget atomic.Int64

	s, err := InMemory(WithTenancy(tenant.ResolverFunc(func(signal.TenantID) tenant.Policy {
		return tenant.Policy{
			Limits:    tenant.Limits{MaxPartSize: 160}, // ~10 samples per part
			Retention: tenant.Retention{MaxBytes: budget.Load()},
		}
	})))
	require.NoError(t, err)

	ctx := context.Background()
	now := time.Now().UnixNano()

	const (
		rows = 200
		step = int64(time.Second)
	)

	write := func(start int64) {
		t.Helper()

		ts, vals := make([]int64, rows), make([]float64, rows)
		for i := range ts {
			ts[i] = start + int64(i)*step
			vals[i] = float64(i)
		}

		_, err := s.WriteMetrics(ctx, gaugeBatch("api", "m", ts, vals))
		require.NoError(t, err)
	}

	oldest := now - time.Hour.Nanoseconds()
	write(oldest)
	s.maintain(ctx)

	eng := mustEngine(s.engineFor("default"))
	require.Greater(t, eng.PartCount(), 1, "MaxPartSize split the flush into several parts")

	// Budget the first half of what is stored so far: once the same amount is written again, the
	// oldest parts must go.
	eff, err := s.EfficiencyStats(ctx)
	require.NoError(t, err)
	require.Len(t, eff, 1)
	require.Len(t, eff[0].Signals, 1)
	require.Positive(t, eff[0].Signals[0].StoredBytes)
	budget.Store(eff[0].Signals[0].StoredBytes)

	write(now - rows*step)
	s.maintain(ctx) // flushes the new parts (the budget was still met when the cycle started)
	s.maintain(ctx) // over budget now: drops oldest-first

	it, err := eng.Fetch(ctx, fetch.Request{Start: 0, End: now + 1, Matchers: []fetch.Matcher{nameMatcher("m")}})
	require.NoError(t, err)
	batches, err := fetch.Drain(ctx, it)
	require.NoError(t, err)
	require.Len(t, batches, 1)

	got := batches[0].Timestamps
	require.NotEmpty(t, got)
	assert.Greater(t, got[0], oldest, "the oldest samples were dropped by the byte budget")
	assert.Equal(t, now-step, got[len(got)-1], "the newest samples are retained")

	eff, err = s.EfficiencyStats(ctx)
	require.NoError(t, err)
	assert.LessOrEqual(t, eff[0].Signals[0].StoredBytes, budget.Load(), "retained bytes are back under budget")
}

func TestMaintainAppliesSizeRetentionToLogs(t *testing.T) {
	t.Parallel()

	var budget atomic.Int64

	s, err := InMemory(WithTenancy(tenant.ResolverFunc(func(signal.TenantID) tenant.Policy {
		return tenant.Policy{
			Limits:    tenant.Limits{MaxPartSize: 10 << 10}, // ~10 records per part
			Retention: tenant.Retention{MaxBytes: budget.Load()},
		}
	})))
	require.NoError(t, err)

	ctx := context.Background()

	write := func(from int) {
		t.Helper()

		recs := make([][3]any, 0, 200)
		for i := from; i < from+200; i++ {
			recs = append(recs, [3]any{i, 9, "body-" + strconv.Itoa(i)})
		}

		_, err := s.WriteLogs(ctx, logBatch("api", recs...))
		require.NoError(t, err)
	}

	write(1)
	s.maintain(ctx)

	eff, err := s.EfficiencyStats(ctx)
	require.NoError(t, err)
	require.Len(t, eff, 1)
	require.Len(t, eff[0].Signals, 1)
	require.Positive(t, eff[0].Signals[0].StoredBytes)
	budget.Store(eff[0].Signals[0].StoredBytes)

	write(1001)
	s.maintain(ctx)
	s.maintain(ctx)

	eng, ok := s.lookupRecordEngine(signal.Log, "default")
	require.True(t, ok)

	bodies := logBodies(t, eng, fetch.Request{Start: 0, End: 1 << 40, Matchers: []fetch.Matcher{logSvcMatcher("api")}})
	require.NotEmpty(t, bodies)
	assert.NotContains(t, bodies, "body-1", "the oldest log records were dropped by the byte budget")
	assert.Contains(t, bodies, "body-1200", "the newest log records are retained")
}

func TestMaintainNoSizeRetentionWithoutBudget(t *testing.T) {
	t.Parallel()

	s, err := InMemory()
	require.NoError(t, err)

	ctx := context.Background()
	now := time.Now().UnixNano()
	old := now - time.Hour.Nanoseconds()

	_, err = s.WriteMetrics(ctx, gaugeBatch("api", "m", []int64{old, now}, []float64{1, 2}))
	require.NoError(t, err)
	s.maintain(ctx)
	s.maintain(ctx)

	assert.Empty(t, s.sizeCutoffs(ctx, map[signal.TenantID]struct{}{"default": {}}),
		"no MaxBytes policy ⇒ no size cutoff (and no part-size I/O)")

	eng := mustEngine(s.engineFor("default"))

	it, err := eng.Fetch(ctx, fetch.Request{Start: 0, End: now + 1, Matchers: []fetch.Matcher{nameMatcher("m")}})
	require.NoError(t, err)
	batches, err := fetch.Drain(ctx, it)
	require.NoError(t, err)
	require.Len(t, batches, 1)
	assert.Equal(t, []int64{old, now}, batches[0].Timestamps, "everything is retained")
}

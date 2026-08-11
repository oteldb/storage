package storage

import (
	"context"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/backend/file"
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

// listCountingBackend counts the List calls the part-size enumeration makes.
type listCountingBackend struct {
	backend.Backend

	lists atomic.Int64
}

func (b *listCountingBackend) List(ctx context.Context, prefix string) ([]string, error) {
	b.lists.Add(1)

	return b.Backend.List(ctx, prefix)
}

// TestSizeCutoffsSkipsUnchangedPartSet pins the memoization: the cutoff is a pure function of the
// part set and the budget, so a cycle that follows one with no flush, merge, or delete in between
// must not re-enumerate part sizes on the backend.
func TestSizeCutoffsSkipsUnchangedPartSet(t *testing.T) {
	t.Parallel()

	be := &listCountingBackend{Backend: backend.Memory()}
	s, err := Open(context.Background(), Options{},
		WithBackend(be),
		WithFlushInterval(-1), // no background loop: this test drives maintain itself
		WithTenancy(tenant.ResolverFunc(func(signal.TenantID) tenant.Policy {
			return tenant.Policy{Retention: tenant.Retention{MaxBytes: 1 << 30}}
		})))
	require.NoError(t, err)

	t.Cleanup(func() { require.NoError(t, s.Close(context.Background())) })

	ctx := context.Background()
	now := time.Now().UnixNano()

	_, err = s.WriteMetrics(ctx, gaugeBatch("api", "m", []int64{now}, []float64{1}))
	require.NoError(t, err)
	s.maintain(ctx) // flushes the head into a part

	tids := map[signal.TenantID]struct{}{"default": {}}

	be.lists.Store(0)
	s.sizeCutoffs(ctx, tids)
	require.Positive(t, be.lists.Load(), "the first resolution enumerates the parts")

	be.lists.Store(0)
	s.sizeCutoffs(ctx, tids)
	s.sizeCutoffs(ctx, tids)
	assert.Zero(t, be.lists.Load(), "an unchanged part set must not re-enumerate part sizes")

	// A new part changes the fingerprint, so the cutoff is resolved again.
	_, err = s.WriteMetrics(ctx, gaugeBatch("api", "m", []int64{now + 1}, []float64{2}))
	require.NoError(t, err)
	s.maintain(ctx)

	be.lists.Store(0)
	s.sizeCutoffs(ctx, tids)
	assert.Positive(t, be.lists.Load(), "a changed part set must re-enumerate")
}

// TestSizeRetentionMemoDropsUnheldTenants covers the cache prune: a tenant that stops appearing in
// a cycle must not pin its entry.
func TestSizeRetentionMemoDropsUnheldTenants(t *testing.T) {
	t.Parallel()

	var c sizeRetentionCache

	c.store("a", 1, 10)
	c.store("b", 2, 20)

	got, ok := c.lookup("a", 1)
	require.True(t, ok)
	assert.Equal(t, int64(10), got)

	_, ok = c.lookup("a", 99)
	assert.False(t, ok, "a different part set is a miss")

	c.retain(map[signal.TenantID]struct{}{"a": {}})
	_, ok = c.lookup("b", 2)
	assert.False(t, ok, "a tenant outside the retained set is dropped")

	c.forget("a")
	_, ok = c.lookup("a", 1)
	assert.False(t, ok, "forget drops the entry")
}

func TestPartSetFingerprintOrderIndependent(t *testing.T) {
	t.Parallel()

	a := hashPartID("0000000001", 10)
	b := hashPartID("0000000002", 20)

	assert.Equal(t, a^b, b^a)
	assert.NotEqual(t, a, hashPartID("0000000001", 11), "the time bound is part of the identity")
	assert.NotEqual(t, a, hashPartID("0000000002", 10), "the prefix is part of the identity")
	assert.NotEqual(t, hashUint64(a, 1), hashUint64(a, 2), "the budget is folded in")
}

// BenchmarkSizeCutoffsIdle is the maintenance-loop cost of size retention on a node where nothing
// changed since the last cycle — the shape of an idle deployment's background CPU. "recomputed" is
// what every cycle cost before the memo; the file backend is the deployed shape, where the
// enumeration is syscalls rather than map lookups.
func BenchmarkSizeCutoffsIdle(b *testing.B) {
	backends := []struct {
		name string
		open func(b *testing.B) backend.Backend
	}{
		{"memory", func(*testing.B) backend.Backend { return backend.Memory() }},
		{"file", func(b *testing.B) backend.Backend {
			b.Helper()

			be, err := file.New(b.TempDir())
			if err != nil {
				b.Fatal(err)
			}

			return be
		}},
	}

	for _, bk := range backends {
		b.Run(bk.name, func(b *testing.B) {
			ctx := context.Background()

			s, err := Open(ctx, Options{},
				WithBackend(bk.open(b)),
				WithFlushInterval(-1), // no background loop: the benchmark drives maintenance
				WithTenancy(tenant.ResolverFunc(func(signal.TenantID) tenant.Policy {
					return tenant.Policy{
						Limits:    tenant.Limits{MaxPartSize: 4 << 10},
						Retention: tenant.Retention{MaxBytes: 1 << 30},
					}
				})))
			if err != nil {
				b.Fatal(err)
			}

			defer func() { _ = s.Close(ctx) }()

			now := time.Now().UnixNano()
			for i := range 32 {
				ts, vals := make([]int64, 128), make([]float64, 128)
				for j := range ts {
					ts[j] = now + int64(i*128+j)*int64(time.Second)
					vals[j] = float64(j)
				}

				if _, err := s.WriteMetrics(ctx, gaugeBatch("api", "m", ts, vals)); err != nil {
					b.Fatal(err)
				}

				s.maintain(ctx)
			}

			tids := map[signal.TenantID]struct{}{"default": {}}

			b.Run("recomputed", func(b *testing.B) {
				b.ReportAllocs()

				for b.Loop() {
					s.sizeRetention.forget("default")
					s.sizeCutoffs(ctx, tids)
				}
			})

			b.Run("memoized", func(b *testing.B) {
				b.ReportAllocs()

				for b.Loop() {
					s.sizeCutoffs(ctx, tids)
				}
			})
		})
	}
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

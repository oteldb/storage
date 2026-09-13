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
			Limits:    tenant.Limits{MaxPartSize: 512}, // a handful of records per part
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

	ca := bySignal{signal.Metric: 10, signal.Log: 11}
	c.store("a", 1, ca)
	c.store("b", 2, bySignal{signal.Metric: 20})

	got, ok := c.lookup("a", 1)
	require.True(t, ok)
	assert.Equal(t, ca, got, "every signal's cutoff comes back")

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

// mixedSignalStore builds a store holding both metrics and logs for one tenant, with the byte
// budgets resolved live from budgets so a test can set them after measuring real parts. Log records
// are strictly older than every metric sample, which is what makes a *pooled* budget's damage
// unambiguous: the logs are always the oldest thing to drop.
func mixedSignalStore(t *testing.T, budgets *atomic.Pointer[tenant.Retention]) *Storage {
	t.Helper()

	s, err := InMemory(WithTenancy(tenant.ResolverFunc(func(signal.TenantID) tenant.Policy {
		var r tenant.Retention
		if p := budgets.Load(); p != nil {
			r = *p
		}

		return tenant.Policy{Limits: tenant.Limits{MaxPartSize: 512}, Retention: r}
	})))
	require.NoError(t, err)

	return s
}

const (
	mixedLogBase    = 1
	mixedMetricBase = 10_000
	mixedRows       = 200
)

func writeMixedLogs(t *testing.T, s *Storage, from int) {
	t.Helper()

	recs := make([][3]any, 0, mixedRows)
	for i := from; i < from+mixedRows; i++ {
		recs = append(recs, [3]any{i, 9, "body-" + strconv.Itoa(i)})
	}

	_, err := s.WriteLogs(context.Background(), logBatch("api", recs...))
	require.NoError(t, err)
}

func writeMixedMetrics(t *testing.T, s *Storage, from int64) {
	t.Helper()

	ts, vals := make([]int64, mixedRows), make([]float64, mixedRows)
	for i := range ts {
		ts[i] = from + int64(i)
		vals[i] = float64(i)
	}

	_, err := s.WriteMetrics(context.Background(), gaugeBatch("api", "m", ts, vals))
	require.NoError(t, err)
}

// storedBytes reads one signal's flushed footprint for the default tenant.
func storedBytes(t *testing.T, s *Storage, sig signal.Signal) int64 {
	t.Helper()

	eff, err := s.EfficiencyStats(context.Background())
	require.NoError(t, err)
	require.Len(t, eff, 1)

	for _, se := range eff[0].Signals {
		if se.Signal == sig {
			require.Positive(t, se.StoredBytes)

			return se.StoredBytes
		}
	}

	t.Fatalf("no %s parts", sig)

	return 0
}

func mixedLogBodies(t *testing.T, s *Storage) []string {
	t.Helper()

	eng, ok := s.lookupRecordEngine(signal.Log, "default")
	require.True(t, ok)

	return logBodies(t, eng, fetch.Request{Start: 0, End: 1 << 40, Matchers: []fetch.Matcher{logSvcMatcher("api")}})
}

func mixedMetricTimestamps(t *testing.T, s *Storage) []int64 {
	t.Helper()

	eng := mustEngine(s.engineFor("default"))
	it, err := eng.Fetch(context.Background(), fetch.Request{
		Start: 0, End: 1 << 40, Matchers: []fetch.Matcher{nameMatcher("m")},
	})
	require.NoError(t, err)
	batches, err := fetch.Drain(context.Background(), it)
	require.NoError(t, err)
	require.Len(t, batches, 1)

	return batches[0].Timestamps
}

// TestSizeRetentionIsolatesSignals is the reason per-signal budgets exist: a tenant whose metrics
// outgrow their budget must not lose log history to it. The same load under the pooled budget is
// the contrast — there the logs, being the oldest data, are what the metric growth evicts.
func TestSizeRetentionIsolatesSignals(t *testing.T) {
	t.Parallel()

	t.Run("per-signal", func(t *testing.T) {
		t.Parallel()

		var budgets atomic.Pointer[tenant.Retention]

		s := mixedSignalStore(t, &budgets)
		ctx := context.Background()

		writeMixedLogs(t, s, mixedLogBase)
		writeMixedMetrics(t, s, mixedMetricBase)
		s.maintain(ctx)

		// One batch of metrics is the metric budget, so the second batch below forces a drop; the
		// logs get ten times what they occupy and must be untouched by it.
		budgets.Store(&tenant.Retention{MaxBytesPerSignal: map[signal.Signal]int64{
			signal.Metric: storedBytes(t, s, signal.Metric),
			signal.Log:    10 * storedBytes(t, s, signal.Log),
		}})

		writeMixedMetrics(t, s, mixedMetricBase+10_000)
		s.maintain(ctx) // flushes the new parts (the budget was met when the cycle started)
		s.maintain(ctx) // over the metric budget now: drops oldest metric parts

		ts := mixedMetricTimestamps(t, s)
		require.NotEmpty(t, ts)
		assert.Greater(t, ts[0], int64(mixedMetricBase), "the oldest metric samples were dropped")

		bodies := mixedLogBodies(t, s)
		assert.Len(t, bodies, mixedRows, "every log record survived the metric eviction")
		assert.Contains(t, bodies, "body-"+strconv.Itoa(mixedLogBase), "the oldest log record is retained")
	})

	t.Run("pooled", func(t *testing.T) {
		t.Parallel()

		var budgets atomic.Pointer[tenant.Retention]

		s := mixedSignalStore(t, &budgets)
		ctx := context.Background()

		writeMixedLogs(t, s, mixedLogBase)
		writeMixedMetrics(t, s, mixedMetricBase)
		s.maintain(ctx)
		require.Len(t, mixedLogBodies(t, s), mixedRows, "the logs are there to lose")

		// The pooled budget fits the metrics alone, so the logs — the oldest parts — are what pays
		// for the metric growth even though their own volume never moved.
		budgets.Store(&tenant.Retention{MaxBytes: 2 * storedBytes(t, s, signal.Metric)})

		writeMixedMetrics(t, s, mixedMetricBase+10_000)
		s.maintain(ctx)
		s.maintain(ctx)

		assert.Empty(t, mixedLogBodies(t, s), "a pooled budget lets metric growth evict the logs")
	})
}

// TestSizeRetentionPooledBoundsPerSignalBudgets pins that the two budgets compose: per-signal
// budgets no signal exceeds still answer to the tenant-wide bound when their sum does not fit.
func TestSizeRetentionPooledBoundsPerSignalBudgets(t *testing.T) {
	t.Parallel()

	var budgets atomic.Pointer[tenant.Retention]

	s := mixedSignalStore(t, &budgets)
	ctx := context.Background()

	writeMixedLogs(t, s, mixedLogBase)
	writeMixedMetrics(t, s, mixedMetricBase)
	s.maintain(ctx)

	require.Len(t, mixedLogBodies(t, s), mixedRows, "the logs are there to lose")

	metricBytes := storedBytes(t, s, signal.Metric)
	budgets.Store(&tenant.Retention{
		MaxBytes: 2 * metricBytes, // fits the metrics alone, as above
		MaxBytesPerSignal: map[signal.Signal]int64{
			signal.Metric: 1 << 30, // neither per-signal budget binds
			signal.Log:    1 << 30,
		},
	})

	writeMixedMetrics(t, s, mixedMetricBase+10_000)
	s.maintain(ctx)
	s.maintain(ctx)

	assert.Empty(t, mixedLogBodies(t, s), "the pooled budget still bounds a signal its own budget does not")
}

// TestSizeCutoffsSkipsUnbudgetedSignals pins the enumeration scope: measuring a signal nothing
// budgets is pure backend I/O for a number no engine reads.
func TestSizeCutoffsSkipsUnbudgetedSignals(t *testing.T) {
	t.Parallel()

	be := &listCountingBackend{Backend: backend.Memory()}
	s, err := Open(context.Background(), Options{},
		WithBackend(be),
		WithFlushInterval(-1), // no background loop: this test drives maintain itself
		WithTenancy(tenant.ResolverFunc(func(signal.TenantID) tenant.Policy {
			return tenant.Policy{Retention: tenant.Retention{
				MaxBytesPerSignal: map[signal.Signal]int64{signal.Log: 1 << 30},
			}}
		})))
	require.NoError(t, err)

	t.Cleanup(func() { require.NoError(t, s.Close(context.Background())) })

	ctx := context.Background()

	writeMixedMetrics(t, s, mixedMetricBase)
	s.maintain(ctx)

	tids := map[signal.TenantID]struct{}{"default": {}}

	be.lists.Store(0)
	s.sizeCutoffs(ctx, tids)
	assert.Zero(t, be.lists.Load(), "a metric-only store under a log-only budget enumerates nothing")

	writeMixedLogs(t, s, mixedLogBase)
	s.maintain(ctx)

	be.lists.Store(0)
	s.sizeCutoffs(ctx, tids)
	assert.Positive(t, be.lists.Load(), "the budgeted signal is measured")
}

func TestBudgetsOf(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		retention tenant.Retention
		want      sizeBudgets
		empty     bool
		covers    map[signal.Signal]bool
	}{
		{
			name:   "unset",
			empty:  true,
			covers: map[signal.Signal]bool{signal.Metric: false, signal.Log: false},
		},
		{
			name:      "pooled only covers every signal",
			retention: tenant.Retention{MaxBytes: 100},
			want:      sizeBudgets{pooled: 100},
			covers:    map[signal.Signal]bool{signal.Metric: true, signal.Profile: true},
		},
		{
			name:      "per-signal covers only its own",
			retention: tenant.Retention{MaxBytesPerSignal: map[signal.Signal]int64{signal.Log: 100}},
			want:      sizeBudgets{perSignal: bySignal{signal.Log: 100}},
			covers:    map[signal.Signal]bool{signal.Log: true, signal.Metric: false},
		},
		{
			name: "both",
			retention: tenant.Retention{
				MaxBytes:          100,
				MaxBytesPerSignal: map[signal.Signal]int64{signal.Trace: 50},
			},
			want:   sizeBudgets{pooled: 100, perSignal: bySignal{signal.Trace: 50}},
			covers: map[signal.Signal]bool{signal.Trace: true, signal.Metric: true},
		},
		{
			name:      "unknown signal is ignored",
			retention: tenant.Retention{MaxBytesPerSignal: map[signal.Signal]int64{signal.Signal(200): 100}},
			empty:     true,
			covers:    map[signal.Signal]bool{signal.Signal(200): false},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := budgetsOf(tt.retention)
			assert.Equal(t, tt.want, got)
			assert.Equal(t, tt.empty, got.empty())

			for sig, want := range tt.covers {
				assert.Equal(t, want, got.covers(sig), "covers(%s)", sig)
			}
		})
	}
}

func TestBySignalAt(t *testing.T) {
	t.Parallel()

	v := bySignal{signal.Metric: 7}

	assert.Equal(t, int64(7), v.at(signal.Metric))
	assert.Zero(t, v.at(signal.Log))
	assert.Zero(t, v.at(signal.Signal(200)), "a signal outside the known range reads as unbudgeted")
	assert.True(t, v.any())
	assert.False(t, bySignal{}.any())
}

// TestPartSetFingerprintFoldsBudgets pins the memo's other input: moving a budget — pooled or
// per-signal — must invalidate the entry, or the cutoff would be memoized against the old one.
func TestPartSetFingerprintFoldsBudgets(t *testing.T) {
	t.Parallel()

	var budgets atomic.Pointer[tenant.Retention]

	s := mixedSignalStore(t, &budgets)
	ctx := context.Background()

	writeMixedLogs(t, s, mixedLogBase)
	writeMixedMetrics(t, s, mixedMetricBase)
	s.maintain(ctx)

	base := sizeBudgets{pooled: 1 << 30, perSignal: bySignal{signal.Log: 1 << 20}}
	fp := s.partSetFingerprint("default", base)

	pooled := base
	pooled.pooled++
	assert.NotEqual(t, fp, s.partSetFingerprint("default", pooled), "the pooled budget is folded in")

	perSignal := base
	perSignal.perSignal[signal.Log]++
	assert.NotEqual(t, fp, s.partSetFingerprint("default", perSignal), "a per-signal budget is folded in")

	moved := base
	moved.perSignal[signal.Trace] = base.perSignal[signal.Log]
	moved.perSignal[signal.Log] = 0
	assert.NotEqual(t, fp, s.partSetFingerprint("default", moved),
		"the same budget on a different signal is a different fingerprint")

	assert.Equal(t, fp, s.partSetFingerprint("default", base), "an unchanged input is an unchanged fingerprint")
}

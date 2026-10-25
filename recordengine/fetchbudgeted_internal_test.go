package recordengine

import (
	"context"
	"math"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/go-faster/errors"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/encoding/chunk"
	"github.com/oteldb/storage/encoding/compress"
	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/readbudget"
	"github.com/oteldb/storage/signal"
)

func projectedBudgetEngine(t *testing.T, rows int, location string) *Engine {
	t.Helper()
	return projectedBudgetEngineOn(t, rows, location, backend.Memory())
}

func projectedBudgetEngineOn(t *testing.T, rows int, location string, be backend.Backend) *Engine {
	t.Helper()
	ctx := t.Context()
	e := New(Config{Schema: NewSchema(
		Column{Name: "severity", Kind: KindInt64, Codec: chunk.CodecT64},
		Column{Name: "severity_text", Kind: KindBytes, Codec: chunk.CodecDict},
		Column{Name: "body", Kind: KindBytes, Codec: chunk.CodecDict},
	), Backend: be, Prefix: "test/projected"})
	series := signal.Series{}
	b := &Batch{Stream: series.Hash(), Identity: func() signal.Series { return series },
		Ints: make([][]int64, 1), Bytes: make([][][]byte, 2)}
	body := []byte(strings.Repeat("b", 1024))
	for i := range rows {
		b.Ts = append(b.Ts, int64(i))
		b.Ints[0] = append(b.Ints[0], int64(9+i%3))
		b.Bytes[0] = append(b.Bytes[0], []byte("info"))
		b.Bytes[1] = append(b.Bytes[1], body)
	}
	_, err := e.AppendBatch(b, AppendLimits{})
	require.NoError(t, err)
	switch location {
	case "part":
		require.NoError(t, e.Flush(ctx))
	case "flushing":
		e.flushing, e.head.records = e.head.records, nil
	}
	return e
}

func TestProjectedFetchBudget(t *testing.T) {
	t.Parallel()
	for _, location := range []string{"part", "head", "flushing"} {
		for _, tc := range []struct {
			name       string
			projection []string
			start, end int64
			want       int
			refused    bool
		}{
			{"all severity", []string{"severity", "severity_text"}, 0, 4095, 4096, false},
			{"one severity", []string{"severity", "severity_text"}, 2000, 2000, 1, false},
			{"one full row", nil, 2000, 2000, 1, false},
			{"all full rows refused", nil, 0, 4095, 0, true},
		} {
			t.Run(location+"/"+tc.name, func(t *testing.T) {
				t.Parallel()
				e := projectedBudgetEngine(t, 4096, location)
				budget := readbudget.New(1 << 20)
				ctx := readbudget.With(t.Context(), budget)
				it, err := e.Fetch(ctx, fetch.Request{Start: tc.start, End: tc.end, Projection: tc.projection})
				if tc.refused {
					require.ErrorIs(t, err, readbudget.ErrExceeded)
					assert.Equal(t, int64(1<<20), budget.Remaining())
					return
				}
				require.NoError(t, err)
				batches, err := fetch.Drain(ctx, it)
				require.NoError(t, err)
				got := 0
				for _, b := range batches {
					got += len(b.Timestamps)
					if tc.projection != nil {
						assert.Len(t, b.Columns, len(tc.projection))
					}
				}
				assert.Equal(t, tc.want, got)
				assert.Equal(t, int64(1<<20), budget.Remaining())
			})
		}
	}
}

func TestBudgetedFetchMatchesUnbounded(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name string
		r    fetch.Request
	}{
		{"window", fetch.Request{Start: 200, End: 240, Projection: []string{"severity", "severity_text"}}},
		{"empty", fetch.Request{Start: 5000, End: 5100}},
		{"limit oldest", fetch.Request{Start: 0, End: 4095, Limit: 7, Projection: []string{"severity"}}},
		{"limit newest", fetch.Request{Start: 0, End: 4095, Limit: 7, Reverse: true, Projection: []string{"severity"}}},
		{"condition", fetch.Request{Start: 0, End: 4095, Projection: []string{"severity_text"}, AllConditions: true,
			Conditions: []fetch.Condition{{Column: "severity", Match: func(v signal.Value) bool { return v.Int() == 10 }}}}},
		{"condition no hits", fetch.Request{Start: 0, End: 4095, Projection: []string{"severity"}, AllConditions: true,
			Conditions: []fetch.Condition{{Column: "body", Match: func(signal.Value) bool { return false }}}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			e := projectedBudgetEngine(t, 4096, "part")
			ctx := t.Context()
			wantIt, err := e.Fetch(ctx, tc.r)
			require.NoError(t, err)
			want, err := fetch.Drain(ctx, wantIt)
			require.NoError(t, err)
			budget := readbudget.New(32 << 20)
			bctx := readbudget.With(ctx, budget)
			gotIt, err := e.Fetch(bctx, tc.r)
			require.NoError(t, err)
			got, err := fetch.Drain(bctx, gotIt)
			require.NoError(t, err)
			assert.Equal(t, want, got)
			assert.Equal(t, int64(32<<20), budget.Remaining())
		})
	}
}

func TestBudgetedPlanDoesNotPresizeWholePart(t *testing.T) {
	t.Parallel()
	e := projectedBudgetEngine(t, 4096, "part")
	budget := readbudget.New(1 << 20)
	ctx := readbudget.With(context.Background(), budget)
	e.mu.RLock()
	p, err := e.planFetch(ctx, []signal.SeriesID{(signal.Series{}).Hash()}, fetch.Request{Start: 2000, End: 2000, Projection: []string{"severity"}})
	e.mu.RUnlock()
	require.NoError(t, err)
	defer p.releaseBudget()
	defer p.releaseParts()
	for _, acc := range p.accs {
		assert.Zero(t, cap(acc.ts))
		assert.Zero(t, cap(acc.ints[0]))
		assert.Zero(t, cap(acc.bytes[1].offsets))
	}
}

func TestBudgetedFetchReleasesAfterDecodeRefusal(t *testing.T) {
	t.Parallel()
	e := projectedBudgetEngine(t, 4096, "part")
	budget := readbudget.New(2 << 10)
	_, err := e.Fetch(readbudget.With(t.Context(), budget), fetch.Request{Start: 2000, End: 2000, Projection: []string{"severity"}})
	require.ErrorIs(t, err, readbudget.ErrExceeded)
	assert.Equal(t, int64(2<<10), budget.Remaining())
	assert.Zero(t, e.parts[0].refs.Load())
}

type budgetReadSpy struct {
	backend.Backend

	active bool
	fail   bool
	reads  int
}

func (b *budgetReadSpy) Read(ctx context.Context, key string) ([]byte, error) {
	if b.active {
		b.reads++
		budget := readbudget.From(ctx)
		if budget == nil || budget.Peak() <= 2048 {
			return nil, errors.New("read started without decode reservation")
		}
		if b.fail && strings.Contains(key, "/c/") {
			return nil, errors.New("injected read failure")
		}
	}
	return b.Backend.Read(ctx, key)
}

func TestBudgetedReadReservesBeforeIO(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name            string
		budget          int64
		fail, wantError bool
	}{
		{"refused before decode", 2048, false, true},
		{"read failure cleans up", 1 << 20, true, true},
		{"projected decode", 1 << 20, false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			be := &budgetReadSpy{Backend: backend.Memory()}
			e := projectedBudgetEngineOn(t, 4096, "part", be)
			be.active, be.fail = true, tc.fail
			budget := readbudget.New(tc.budget)
			ctx := readbudget.With(t.Context(), budget)
			it, err := e.Fetch(ctx, fetch.Request{Start: 2000, End: 2000, Projection: []string{"severity"}})
			if tc.wantError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.NoError(t, it.Close())
			}
			if tc.budget == 2048 {
				assert.Zero(t, be.reads)
			} else {
				assert.Positive(t, be.reads)
			}
			assert.Equal(t, tc.budget, budget.Remaining())
			assert.Zero(t, e.parts[0].refs.Load())
		})
	}
}

func TestBudgetedLegacyReadStaysConservative(t *testing.T) {
	t.Parallel()
	e := projectedBudgetEngine(t, 4096, "part")
	old := e.parts[0]
	m := old.reader.Manifest()
	m.Columns = slices.Clone(m.Columns)
	for i := range m.Columns {
		m.Columns[i].HasSizing = false
	}
	require.NoError(t, e.cfg.Backend.Write(t.Context(), old.prefix+"/manifest", m.Encode(nil)))
	p, err := openPart(t.Context(), e.cfg.Backend, e.cfg.Schema, old.prefix, e.cfg.Obs.Corruption)
	require.NoError(t, err)
	p.minTime, p.maxTime = old.minTime, old.maxTime
	e.parts[0] = p
	r := fetch.Request{Start: 2000, End: 2000, Projection: []string{"severity"}}
	_, err = e.Fetch(readbudget.With(t.Context(), readbudget.New(1<<20)), r)
	require.ErrorIs(t, err, readbudget.ErrExceeded)
	it, err := e.Fetch(readbudget.With(t.Context(), readbudget.New(32<<20)), r)
	require.NoError(t, err)
	require.NoError(t, it.Close())
}

func TestFetchBudgetArithmetic(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name           string
		a, b, add, mul int64
	}{
		{"zero", 0, 0, 0, 0},
		{"ordinary", 5, 7, 12, 35},
		{"addition saturates", math.MaxInt64, 1, math.MaxInt64, math.MaxInt64},
		{"multiplication saturates", math.MaxInt64 / 2, 3, math.MaxInt64/2 + 3, math.MaxInt64},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.add, fetchAdd(tc.a, tc.b))
			assert.Equal(t, tc.mul, fetchMul(tc.a, tc.b))
		})
	}
}

func TestConditionMemoUsesDictionaryDomain(t *testing.T) {
	t.Parallel()
	p := &fetchPlan{conds: make([]fetch.Condition, 2)}
	lz := &lazyCols{bytes: []*chunk.DictColumn{{Entries: make([][]byte, 100_000)}}}
	assert.GreaterOrEqual(t, p.conditionMemoBytes(lz), int64(2*100_000))
}

func TestBudgetedHugeLimitDoesNotPresizeWatermark(t *testing.T) {
	t.Parallel()
	e := projectedBudgetEngine(t, 4096, "part")
	budget := readbudget.New(1 << 20)
	it, err := e.Fetch(readbudget.With(t.Context(), budget), fetch.Request{Start: 2000, End: 2000, Limit: math.MaxInt, Projection: []string{"severity"}})
	require.NoError(t, err)
	got, err := fetch.Drain(t.Context(), it)
	require.NoError(t, err)
	require.Len(t, got, 1)
	assert.Len(t, got[0].Timestamps, 1)
	assert.Equal(t, int64(1<<20), budget.Remaining())
}

func TestBudgetedPlanCountsAdoptedParts(t *testing.T) {
	t.Parallel()
	e := projectedBudgetEngine(t, 4096, "part")
	e.foreignParts = make(map[string]*part, 1000)
	for i := range 1000 {
		e.foreignParts[strconv.Itoa(i)] = e.parts[0]
	}
	budget := readbudget.New(8 << 10)
	ctx := readbudget.With(t.Context(), budget)
	e.mu.RLock()
	p, err := e.planFetch(ctx, []signal.SeriesID{(signal.Series{}).Hash()}, fetch.Request{Start: 2000, End: 2000, Projection: []string{"severity"}})
	e.mu.RUnlock()
	require.ErrorIs(t, err, readbudget.ErrExceeded)
	assert.Empty(t, p.liveParts, "metadata admission must precede the owned/adopted part snapshot")
	p.releaseBudget()
	assert.Equal(t, int64(8<<10), budget.Remaining())
}

func TestBudgetedRecycleDoesNotRetainFreshBuffers(t *testing.T) {
	t.Parallel()
	e := projectedBudgetEngine(t, 4096, "part")
	budget := readbudget.New(1 << 20)
	it, err := e.Fetch(readbudget.With(t.Context(), budget), fetch.Request{Start: 2000, End: 2000, Projection: []string{"severity"}, Recycle: true})
	require.NoError(t, err)
	got, err := fetch.Drain(t.Context(), it)
	require.NoError(t, err)
	require.Len(t, got, 1)
	got[0].Release()
	assert.Nil(t, e.recPool.Get(), "budgeted reads do not consume this pool, so must not fill it")
	assert.Equal(t, int64(1<<20), budget.Remaining())
}

//nolint:paralleltest // lowers the package-level column cap
func TestBudgetedPregrowRefusesColumnOverflow(t *testing.T) {
	withByteColCap(t, 48)
	acc := capTestCols(t, strings.Repeat("a", 40))
	budget := readbudget.New(1 << 20)
	p := &fetchPlan{sel: acc.sel, memory: newFetchMemory(readbudget.With(t.Context(), budget))}
	oldCap := cap(acc.bytes[0].data)
	err := p.prepareRows(acc, 1, []int64{40})
	require.ErrorContains(t, err, "fetch result too large")
	assert.Equal(t, oldCap, cap(acc.bytes[0].data), "refuse before growing the oversized blob")
	assert.Zero(t, budget.Peak())
}

func TestBudgetedCompressedProjection(t *testing.T) {
	t.Parallel()
	e := projectedBudgetEngine(t, 4096, "part")
	e.cfg.MergeCompression = compress.AlgorithmZSTD
	require.NoError(t, e.MergeWith(t.Context(), MergeOptions{}))
	budget := readbudget.New(2 << 20)
	it, err := e.Fetch(readbudget.With(t.Context(), budget), fetch.Request{Start: 2000, End: 2000,
		Projection: []string{"severity", "severity_text"}})
	require.NoError(t, err)
	got, err := fetch.Drain(t.Context(), it)
	require.NoError(t, err)
	require.Len(t, got, 1)
	assert.Len(t, got[0].Timestamps, 1)
	assert.Equal(t, int64(2<<20), budget.Remaining())
}

func TestBudgetedMultipleSourcesMatchUnbounded(t *testing.T) {
	t.Parallel()
	e := projectedBudgetEngine(t, 100, "part")
	series := []signal.Series{{}, {Resource: signal.Resource{Attributes: signal.NewAttributes(
		signal.KeyValue{Key: []byte("service.name"), Value: signal.StringValue([]byte("other"))},
	)}}}
	for round := range 2 {
		for _, s := range series {
			batch := &Batch{Stream: s.Hash(), Identity: func() signal.Series { return s },
				Ts: []int64{50, 50, 75, 100, 150}, Ints: [][]int64{{9, 10, 11, 9, 10}},
				Bytes: [][][]byte{{[]byte("info"), []byte("warn"), []byte("error"), []byte("info"), []byte("warn")},
					{[]byte("a"), []byte("b"), []byte("c"), []byte("d"), []byte(strconv.Itoa(round))}}}
			_, err := e.AppendBatch(batch, AppendLimits{})
			require.NoError(t, err)
		}
		require.NoError(t, e.Flush(t.Context()))
	}
	for _, s := range series {
		_, err := e.AppendBatch(&Batch{Stream: s.Hash(), Identity: func() signal.Series { return s },
			Ts: []int64{50, 100, 250}, Ints: [][]int64{{9, 10, 11}},
			Bytes: [][][]byte{{[]byte("info"), []byte("warn"), []byte("error")},
				{[]byte("head-a"), []byte("head-b"), []byte("head-c")}}}, AppendLimits{})
		require.NoError(t, err)
		buf := newRecordCols(e.cfg.Schema, 3, fullSel(e.cfg.Schema))
		for _, ts := range []int64{50, 150, 300} {
			buf.appendClone(rec{ts: ts, ints: []int64{10}, bytes: [][]byte{[]byte("warn"), []byte("flushing")}})
		}
		if e.flushing == nil {
			e.flushing = make(map[signal.SeriesID]*recordCols)
		}
		e.flushing[s.Hash()] = buf
	}
	require.Len(t, e.parts, 3)
	require.Equal(t, 6, e.HeadRecordCount())
	for _, tc := range []struct {
		name string
		r    fetch.Request
	}{
		{"all", fetch.Request{Start: 0, End: 500}},
		{"narrow projection", fetch.Request{Start: 70, End: 100, Projection: []string{"severity", "severity_text"}}},
		{"oldest boundary ties", fetch.Request{Start: 50, End: 150, Limit: 5, Projection: []string{"severity"}}},
		{"newest boundary ties", fetch.Request{Start: 50, End: 150, Limit: 3, Reverse: true, Projection: []string{"severity"}}},
		{"head and part condition", fetch.Request{Start: 0, End: 500, Projection: []string{"severity_text"}, AllConditions: true,
			Conditions: []fetch.Condition{{Column: "severity", Match: func(v signal.Value) bool { return v.Int() == 10 }}}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			wantIt, err := e.Fetch(t.Context(), tc.r)
			require.NoError(t, err)
			want, err := fetch.Drain(t.Context(), wantIt)
			require.NoError(t, err)
			budget := readbudget.New(32 << 20)
			ctx := readbudget.With(t.Context(), budget)
			gotIt, err := e.Fetch(ctx, tc.r)
			require.NoError(t, err)
			got, err := fetch.Drain(ctx, gotIt)
			require.NoError(t, err)
			assert.Equal(t, want, got)
			assert.Equal(t, int64(32<<20), budget.Remaining())
		})
	}
}

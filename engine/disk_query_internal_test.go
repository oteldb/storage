package engine

import (
	"bytes"
	"context"
	"slices"
	"strconv"
	"testing"

	"github.com/go-faster/errors"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/backend/backendtest"
	"github.com/oteldb/storage/backend/bucketindex"
	backendfile "github.com/oteldb/storage/backend/file"
	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/readbudget"
	"github.com/oteldb/storage/signal"
)

func diskQuerySeries(value signal.Value) signal.Series {
	attrs := signal.Attributes{{Key: []byte("job"), Value: signal.StringValue([]byte("api"))}}
	if value.Kind() != signal.KindEmpty {
		attrs = append(attrs, signal.KeyValue{Key: []byte("host"), Value: value})
	}
	return signal.Series{Attributes: signal.NewAttributes(attrs...)}
}

// Queries must work with no historical identity registry, including legacy and new parts together.
func TestDiskQueryWithoutResidentHistory(t *testing.T) {
	t.Parallel()
	for _, mixed := range []bool{false, true} {
		t.Run(map[bool]string{false: "disk", true: "mixed"}[mixed], func(t *testing.T) {
			t.Parallel()
			ctx := t.Context()
			ref := New(Config{Backend: backend.Memory(), Prefix: "reference", MetricBlockRows: 4})
			e := New(Config{Backend: backend.Memory(), Prefix: "disk", DiskSeries: !mixed, MetricBlockRows: 4})
			series := []signal.Series{
				diskQuerySeries(signal.StringValue([]byte("a"))),
				diskQuerySeries(signal.StringValue([]byte("1"))),
				diskQuerySeries(signal.IntValue(1)), diskQuerySeries(signal.EmptyValue()),
			}
			for generation := range 3 {
				for i, s := range series {
					for _, ts := range []int64{10, 20} {
						for _, target := range []*Engine{ref, e} {
							_, err := target.Append(s, ts+int64(generation*30+i), float64(i+1))
							require.NoError(t, err)
						}
					}
				}
				if generation < 2 {
					for _, target := range []*Engine{ref, e} {
						require.NoError(t, target.Flush(ctx))
					}
					// Simulate a cold resident head independently of the disk lifecycle changes.
					e.mu.LockState()
					disk := e.head.disk
					e.head = newHead()
					if disk == nil {
						disk = newDiskHeadState()
					}
					e.head.disk = disk
					e.cfg.DiskSeries = true
					e.mu.UnlockState()
				}
			}
			equal := fetch.EqualMatcher{Name: "host", Value: "1"}
			matchers := [][]fetch.Matcher{
				nil,
				{{Name: []byte("host"), Match: equal.Predicate(), Spec: &equal}},
				{{Name: []byte("host"), Match: func(v signal.Value) bool { return v.Kind() == signal.KindEmpty }}},
				{{Name: []byte("host"), Match: func(v signal.Value) bool { return string(v.AppendText(nil)) != "a" }}},
				{{Name: []byte("host"), Match: func(v signal.Value) bool { return string(v.AppendText(nil)) == "1" }, StringValues: [][]byte{[]byte("1")}}},
			}
			for _, m := range matchers {
				for _, bounds := range [][2]int64{{0, 100}, {15, 48}, {21, 39}, {100, 200}} {
					r := fetch.Request{Start: bounds[0], End: bounds[1], Matchers: m}
					wantCount, err := ref.Count(ctx, r)
					require.NoError(t, err)
					gotCount, err := e.Count(ctx, r)
					require.NoError(t, err)
					require.Equal(t, wantCount, gotCount)
					wantGroups, err := ref.CountBy(ctx, r, []byte("host"))
					require.NoError(t, err)
					gotGroups, err := e.CountBy(ctx, r, []byte("host"))
					require.NoError(t, err)
					require.Equal(t, wantGroups, gotGroups)
					wantSeries, err := ref.Series(ctx, r)
					require.NoError(t, err)
					gotSeries, err := e.Series(ctx, r)
					require.NoError(t, err)
					wantIDs, gotIDs := make([]signal.SeriesID, len(wantSeries)), make([]signal.SeriesID, len(gotSeries))
					for i, s := range wantSeries {
						wantIDs[i] = s.Hash()
					}
					for i, s := range gotSeries {
						gotIDs[i] = s.Hash()
					}
					require.ElementsMatch(t, wantIDs, gotIDs)
					wantNames, err := ref.LabelNames(ctx, r)
					require.NoError(t, err)
					gotNames, err := e.LabelNames(ctx, r)
					require.NoError(t, err)
					require.ElementsMatch(t, wantNames, gotNames)
					wantValues, err := ref.LabelValues(ctx, r, []byte("host"))
					require.NoError(t, err)
					gotValues, err := e.LabelValues(ctx, r, []byte("host"))
					require.NoError(t, err)
					require.ElementsMatch(t, wantValues, gotValues)
					wantAgg, err := ref.AggregateRange(ctx, r)
					require.NoError(t, err)
					gotAgg, err := e.AggregateRange(ctx, r)
					require.NoError(t, err)
					require.Equal(t, wantAgg, gotAgg)
					for _, group := range [][]byte{nil, []byte("host")} {
						grid := fetch.WindowGrid{Start: 0, Step: 5, Width: 20, NumSteps: 25}
						want, err := ref.CountGrid(ctx, r, grid, group)
						require.NoError(t, err)
						got, err := e.CountGrid(ctx, r, grid, group)
						require.NoError(t, err)
						require.Equal(t, want, got)
					}
					wantIt, err := ref.Fetch(ctx, r)
					require.NoError(t, err)
					want, err := fetch.Drain(ctx, wantIt)
					require.NoError(t, err)
					gotIt, err := e.Fetch(ctx, r)
					require.NoError(t, err)
					got, err := fetch.Drain(ctx, gotIt)
					require.NoError(t, err)
					for i := range want {
						want[i].Series = signal.Series{}
					}
					for i := range got {
						require.Equal(t, got[i].ID, got[i].Series.Hash())
						got[i].Series = signal.Series{}
					}
					require.Equal(t, want, got)
				}
			}
			for _, pt := range e.parts {
				require.Nil(t, pt.stats, "disk queries must not populate historical aggregate caches")
			}
		})
	}
}

func TestDiskCountTimestampOnlyAndBudgetCleanup(t *testing.T) {
	t.Parallel()
	files, err := backendfile.New(t.TempDir())
	require.NoError(t, err)
	counter := backendtest.NewSizedByteCounter(files)
	e := New(Config{Backend: counter, Prefix: "disk-count", DiskSeries: true,
		MetricBlockRows: 4, MetricSeriesChunks: true, DecodeCacheBytes: 1 << 20, DecodeBudget: NewDecodeBudget(1 << 20)})
	for i := range 32 {
		_, err := e.Append(apiSeries(), int64(i), float64(i))
		require.NoError(t, err)
	}
	require.NoError(t, e.Flush(t.Context()))
	disk := e.head.disk
	e.head = newHead()
	if disk == nil {
		disk = newDiskHeadState()
	}
	e.head.disk = disk
	pt := e.parts[0]
	require.NoError(t, files.Delete(t.Context(), pt.prefix+"/c/2"))
	for _, limit := range []int64{1, 1 << 20} {
		b := readbudget.New(limit)
		ctx := readbudget.With(t.Context(), b)
		n, err := e.Count(ctx, fetch.Request{Start: 3, End: 7})
		if limit == 1 {
			require.ErrorIs(t, err, readbudget.ErrExceeded)
		} else {
			require.NoError(t, err)
			require.Equal(t, 1, n)
		}
		require.Equal(t, limit, b.Remaining())
		require.Zero(t, pt.refs.Load())
	}
	counter.Reset()
	got, err := e.CountGrid(t.Context(), fetch.Request{Start: 0, End: 40},
		fetch.WindowGrid{Start: 4, Step: 4, Width: 3, NumSteps: 5}, nil)
	require.NoError(t, err)
	require.Equal(t, map[string][]int{"": {1, 1, 1, 1, 1}}, got)
	require.NotContains(t, counter.Report(), "/c/2")
	require.Zero(t, e.budget.inFlight())
	require.Zero(t, pt.refs.Load())
}

func TestDiskPlanPinsBeforeResolvingParts(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	e := New(Config{Backend: backend.Memory(), Prefix: "disk-snapshot", DiskSeries: true})
	a, b := diskQuerySeries(signal.StringValue([]byte("a"))), diskQuerySeries(signal.StringValue([]byte("b")))
	_, err := e.Append(a, 10, 1)
	require.NoError(t, err)
	require.NoError(t, e.Flush(ctx))
	disk := e.head.disk
	e.head = newHead()
	if disk == nil {
		disk = newDiskHeadState()
	}
	e.head.disk = disk
	_, err = e.Append(b, 20, 2)
	require.NoError(t, err)
	captured := make(chan struct{})
	resume := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		p, err := e.snapshotDiskQuery(ctx, fetch.Request{Start: 0, End: 100}, true, false, func(*enginePlan) error {
			close(captured)
			return nil
		})
		if err == nil {
			<-resume
			defer p.releaseParts()
			if len(p.ids) != 2 {
				done <- errors.New("snapshot omitted a series")
				return
			}
			for _, id := range p.ids {
				m, mergeErr := p.mergeSeries(ctx, id)
				if mergeErr != nil {
					err = mergeErr
					break
				}
				ts, _, _ := m.collect(nil, nil)
				if len(ts) != 1 || !slices.Contains([]int64{10, 20}, ts[0]) {
					err = errors.New("snapshot omitted or duplicated samples")
				}
			}
		}
		done <- err
	}()
	select {
	case <-captured:
	case err := <-done:
		require.NoError(t, err)
		return
	}
	defer func() { close(resume); require.NoError(t, <-done) }()
	require.NoError(t, e.Flush(ctx))
	require.NoError(t, e.Merge(ctx, 0))
}

func TestDiskQueryCorruptIndexCleanup(t *testing.T) {
	t.Parallel()
	be := backend.Memory()
	e := New(Config{Backend: be, Prefix: "corrupt-query", DiskSeries: true})
	_, err := e.Append(apiSeries(), 10, 1)
	require.NoError(t, err)
	require.NoError(t, e.Flush(t.Context()))
	pt := e.parts[0]
	require.NoError(t, be.Write(t.Context(), seriesIndexKey(pt.prefix), []byte("corrupt")))
	r := fetch.Request{Start: 0, End: 100}
	filtered := r
	filtered.Matchers = []fetch.Matcher{{Name: []byte("job"), Match: func(signal.Value) bool { return true }}}
	for name, query := range map[string]func(context.Context) error{
		"fetch": func(ctx context.Context) error {
			it, err := e.Fetch(ctx, r)
			if err == nil {
				err = it.Close()
			}
			return err
		},
		"series":    func(ctx context.Context) error { _, err := e.Series(ctx, r); return err },
		"count":     func(ctx context.Context) error { _, err := e.Count(ctx, filtered); return err },
		"labels":    func(ctx context.Context) error { _, err := e.LabelNames(ctx, r); return err },
		"values":    func(ctx context.Context) error { _, err := e.LabelValues(ctx, r, []byte("job")); return err },
		"aggregate": func(ctx context.Context) error { _, err := e.AggregateRange(ctx, r); return err },
		"grid": func(ctx context.Context) error {
			_, err := e.CountGrid(ctx, filtered, fetch.WindowGrid{Start: 10, Width: 20, Step: 10, NumSteps: 2}, nil)
			return err
		},
	} {
		b := readbudget.New(1 << 20)
		require.Error(t, query(readbudget.With(t.Context(), b)), name)
		require.EqualValues(t, 1<<20, b.Remaining(), name)
		require.Zero(t, pt.refs.Load(), name)
		require.Zero(t, e.budget.inFlight(), name)
	}
}

func TestDiskQuerySelectiveHintBudget(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	e := New(Config{Backend: backend.Memory(), Prefix: "selective-budget", DiskSeries: true})
	for i := range 2048 {
		s := diskQuerySeries(signal.StringValue([]byte(strconv.Itoa(i))))
		s.Resource.SchemaURL = bytes.Repeat([]byte("x"), 512)
		_, err := e.Append(s, 10, float64(i))
		require.NoError(t, err)
	}
	require.NoError(t, e.Flush(ctx))
	require.Zero(t, e.head.series.Len())
	equal := fetch.EqualMatcher{Name: "host", Value: "7"}
	r := fetch.Request{Start: 0, End: 100, Matchers: []fetch.Matcher{{Name: []byte("host"), Match: equal.Predicate(), StringValues: [][]byte{[]byte("7")}}}}
	b := readbudget.New(4 << 20)
	selectedCtx := readbudget.With(ctx, b)
	series, err := e.Series(selectedCtx, r)
	require.NoError(t, err)
	require.Len(t, series, 1)
	for _, slab := range e.planMaps.series.free {
		for _, s := range slab {
			require.Equal(t, signal.Series{}, s, "recycled slabs must not pin historical identities")
		}
	}
	require.EqualValues(t, 4<<20, b.Remaining())
	r.Matchers = nil
	_, err = e.Series(selectedCtx, r)
	require.ErrorIs(t, err, readbudget.ErrExceeded)
	require.EqualValues(t, 4<<20, b.Remaining())
	count, err := e.Count(selectedCtx, r)
	require.NoError(t, err, "broad timestamp-only counts do not retain identity headers")
	require.Equal(t, 2048, count)
	require.EqualValues(t, 4<<20, b.Remaining())
	names, err := e.LabelNames(selectedCtx, r)
	require.NoError(t, err)
	require.Equal(t, []string{"host", "job"}, names)
	values, err := e.LabelValues(selectedCtx, r, []byte("host"))
	require.NoError(t, err)
	require.Len(t, values, 2048)
	require.EqualValues(t, 4<<20, b.Remaining())
	for _, p := range e.parts {
		require.Zero(t, p.refs.Load())
	}
}

func TestDiskCardinalityScope(t *testing.T) {
	t.Parallel()
	e := New(Config{Backend: backend.Memory(), Prefix: "cardinality-scope", DiskSeries: true})
	_, err := e.Append(apiSeries(), 10, 1)
	require.NoError(t, err)
	require.NoError(t, e.Flush(t.Context()))
	cs := e.Cardinality(10)
	require.EqualValues(t, 1, cs.TotalSeries)
	require.True(t, cs.TotalSeriesComplete)
	require.Equal(t, "resident_head", cs.IndexScope)
	require.Zero(t, cs.DistinctLabelNames)
}

func TestDiskQueryReopen(t *testing.T) {
	t.Parallel()
	for _, legacy := range []bool{false, true} {
		t.Run(map[bool]string{false: "new-index", true: "legacy-index"}[legacy], func(t *testing.T) {
			t.Parallel()
			be := backend.Memory()
			cfg := Config{Backend: be, Prefix: "reopen-query", DiskSeries: !legacy}
			e := New(cfg)
			for _, host := range []string{"a", "b"} {
				_, err := e.Append(diskQuerySeries(signal.StringValue([]byte(host))), 10, 1)
				require.NoError(t, err)
			}
			require.NoError(t, e.Flush(t.Context()))
			cfg.DiskSeries = true
			reopened := New(cfg)
			require.NoError(t, reopened.LoadPartsReadOnly(t.Context()))
			require.Zero(t, reopened.head.series.Len())
			r := fetch.Request{Start: 0, End: 100}
			series, err := reopened.Series(t.Context(), r)
			require.NoError(t, err)
			require.Len(t, series, 2)
			count, err := reopened.Count(t.Context(), r)
			require.NoError(t, err)
			require.Equal(t, 2, count)
			values, err := reopened.LabelValues(t.Context(), r, []byte("host"))
			require.NoError(t, err)
			require.Equal(t, []string{"a", "b"}, values)
			cs := reopened.Cardinality(10)
			require.EqualValues(t, 2, cs.TotalSeries)
			require.True(t, cs.TotalSeriesComplete)
			require.Equal(t, "resident_head", cs.IndexScope)
			require.Zero(t, reopened.head.series.Len())
		})
	}
}

func TestDiskQueryFenced(t *testing.T) {
	t.Parallel()
	e := New(Config{Backend: backend.Memory(), Prefix: "query-fenced", DiskSeries: true})
	e.loadErr = errors.New("unreadable adopted part")
	r := fetch.Request{Start: 0, End: 100}
	b := readbudget.New(1 << 20)
	ctx := readbudget.With(t.Context(), b)
	_, err := e.Fetch(ctx, r)
	require.ErrorIs(t, err, bucketindex.ErrFenced)
	_, err = e.Series(ctx, r)
	require.ErrorIs(t, err, bucketindex.ErrFenced)
	_, err = e.Count(ctx, r)
	require.ErrorIs(t, err, bucketindex.ErrFenced)
	_, err = e.LabelNames(ctx, r)
	require.ErrorIs(t, err, bucketindex.ErrFenced)
	_, err = e.LabelValues(ctx, r, []byte("job"))
	require.ErrorIs(t, err, bucketindex.ErrFenced)
	_, err = e.AggregateRange(ctx, r)
	require.ErrorIs(t, err, bucketindex.ErrFenced)
	_, err = e.CountGrid(ctx, r, fetch.WindowGrid{Start: 10, Width: 20, Step: 10, NumSteps: 2}, nil)
	require.ErrorIs(t, err, bucketindex.ErrFenced)
	require.EqualValues(t, 1<<20, b.Remaining())
	require.False(t, e.Cardinality(10).TotalSeriesComplete)
}

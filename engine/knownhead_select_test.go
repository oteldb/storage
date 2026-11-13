package engine

import (
	"context"
	"strconv"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/readbudget"
	"github.com/oteldb/storage/signal"
)

func seedKnownSelection(t *testing.T, e *Engine, n int) []signal.Series {
	t.Helper()
	identities := make([]signal.Series, n)
	ids := make([]signal.SeriesID, n)
	ts := make([]int64, n)
	values := make([]float64, n)
	for i := range n {
		identities[i] = diskQuerySeries(signal.StringValue([]byte(strconv.Itoa(i))))
		ids[i] = identities[i].Hash()
		ts[i] = 10
		values[i] = 1
	}
	_, err := e.AppendBatch(ids, ts, values, nil, func(i int) signal.Series { return identities[i] }, AppendLimits{})
	require.NoError(t, err)
	require.NoError(t, e.Flush(t.Context()))
	return identities
}

func TestKnownHeadSelectiveCopies(t *testing.T) {
	t.Parallel()
	e := knownHeadEngine(t, backend.Memory())
	identities := seedKnownSelection(t, e, 256)
	const samples = 256
	ids := make([]signal.SeriesID, samples)
	ts := make([]int64, samples)
	values := make([]float64, samples)
	for _, s := range identities {
		for i := range samples {
			ids[i] = s.Hash()
			ts[i] = 100 + int64(i)
			values[i] = 1
		}
		_, err := e.AppendBatch(ids, ts, values, nil, func(int) signal.Series { return s }, AppendLimits{})
		require.NoError(t, err)
	}
	equal := fetch.EqualMatcher{Name: "host", Value: "42"}
	r := fetch.Request{Start: 100, End: 400, Matchers: []fetch.Matcher{{Name: []byte("host"), Match: equal.Predicate(), Spec: &equal}}}
	const limit = 1 << 20
	budget := readbudget.New(limit)
	ctx := readbudget.With(t.Context(), budget)
	plan, err := e.planDiskQuery(ctx, r, true, false, false)
	require.NoError(t, err)
	require.Len(t, plan.ids, 1)
	require.Equal(t, identities[42].Hash(), plan.ids[0])
	require.Len(t, plan.headB, 1, "unselected series must never enter the copied sample plan")
	require.Empty(t, plan.flushB)
	require.Empty(t, plan.liveParts, "old numeric samples are outside this window")
	require.Len(t, plan.headB[identities[42].Hash()].Timestamps, samples)
	plan.releaseParts()
	require.EqualValues(t, limit, budget.Remaining())
	count, err := e.Count(ctx, r)
	require.NoError(t, err)
	require.Equal(t, 1, count)
	labels, err := e.LabelValues(ctx, r, []byte("host"))
	require.NoError(t, err)
	require.Equal(t, []string{"42"}, labels)
	grid, err := e.CountGrid(ctx, r, fetch.WindowGrid{Start: 355, Step: 1, Width: 256, NumSteps: 1}, []byte("host"))
	require.NoError(t, err)
	require.Equal(t, map[string][]int{"42": {1}}, grid)
	require.EqualValues(t, limit, budget.Remaining())
	entered, resume := make(chan struct{}), make(chan struct{})
	var release sync.Once
	defer release.Do(func() { close(resume) })
	e.cfg.FlushAdmission = func(context.Context, int64) (func(), error) { close(entered); <-resume; return func() {}, nil }
	flushed := make(chan error, 1)
	go func() { flushed <- e.Flush(t.Context()) }()
	<-entered
	plan, err = e.planDiskQuery(ctx, r, true, false, false)
	require.NoError(t, err)
	require.Empty(t, plan.headB)
	require.Len(t, plan.flushB, 1, "frozen unselected samples must not be copied")
	plan.releaseParts()
	require.EqualValues(t, limit, budget.Remaining())
	release.Do(func() { close(resume) })
	require.NoError(t, <-flushed)
}

func TestKnownHeadSelectionConcurrentAdmission(t *testing.T) {
	t.Parallel()
	e := knownHeadEngine(t, backend.Memory())
	identities := seedKnownSelection(t, e, 64)
	knownHeadAppend(t, e, identities[0], 100, 1)
	entered, resume := make(chan struct{}), make(chan struct{})
	var once sync.Once
	matcher := fetch.Matcher{Name: []byte("job"), Match: func(v signal.Value) bool {
		once.Do(func() { close(entered); <-resume })
		return string(v.AppendText(nil)) == "api"
	}}
	r := fetch.Request{Start: 90, End: 110, Matchers: []fetch.Matcher{matcher}}
	type result struct {
		plan *enginePlan
		err  error
	}
	done := make(chan result, 1)
	go func() { p, err := e.planDiskQuery(t.Context(), r, true, false, false); done <- result{p, err} }()
	<-entered
	// Membership changes while immutable predicates are resolving must not restart selection.
	for _, s := range identities[1:] {
		knownHeadAppend(t, e, s, 100, 2)
	}
	close(resume)
	got := <-done
	require.NoError(t, got.err)
	defer got.plan.releaseParts()
	require.Len(t, got.plan.ids, len(identities))
	require.Len(t, got.plan.headB, len(identities))
}

func TestKnownHeadSelectionSnapshotRetry(t *testing.T) {
	t.Parallel()
	e := knownHeadEngine(t, backend.Memory())
	identities := seedKnownSelection(t, e, 2)
	knownHeadAppend(t, e, identities[0], 100, 1)
	equal := fetch.EqualMatcher{Name: "job", Value: "api"}
	r := fetch.Request{Start: 90, End: 110, Matchers: []fetch.Matcher{{Name: []byte("job"), Match: equal.Predicate(), Spec: &equal}}}
	const limit = 1 << 20
	budget := readbudget.New(limit)
	selected, err := e.selectKnownHead(t.Context(), r, budget)
	require.NoError(t, err)
	require.NoError(t, e.Flush(t.Context()))
	knownHeadAppend(t, e, identities[1], 100, 2)
	plan, retry, err := e.captureSelectedDiskHead(t.Context(), r, true, false, budget, nil, selected)
	require.NoError(t, err)
	require.True(t, retry)
	require.Nil(t, plan)
	selected.release()
	require.EqualValues(t, limit, budget.Remaining())
	plan, err = e.planDiskQuery(readbudget.With(t.Context(), budget), r, true, false, false)
	require.NoError(t, err)
	require.Len(t, plan.ids, 2)
	plan.releaseParts()
	require.EqualValues(t, limit, budget.Remaining())
}

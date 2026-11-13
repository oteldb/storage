package fetch_test

import (
	"context"
	"math"
	"testing"

	"github.com/go-faster/errors"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/query/fetch"
)

type disjointCounter struct {
	fetch.Fetcher

	counts  map[string][]int
	err     error
	calls   int
	before  func()
	request fetch.Request
	group   []byte
}

func (f *disjointCounter) CountGrid(_ context.Context, r fetch.Request, _ fetch.WindowGrid, group []byte) (map[string][]int, error) {
	f.calls++
	f.request = r
	f.group = group
	if f.before != nil {
		f.before()
	}
	return f.counts, f.err
}

func TestMergeDisjointCountGrid(t *testing.T) {
	t.Parallel()
	a := &disjointCounter{counts: map[string][]int{"a": {1, 0, 2}, "zero": {0, 0, 0}}}
	b := &disjointCounter{counts: map[string][]int{"a": {0, 3, 1}, "b": {0, 1, 0}}}
	f := fetch.MergeDisjoint(func() bool { return true }, a, b)
	grid := fetch.WindowGrid{Start: 10, Step: 2, Width: 5, NumSteps: 3}
	req := fetch.Request{Tenant: "tenant", Start: 2, End: 20, Scope: fetch.NewScope()}
	got, err := fetch.GridCounterOf(f).CountGrid(t.Context(), req, grid, []byte("job"))
	require.NoError(t, err)
	require.Equal(t, map[string][]int{"a": {1, 3, 3}, "b": {0, 1, 0}}, got)
	require.Equal(t, map[string][]int{"a": {1, 0, 2}, "zero": {0, 0, 0}}, a.counts)
	require.Equal(t, req, a.request)
	require.Equal(t, req, b.request)
	require.Equal(t, []byte("job"), b.group)
	require.Equal(t, 1, a.calls)
	require.Equal(t, 1, b.calls)
}

func TestMergeDisjointOverlapFallback(t *testing.T) {
	t.Parallel()
	// Two originals can map to one overflow ID after partitioning.
	a := &disjointCounter{Fetcher: fakeFetcher{batches: []*fetch.Batch{batch(7, [2]int64{10, 1})}}, counts: map[string][]int{"": {1}}}
	b := &disjointCounter{Fetcher: fakeFetcher{batches: []*fetch.Batch{batch(7, [2]int64{20, 2})}}, counts: map[string][]int{"": {1}}}
	f := fetch.MergeDisjoint(func() bool { return false }, a, b)
	_, err := fetch.GridCounterOf(f).CountGrid(t.Context(), fetch.Request{}, fetch.WindowGrid{Step: 1, Width: 1, NumSteps: 1}, nil)
	require.ErrorIs(t, err, fetch.ErrCountGridUnsupported)
	require.Zero(t, a.calls)
	require.Zero(t, b.calls)
	got := drain(t, f)
	require.Len(t, got, 1)
	require.Equal(t, []int64{10, 20}, got[0].Timestamps)
	require.Nil(t, fetch.GridCounterOf(fetch.Merge(a, b)))
	require.Nil(t, fetch.GridCounterOf(fetch.MergeDisjoint(nil, a, b)))
}

func TestMergeDisjointErrors(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name                          string
		counts                        map[string][]int
		err                           error
		missing, invalidates, cancels bool
	}{
		{name: "shape", counts: map[string][]int{"": {1, 2}}},
		{name: "negative", counts: map[string][]int{"": {-1}}},
		{name: "sum-overflow", counts: map[string][]int{"": {math.MaxInt}}},
		{name: "unsupported", err: fetch.ErrCountGridUnsupported},
		{name: "real-error", err: errors.New("read gap")},
		{name: "missing-capability", missing: true},
		{name: "proof-invalidated", counts: map[string][]int{"": {1}}, invalidates: true},
		{name: "cancellation", counts: map[string][]int{"": {1}}, cancels: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			safe := true
			a := &disjointCounter{counts: map[string][]int{"": {1}}}
			b := &disjointCounter{counts: tc.counts, err: tc.err}
			if tc.invalidates {
				b.before = func() { safe = false }
			}
			if tc.cancels {
				b.before = cancel
			}
			var child fetch.Fetcher = b
			if tc.missing {
				child = fakeFetcher{}
			}
			f := fetch.MergeDisjoint(func() bool { return safe }, a, child)
			got, err := fetch.GridCounterOf(f).CountGrid(ctx, fetch.Request{}, fetch.WindowGrid{Step: 1, Width: 1, NumSteps: 1}, nil)
			require.Error(t, err)
			require.Nil(t, got)
			if tc.err != nil {
				require.ErrorIs(t, err, tc.err)
			}
			if tc.invalidates || tc.missing {
				require.ErrorIs(t, err, fetch.ErrCountGridUnsupported)
			}
			if tc.missing {
				require.Zero(t, a.calls)
			}
			if tc.cancels {
				require.ErrorIs(t, err, context.Canceled)
			}
		})
	}
}

func FuzzMergeDisjointCountGrid(f *testing.F) {
	f.Add([]byte{0, 1, 3, 255})
	f.Add([]byte("count grid grouping"))
	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) == 0 || len(data) > 512 {
			return
		}
		a, b := make([]int, len(data)), make([]int, len(data))
		want := make([]int, len(data))
		positive := false
		for i, v := range data {
			a[i] = int(v % 8)
			b[i] = int(v / 8)
			want[i] = a[i] + b[i]
			positive = positive || want[i] != 0
		}
		merged := fetch.MergeDisjoint(func() bool { return true }, &disjointCounter{counts: map[string][]int{"": a}}, &disjointCounter{counts: map[string][]int{"": b}})
		got, err := fetch.GridCounterOf(merged).CountGrid(t.Context(), fetch.Request{}, fetch.WindowGrid{Step: 1, Width: 1, NumSteps: len(data)}, nil)
		if err != nil {
			t.Fatal(err)
		}
		if !positive {
			if len(got) != 0 {
				t.Fatal("zero group retained")
			}
			return
		}
		for i, n := range got[""] {
			if n != want[i] {
				t.Fatal("wrong count")
			}
		}
	})
}

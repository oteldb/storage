package fetch

import (
	"context"
	"math"
	"slices"

	"github.com/go-faster/errors"
)

// MergeDisjoint adds count-grid composition to [Merge] for caller-proven disjoint series.
// canCount certifies stable child ownership across every read, including overflow/remapping.
// It must be concurrency-safe. Invalidate before ownership changes and keep it false until older
// reads finish; merely avoiding simultaneous overlap is insufficient. Nil disables added counting.
func MergeDisjoint(canCount func() bool, fetchers ...Fetcher) Fetcher {
	children := slices.Clone(fetchers)
	base := Merge(children...)
	if canCount == nil || len(children) < 2 {
		return base
	}
	return disjointFetcher{Fetcher: base, children: children, canCount: canCount}
}

type disjointFetcher struct {
	Fetcher

	children []Fetcher
	canCount func() bool
}

func (f disjointFetcher) Unwrap() Fetcher { return f.Fetcher }

func (f disjointFetcher) CountGrid(ctx context.Context, r Request, grid WindowGrid, groupBy []byte) (map[string][]int, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if !f.canCount() {
		return nil, ErrCountGridUnsupported
	}
	if grid.NumSteps <= 0 || grid.Step <= 0 || grid.Width <= 0 {
		return nil, errors.New("invalid count grid")
	}
	counters := make([]GridCounter, len(f.children))
	for i, child := range f.children {
		counters[i] = GridCounterOf(child)
		if counters[i] == nil {
			return nil, ErrCountGridUnsupported
		}
	}
	out := make(map[string][]int)
	for i, counter := range counters {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if !f.canCount() {
			return nil, ErrCountGridUnsupported
		}
		counts, err := counter.CountGrid(ctx, r, grid, groupBy)
		if err != nil {
			return nil, errors.Wrapf(err, "count grid child %d", i)
		}
		if err := addGridCounts(out, counts, grid.NumSteps); err != nil {
			return nil, err
		}
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if !f.canCount() {
		return nil, ErrCountGridUnsupported
	}
	return out, nil
}

func addGridCounts(out, counts map[string][]int, steps int) error {
	for name, values := range counts {
		if len(values) != steps {
			return errors.New("count grid child returned an invalid shape")
		}
		dst := out[name]
		for step, n := range values {
			if n < 0 {
				return errors.New("count grid child returned a negative count")
			}
			if n == 0 {
				continue
			}
			if dst == nil {
				dst = make([]int, steps)
				out[name] = dst
			}
			if n > math.MaxInt-dst[step] {
				return errors.New("count grid overflow")
			}
			dst[step] += n
		}
	}
	return nil
}

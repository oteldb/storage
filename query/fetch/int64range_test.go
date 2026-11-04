package fetch_test

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/query/fetch"
)

func TestInt64RangeOverlaps(t *testing.T) {
	t.Parallel()
	bound := func(v int64, inclusive bool) *fetch.Int64Bound {
		return &fetch.Int64Bound{Value: v, Inclusive: inclusive}
	}
	for _, tt := range []struct {
		name     string
		r        fetch.Int64Range
		min, max int64
		want     bool
	}{
		{"empty_reversed", fetch.Int64Range{Lower: bound(3, true), Upper: bound(2, true)}, math.MinInt64, math.MaxInt64, false},
		{"empty_exclusive_lower", fetch.Int64Range{Lower: bound(2, false), Upper: bound(2, true)}, math.MinInt64, math.MaxInt64, false},
		{"empty_exclusive_upper", fetch.Int64Range{Lower: bound(2, true), Upper: bound(2, false)}, math.MinInt64, math.MaxInt64, false},
		{"equal_inclusive", fetch.Int64Range{Lower: bound(2, true), Upper: bound(2, true)}, math.MinInt64, math.MaxInt64, true},
		{"empty_unknown_stats", fetch.Int64Range{Lower: bound(3, true), Upper: bound(2, true)}, 10, -10, true},
		{"unbounded", fetch.Int64Range{}, math.MinInt64, math.MaxInt64, true},
		{"inclusive_lower", fetch.Int64Range{Lower: bound(2, true)}, 1, 2, true},
		{"exclusive_lower", fetch.Int64Range{Lower: bound(2, false)}, 1, 2, false},
		{"inclusive_upper", fetch.Int64Range{Upper: bound(2, true)}, 2, 3, true},
		{"exclusive_upper", fetch.Int64Range{Upper: bound(2, false)}, 2, 3, false},
		{"above_max", fetch.Int64Range{Lower: bound(math.MaxInt64, false)}, math.MinInt64, math.MaxInt64, false},
		{"below_min", fetch.Int64Range{Upper: bound(math.MinInt64, false)}, math.MinInt64, math.MaxInt64, false},
		{"precision", fetch.Int64Range{Lower: bound(1<<53+1, true)}, 1 << 53, 1 << 53, false},
		{"unknown", fetch.Int64Range{Lower: bound(100, true)}, 10, -10, true},
		{"disjoint_lower", fetch.Int64Range{Lower: bound(3, true)}, 0, 2, false},
		{"disjoint_upper", fetch.Int64Range{Upper: bound(-3, true)}, 0, 2, false},
	} {
		t.Run(tt.name, func(t *testing.T) { t.Parallel(); require.Equal(t, tt.want, tt.r.Overlaps(tt.min, tt.max)) })
	}
}

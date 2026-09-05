package bucketindex_test

import (
	"math"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/oteldb/storage/backend/bucketindex"
)

func TestWantOverlaps(t *testing.T) {
	t.Parallel()

	w := bucketindex.Want{Prefix: "p", MinTime: 100, MaxTime: 200}

	for _, tt := range []struct {
		name       string
		start, end int64
		want       bool
	}{
		{"wholly before", 0, 99, false},
		{"touching the low bound", 0, 100, true},
		{"inside", 120, 130, true},
		{"touching the high bound", 200, 400, true},
		{"wholly after", 201, 300, false},
		{"spanning", math.MinInt64, math.MaxInt64, true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.want, w.Overlaps(tt.start, tt.end))
		})
	}
}

// TestWantWithoutTimeBoundsCoversEverything: a want whose range is unset names a part of unknown
// extent. Reporting it as non-overlapping would let a read past a part nobody can bound.
func TestWantWithoutTimeBoundsCoversEverything(t *testing.T) {
	t.Parallel()

	w := bucketindex.Want{Prefix: "p"}

	assert.True(t, w.Overlaps(0, 0))
	assert.True(t, w.Overlaps(math.MaxInt64-1, math.MaxInt64))
}

func TestWantsOverlap(t *testing.T) {
	t.Parallel()

	wants := []bucketindex.Want{
		{Prefix: "a", MinTime: 100, MaxTime: 200},
		{Prefix: "b", MinTime: 900, MaxTime: 1000},
	}

	assert.False(t, bucketindex.WantsOverlap(nil, 0, math.MaxInt64))
	assert.False(t, bucketindex.WantsOverlap(wants, 300, 800), "between the two lost parts")
	assert.True(t, bucketindex.WantsOverlap(wants, 300, 950), "reaching the second")
}

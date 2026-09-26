package timebucket_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/internal/timebucket"
)

const (
	hour = int64(time.Hour)
	day  = 24 * hour
)

// TestLadderDivides is the invariant the ladder rests on: each level divides the next, so a part
// that fits level L still fits level L+1 and promotion never widens a part past its level.
func TestLadderDivides(t *testing.T) {
	t.Parallel()

	require.NotEmpty(t, timebucket.Ladder)

	for i, level := range timebucket.Ladder {
		require.Positive(t, level, "level %d must be positive", i)

		if i == 0 {
			continue
		}

		prev := timebucket.Ladder[i-1]
		require.Greater(t, level, prev, "levels must ascend")
		require.Zero(t, level%prev, "level %d (%s) must divide by %s", i, time.Duration(level), time.Duration(prev))
	}
}

// TestOfFloorsTowardNegativeInfinity pins the rounding: the naive ts-ts%level puts ts=-1 in the
// bucket starting at 0, the same bucket as ts=+1.
func TestOfFloorsTowardNegativeInfinity(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct{ ts, want int64 }{
		{0, 0},
		{1, 0},
		{hour - 1, 0},
		{hour, hour},
		{-1, -hour},
		{-hour, -hour},
		{-hour - 1, -2 * hour},
	} {
		assert.Equal(t, tt.want, timebucket.Of(tt.ts, hour), "Of(%d)", tt.ts)
	}
}

func TestEnd(t *testing.T) {
	t.Parallel()

	assert.Equal(t, day-1, timebucket.End(0, day))
	assert.Equal(t, 2*day-1, timebucket.End(day, day))
	assert.Equal(t, int64(-1), timebucket.End(-day, day))
	assert.Equal(t, int64(1<<63-1), timebucket.End(1<<63-2, day), "saturates rather than overflowing")
}

func TestFinest(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct {
		name   string
		lo, hi int64
		want   int64
		ok     bool
	}{
		{"within an hour", 0, hour - 1, hour, true},
		{"straddles an hour, within six", hour - 1, hour + 1, 6 * hour, true},
		{"straddles six hours, within a day", 6*hour - 1, 6*hour + 1, day, true},
		{"straddles a day", day - 1, day + 1, 0, false},
		{"wider than the top level", 0, 3 * day, 0, false},
		{"instant", 5 * hour, 5 * hour, hour, true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			level, ok := timebucket.Finest(tt.lo, tt.hi)
			assert.Equal(t, tt.ok, ok)

			if tt.ok {
				assert.Equal(t, time.Duration(tt.want), time.Duration(level))
			}
		})
	}
}

type span struct{ lo, hi, size int64 }

func bounds(s span) (lo, hi int64) { return s.lo, s.hi }

func sizeOf(s span) int64 { return s.size }

func TestSplitsFrom(t *testing.T) {
	t.Parallel()

	parts := []span{{lo: hour, hi: 2 * hour}, {lo: day + hour, hi: day + 2*hour}}

	assert.True(t, timebucket.SplitsFrom(parts, bounds, 0))
	assert.False(t, timebucket.SplitsFrom(parts, bounds, day), "retention drops the first day")
	assert.False(t, timebucket.SplitsFrom(parts[:1], bounds, 0))
}

// TestStraddlers pins the batch: only parts fitting no level, oldest first up to the byte and count
// bounds but never empty, returned in src order.
func TestStraddlers(t *testing.T) {
	t.Parallel()

	fits := span{lo: hour, hi: 2 * hour, size: 1}
	newer := span{lo: 3*day - hour, hi: 3*day + hour, size: 4}
	older := span{lo: day - hour, hi: day + hour, size: 4}
	oldest := span{lo: -hour, hi: hour, size: 4}
	src := []span{fits, newer, older, oldest}

	assert.Equal(t, []span{newer, older, oldest}, timebucket.Straddlers(src, bounds, sizeOf, 0, 0))
	assert.Equal(t, []span{older, oldest}, timebucket.Straddlers(src, bounds, sizeOf, 8, 0),
		"the cap keeps the oldest")
	assert.Equal(t, []span{oldest}, timebucket.Straddlers(src, bounds, sizeOf, 0, 1))
	assert.Equal(t, []span{oldest}, timebucket.Straddlers(src, bounds, sizeOf, 1, 0),
		"a straddler over the cap still goes alone")
	assert.Empty(t, timebucket.Straddlers([]span{fits}, bounds, sizeOf, 0, 0))
}

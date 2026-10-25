package timebucket_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/oteldb/storage/internal/timebucket"
)

func TestCanCompact(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name                  string
		lo, hi, level, newest int64
		want                  bool
	}{
		{name: "finest current bucket", lo: 0, hi: hour - 1, level: hour, newest: hour - 1, want: true},
		{name: "settled fine parts may promote", lo: 0, hi: hour - 1, level: 6 * hour, newest: 7 * hour, want: true},
		{name: "current fine parts wait", lo: 0, hi: hour - 1, level: 6 * hour, newest: 3 * hour},
		{name: "current wide parts may merge", lo: hour - 1, hi: hour + 1, level: 6 * hour, newest: 3 * hour, want: true},
		{name: "current day holds six hour parts", lo: 12 * hour, hi: 13 * hour, level: day, newest: 20 * hour},
		{name: "current day admits wider parts", lo: 16 * hour, hi: 20 * hour, level: day, newest: 23 * hour, want: true},
		{name: "level crossing excluded", lo: hour, hi: 7 * hour, level: 6 * hour, newest: 8 * hour},
		{name: "day straddler excluded", lo: day - 1, hi: day + 1, level: day, newest: day + 1},
		{name: "negative current bucket wide", lo: -8 * hour, hi: -4 * hour, level: day, newest: -hour, want: true},
		{name: "negative current bucket fine", lo: -4 * hour, hi: -3*hour - 1, level: 6 * hour, newest: -hour},
		{name: "unknown level", lo: 0, hi: hour, level: 2 * hour, newest: hour},
		{name: "zero level", lo: 0, hi: hour, newest: hour},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.want, timebucket.CanCompact(tc.lo, tc.hi, tc.level, tc.newest))
		})
	}
}

package tenant

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestDownsampleValidate(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name      string
		intervals []time.Duration
		ok        bool
	}{
		{name: "Empty", ok: true},
		{name: "Single7m", intervals: []time.Duration{7 * time.Minute}, ok: true},
		{name: "Nested", intervals: []time.Duration{time.Hour, time.Minute, 6 * time.Hour, 5 * time.Minute}, ok: true},
		{name: "DisabledIgnored", intervals: []time.Duration{time.Minute, 0, 7 * time.Minute * -1, time.Hour}, ok: true},
		{name: "NotDividing", intervals: []time.Duration{7 * time.Minute, time.Hour}},
		{name: "Duplicate", intervals: []time.Duration{time.Minute, time.Minute}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			var d Downsample
			for _, iv := range tc.intervals {
				d.Tiers = append(d.Tiers, DownsampleTier{After: time.Hour, Interval: iv})
			}

			if err := d.Validate(); tc.ok {
				assert.NoError(t, err)
			} else {
				assert.Error(t, err)
			}
		})
	}
}

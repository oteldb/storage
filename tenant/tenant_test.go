package tenant

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/oteldb/storage/signal"
)

func TestDownsampleValidate(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name      string
		intervals []time.Duration
		aggs      []signal.Aggregation
		ok        bool
	}{
		{name: "Empty", ok: true},
		{name: "Single7m", intervals: []time.Duration{7 * time.Minute}, ok: true},
		{name: "Nested", intervals: []time.Duration{time.Hour, time.Minute, 6 * time.Hour, 5 * time.Minute}, ok: true},
		{name: "DisabledIgnored", intervals: []time.Duration{time.Minute, 0, 7 * time.Minute * -1, time.Hour}, ok: true},
		{name: "NotDividing", intervals: []time.Duration{7 * time.Minute, time.Hour}},
		{name: "Duplicate", intervals: []time.Duration{time.Minute, time.Minute}},
		{
			name:      "OneAgg",
			intervals: []time.Duration{time.Minute, time.Hour},
			aggs:      []signal.Aggregation{signal.AggAvg, signal.AggAvg},
			ok:        true,
		},
		{
			name:      "MixedAgg",
			intervals: []time.Duration{time.Minute, time.Hour},
			aggs:      []signal.Aggregation{signal.AggSum, signal.AggMax},
		},
		{
			name:      "MixedAggOnDisabledTier",
			intervals: []time.Duration{time.Minute, 0, time.Hour},
			aggs:      []signal.Aggregation{signal.AggMin, signal.AggMax, signal.AggMin},
			ok:        true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			var d Downsample
			for i, iv := range tc.intervals {
				tier := DownsampleTier{After: time.Hour, Interval: iv}
				if tc.aggs != nil {
					tier.Agg = tc.aggs[i]
				}

				d.Tiers = append(d.Tiers, tier)
			}

			if err := d.Validate(); tc.ok {
				assert.NoError(t, err)
			} else {
				assert.Error(t, err)
			}
		})
	}
}

package storage

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPressureGenerationsOptions(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name            string
		generations     int
		async, separate bool
		want            string
	}{
		{name: "negative", generations: -1, want: "must be zero, one or two"},
		{name: "unbounded", generations: 3, want: "must be zero, one or two"},
		{name: "requires pipeline", generations: 2, want: "requires MetricAsyncPressureFlush"},
		{name: "separate series without chunks", generations: 2, async: true, separate: true, want: "MetricSeparateSeries requires MetricDiskSeries and MetricSeriesChunks"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			o := Options{MetricPressureGenerations: tc.generations, MetricAsyncPressureFlush: tc.async, MetricSeparateSeries: tc.separate}
			require.ErrorContains(t, o.validate(), tc.want)
		})
	}
}

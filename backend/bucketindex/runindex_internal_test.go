package bucketindex

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRunIndexMeetingInterval(t *testing.T) {
	t.Parallel()

	x := &runIndex{}
	for i, iv := range []Interval{
		Range(0, 4, 40),
		TermBlocks(0, 5, 7, 30),
		Range(0, 1, 1),
		Range(1, 1, 4),
		Range(2, 10, 20),
	} {
		x.add(iv, i)
	}

	x.build()

	for _, tt := range []struct {
		name string
		iv   Interval
	}{
		{name: "unset"},
		{name: "before", iv: Range(0, 2, 3)},
		{name: "boundary", iv: Range(0, 40, 41)},
		{name: "gapped", iv: TermBlocks(0, 1, 6, 10, 41)},
		{name: "prefix maximum", iv: TermBlocks(0, 1, 6, 7, 31, 40, 45)},
		{name: "terms", iv: TermBlocks(0, 5, 7).Union(TermBlocks(1, 1, 4))},
		{name: "after", iv: TermBlocks(3, 1, 3)},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var want, got []int
			for i := range tt.iv.nruns() {
				q := tt.iv.run(i)
				for _, run := range x.runs {
					if !run.Max.less(q.Min) && !q.Max.less(run.Min) {
						want = append(want, run.claim)
					}
				}
			}

			x.meetingInterval(tt.iv, func(i int) { got = append(got, i) })
			require.Equal(t, want, got)
		})
	}

	t.Run("empty index", func(t *testing.T) {
		t.Parallel()

		var empty runIndex
		empty.build()
		empty.meetingInterval(Range(0, 1, 40), func(int) { t.Fatal("empty index matched") })
	})
}

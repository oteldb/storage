package recordengine

import (
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestTSWindow(t *testing.T) {
	t.Parallel()

	// ts is ascending within a stream's part range (parts are (stream, ts)-sorted).
	ts := []int64{10, 20, 30, 40, 50}
	full := rowRange{start: 0, end: len(ts)}

	tests := []struct {
		name             string
		rng              rowRange
		start, end       int64
		wantLo, wantHigh int
	}{
		{"whole range in window", full, 0, 100, 0, 5},
		{"exact inclusive bounds", full, 20, 40, 1, 4},
		{"strict interior", full, 25, 35, 2, 3},
		{"single point on a value", full, 50, 50, 4, 5},
		{"all below window", full, 100, 200, 5, 5},
		{"all above window", full, -10, 5, 0, 0},
		{"open-ended upper (MaxInt64, no overflow)", full, 30, math.MaxInt64, 2, 5},
		{"empty input range", rowRange{start: 2, end: 2}, 0, 100, 2, 2},
		// Offset sub-range: only rows [1,4) = {20,30,40} are searched; the returned
		// bounds are absolute indices into ts.
		{"offset sub-range respects rng.start", rowRange{start: 1, end: 4}, 25, 100, 2, 4},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := tsWindow(ts, tt.rng, tt.start, tt.end)
			assert.Equal(t, tt.wantLo, got.start, "lo")
			assert.Equal(t, tt.wantHigh, got.end, "hi")
			assert.GreaterOrEqual(t, got.end, got.start, "window is well-formed")
			for i := got.start; i < got.end; i++ {
				assert.GreaterOrEqual(t, ts[i], tt.start, "row %d in window", i)
				assert.LessOrEqual(t, ts[i], tt.end, "row %d in window", i)
			}
		})
	}
}

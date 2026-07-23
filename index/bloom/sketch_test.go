package bloom

import (
	"math"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSketchEstimate(t *testing.T) {
	t.Parallel()

	for _, n := range []int{0, 1, 10, 100, 1_000, 10_000, 100_000, 1_000_000} {
		t.Run(strconv.Itoa(n), func(t *testing.T) {
			t.Parallel()

			var s Sketch
			for i := range n {
				s.Add([]byte("token-" + strconv.Itoa(i)))
			}

			got := s.Estimate()
			if n == 0 {
				require.Equal(t, 0, got)

				return
			}

			// HyperLogLog at 2^12 registers: ~1.6% standard error, so 10% is a wide safety band.
			relErr := math.Abs(float64(got-n)) / float64(n)
			assert.Less(t, relErr, 0.10, "n=%d estimate=%d", n, got)
		})
	}
}

// TestSketchRepeatsIgnored is the property the bloom sizing depends on: adding the same items over
// and over must not grow the estimate.
func TestSketchRepeatsIgnored(t *testing.T) {
	t.Parallel()

	var s Sketch
	for range 1000 {
		for i := range 500 {
			s.Add([]byte("word" + strconv.Itoa(i)))
		}
	}

	got := s.Estimate()
	assert.Less(t, math.Abs(float64(got-500))/500, 0.10, "estimate=%d", got)
}

func TestSketchReset(t *testing.T) {
	t.Parallel()

	var s Sketch
	for i := range 10_000 {
		s.Add([]byte(strconv.Itoa(i)))
	}

	s.Reset()
	assert.Equal(t, 0, s.Estimate())
}

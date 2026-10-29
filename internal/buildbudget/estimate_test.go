package buildbudget

import (
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSaturatingEstimates(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name     string
		a, b     int64
		add, mul int64
	}{
		{name: "zero", a: 0, b: 0},
		{name: "small", a: 2, b: 3, add: 5, mul: 6},
		{name: "saturated", a: math.MaxInt64, b: 2, add: math.MaxInt64, mul: math.MaxInt64},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.add, Add(tc.a, tc.b))
			assert.Equal(t, tc.mul, Mul(tc.a, tc.b))
		})
	}
	assert.Equal(t, int64(math.MaxInt64), Flush(math.MaxInt64, 1, 1, 1, 4, 0, 0))
	assert.Greater(t, Flush(100, 1000, 1000, 100, 4, 0, 1000), Flush(1, 1, 1, 1, 4, 0, 0))
}

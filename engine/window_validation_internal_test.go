package engine

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestWindowSpecValidate(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name  string
		step  int64
		valid bool
	}{
		{"negative", -1, false}, {"zero", 0, false}, {"positive", 1, true}, {"maximum", math.MaxInt64, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			err := (WindowSpec{Step: tc.step, Window: -1, Anchor: math.MinInt64}).Validate()
			if tc.valid {
				require.NoError(t, err)
			} else {
				require.ErrorIs(t, err, errWindowStep)
			}
		})
	}
}

package readbudget_test

import (
	"math"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/readbudget"
)

func TestReserveDoesNotOverflow(t *testing.T) {
	t.Parallel()
	for _, held := range []int64{1, 100, math.MaxInt64 - 1} {
		t.Run(strconv.FormatInt(held, 10), func(t *testing.T) {
			t.Parallel()
			b := readbudget.New(math.MaxInt64)
			require.NoError(t, b.Reserve(held))
			require.ErrorIs(t, b.Reserve(math.MaxInt64), readbudget.ErrExceeded)
			assert.Equal(t, int64(math.MaxInt64)-held, b.Remaining())
		})
	}
}

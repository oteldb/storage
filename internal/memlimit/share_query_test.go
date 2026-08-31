package memlimit

import (
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestQueryShare(t *testing.T) {
	t.Parallel()

	assert.Zero(t, QueryShare(-1), "a negative cap opts out, and opting out installs no limiter")
	assert.Equal(t, int64(1<<20), QueryShare(1<<20), "an explicit cap is taken as given")

	got := QueryShare(0)
	assert.Positive(t, got, "a zero cap derives a real bound rather than disabling it")
	assert.Less(t, got, int64(math.MaxInt64), "the derived bound is finite")

	if limit := Bytes(); limit > 0 {
		assert.Equal(t, limit/queryFraction, got, "the derived bound is a minority share of the process budget")
		assert.Less(t, got, limit, "one query may never read the whole process budget")
	}
}

package recordengine

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestSortByTsReusesOrderScratch: a buffer re-armed and sorted again sorts into the permutation it
// already holds instead of allocating one per sort.
func TestSortByTsReusesOrderScratch(t *testing.T) {
	t.Parallel()

	buf := headBuf(30, 10, 20)
	buf.sortByTs()
	require.Equal(t, []int64{10, 20, 30}, buf.ts)
	require.NotEmpty(t, buf.orderScratch)

	first := &buf.orderScratch[0]

	buf.prepare(headTestSchema, 0, fullSel(headTestSchema))
	for _, ts := range []int64{3, 1, 2} {
		buf.appendClone(headTestRec(ts))
	}

	buf.sortByTs()
	require.Equal(t, []int64{1, 2, 3}, buf.ts)
	assert.Same(t, first, &buf.orderScratch[0], "the second sort reuses the first sort's permutation array")
}

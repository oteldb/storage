package series

import (
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIndexSizeBytes(t *testing.T) {
	t.Parallel()

	ix := New()
	empty := ix.SizeBytes()

	ix.Add(mk("api", "route", "/x"))
	one := ix.SizeBytes()
	require.Greater(t, one, empty)

	ix.Add(mk("api", "route", "/x"))
	assert.Equal(t, one, ix.SizeBytes(), "a repeat identity stores nothing new")
}

func TestIndexSizeBytesCountsSharedIdentityOnce(t *testing.T) {
	t.Parallel()

	// Two identities sharing a resource and scope: the second pays only for its own entry and point
	// attributes, since the resource/scope sets and every repeated label byte are interned.
	shared := New()
	shared.Add(mk("api", "route", "/x"))
	first := shared.SizeBytes()
	shared.Add(mk("api", "route", "/y"))
	repeat := shared.SizeBytes() - first

	distinct := New()
	distinct.Add(mk("api", "route", "/x"))
	before := distinct.SizeBytes()
	distinct.Add(mk("billing", "route", "/y"))
	fresh := distinct.SizeBytes() - before

	assert.Less(t, repeat, fresh, "a shared resource is counted once, not per series")
}

func TestIndexSizeBytesScalesWithCardinality(t *testing.T) {
	t.Parallel()

	const n = 1000

	ix := New()
	for i := range n {
		ix.Add(mk("api", "route", "/"+strconv.Itoa(i)))
	}

	perSeries := ix.SizeBytes() / n
	assert.Positive(t, perSeries)
	assert.Less(t, perSeries, int64(4096), "a flat identity should not report kilobytes per series")
}

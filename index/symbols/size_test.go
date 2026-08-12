package symbols

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTableSizeBytes(t *testing.T) {
	t.Parallel()

	tab := New()
	empty := tab.SizeBytes()
	require.Positive(t, empty, "the lookup map's backing arrays are allocated up front")

	sym := []byte("service.name")
	tab.Intern(sym)
	one := tab.SizeBytes()
	assert.GreaterOrEqual(t, one-empty, int64(len(sym)), "the owned copy is counted")

	tab.Intern(sym)
	assert.Equal(t, one, tab.SizeBytes(), "re-interning stores nothing new")

	tab.Intern([]byte("service.namespace"))
	assert.Greater(t, tab.SizeBytes(), one)
}

func TestTableSizeBytesGrowsWithPayload(t *testing.T) {
	t.Parallel()

	short, long := New(), New()

	short.Intern([]byte("a"))
	long.Intern(make([]byte, 4096))

	assert.Greater(t, long.SizeBytes()-short.SizeBytes(), int64(4000),
		"a table interning larger symbols reports proportionally more")
}

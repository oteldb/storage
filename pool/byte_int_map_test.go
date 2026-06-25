package pool

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestByteIntMapBasic(t *testing.T) {
	t.Parallel()
	m := NewByteIntMap()
	defer m.PutBack()

	m.Put([]byte("hello"), 1)
	m.Put([]byte("world"), 2)
	m.Put([]byte("foo"), 3)

	v, ok := m.Get([]byte("hello"))
	assert.True(t, ok)
	assert.Equal(t, 1, v)

	v, ok = m.Get([]byte("world"))
	assert.True(t, ok)
	assert.Equal(t, 2, v)

	v, ok = m.Get([]byte("foo"))
	assert.True(t, ok)
	assert.Equal(t, 3, v)

	_, ok = m.Get([]byte("missing"))
	assert.False(t, ok, "Get(missing) should be not-found")

	assert.Equal(t, 3, m.Len())
}

func TestByteIntMapOverwrite(t *testing.T) {
	t.Parallel()
	m := NewByteIntMap()
	defer m.PutBack()

	old, existed := m.Put([]byte("key"), 10)
	require.False(t, existed, "first Put should not exist")
	require.Equal(t, 10, old)

	old, existed = m.Put([]byte("key"), 20)
	require.True(t, existed, "second Put should exist")
	require.Equal(t, 10, old, "second Put should return the old value")

	v, _ := m.Get([]byte("key"))
	assert.Equal(t, 20, v, "Get after overwrite")
	assert.Equal(t, 1, m.Len())
}

func TestByteIntMapPutOrGet(t *testing.T) {
	t.Parallel()
	m := NewByteIntMap()
	defer m.PutBack()

	_, existed := m.PutOrGet([]byte("a"), 0)
	require.False(t, existed, "first PutOrGet should not exist")

	v, existed := m.PutOrGet([]byte("a"), 1)
	require.True(t, existed, "second PutOrGet should exist")
	require.Equal(t, 0, v, "PutOrGet must return the existing value, not the new one")

	_, existed = m.PutOrGet([]byte("b"), 1)
	require.False(t, existed, "new key should not exist")
}

func TestByteIntMapDeleteProbeChain(t *testing.T) {
	t.Parallel()
	m := NewByteIntMap()
	defer m.PutBack()

	m.Put([]byte("a"), 1)
	m.Put([]byte("b"), 2)
	m.Put([]byte("c"), 3)

	require.True(t, m.Delete([]byte("b")), "Delete(b)")

	_, ok := m.Get([]byte("b"))
	assert.False(t, ok, "Get(b) after delete should be not-found")

	v, _ := m.Get([]byte("a"))
	assert.Equal(t, 1, v, "Get(a) after delete(b)")
	v, _ = m.Get([]byte("c"))
	assert.Equal(t, 3, v, "Get(c) after delete(b)")
	assert.Equal(t, 2, m.Len())

	// Delete a key in the middle of a probe chain to exercise backshift re-insert.
	m.Put([]byte("d"), 4)
	m.Put([]byte("e"), 5)
	m.Put([]byte("f"), 6)
	require.True(t, m.Delete([]byte("d")), "Delete(d)")

	v, _ = m.Get([]byte("e"))
	assert.Equal(t, 5, v, "Get(e) after delete(d)")
	v, _ = m.Get([]byte("f"))
	assert.Equal(t, 6, v, "Get(f) after delete(d)")

	_, ok = m.Get([]byte("d"))
	assert.False(t, ok, "Get(d) after delete should be not-found")
}

func TestByteIntMapDeleteMissing(t *testing.T) {
	t.Parallel()
	m := NewByteIntMap()
	defer m.PutBack()

	m.Put([]byte("x"), 1)
	assert.False(t, m.Delete([]byte("missing")), "Delete(missing) should return false")
}

func TestByteIntMapHashZeroKey(t *testing.T) {
	t.Parallel()
	m := NewByteIntMap()
	defer m.PutBack()

	// Verify that a key hashing to 0 is handled (hashKey remaps 0→1).
	m.Put([]byte{0}, 42)
	v, ok := m.Get([]byte{0})
	assert.True(t, ok)
	assert.Equal(t, 42, v)
}

func TestByteIntMapGrow(t *testing.T) {
	t.Parallel()
	m := NewByteIntMap()
	defer m.PutBack()

	for i := range 200 {
		m.Put([]byte("key-"+itoa(i)), i)
	}
	for i := range 200 {
		v, ok := m.Get([]byte("key-" + itoa(i)))
		require.Truef(t, ok, "Get(key-%d) missing", i)
		require.Equalf(t, i, v, "Get(key-%d)", i)
	}
	assert.Equal(t, 200, m.Len())
}

func TestByteIntMapReset(t *testing.T) {
	t.Parallel()
	m := NewByteIntMap()
	defer m.PutBack()

	m.Put([]byte("a"), 1)
	m.Reset()
	assert.Equal(t, 0, m.Len(), "Len after Reset")

	_, ok := m.Get([]byte("a"))
	assert.False(t, ok, "Get after Reset should be not-found")
}

func TestByteIntMapForEach(t *testing.T) {
	t.Parallel()
	m := NewByteIntMap()
	defer m.PutBack()

	m.Put([]byte("x"), 10)
	m.Put([]byte("y"), 20)

	seen := map[string]int{}
	m.ForEach(func(key []byte, value int) {
		seen[string(key)] = value
	})
	assert.Equal(t, map[string]int{"x": 10, "y": 20}, seen)
}

func TestByteIntMapCollision(t *testing.T) {
	t.Parallel()
	m := NewByteIntMap()
	defer m.PutBack()

	// Keys that are similar — verify linear probing works.
	keys := [][]byte{
		[]byte("alpha"), []byte("alphabet"), []byte("alpine"), []byte("alt"),
		[]byte("alphabet"), []byte("alpha"), // duplicates
	}
	for i, k := range keys {
		m.Put(k, i)
	}

	v, _ := m.Get([]byte("alpha"))
	assert.Equal(t, 5, v, "last Put at i=5 overwrites i=0")
	// "alphabet" was set at i=1 then overwritten at i=4.
	v, _ = m.Get([]byte("alphabet"))
	assert.Equal(t, 4, v)
}

func TestBytesEqualUsage(t *testing.T) {
	t.Parallel()
	// Verify the map uses bytes.Equal correctly via Get/Put behavior.
	m := NewByteIntMap()
	defer m.PutBack()

	m.Put([]byte("abc"), 1)
	_, ok := m.Get([]byte("abc"))
	assert.True(t, ok, "Get(abc) should find it")
	_, ok = m.Get([]byte("abd"))
	assert.False(t, ok, "Get(abd) should not find abc")
	_, ok = m.Get([]byte("ab"))
	assert.False(t, ok, "Get(ab) should not find abc (different length)")
}

func TestByteIntMapDeleteAllAndReinsert(t *testing.T) {
	t.Parallel()
	m := NewByteIntMap()
	defer m.PutBack()

	m.Put([]byte("a"), 1)
	m.Put([]byte("b"), 2)
	m.Put([]byte("c"), 3)
	// Delete all, then reinsert to exercise the full Delete backshift.
	m.Delete([]byte("a"))
	m.Delete([]byte("c"))
	m.Delete([]byte("b"))
	assert.Equal(t, 0, m.Len(), "Len after deleting all")

	m.Put([]byte("d"), 4)
	v, ok := m.Get([]byte("d"))
	assert.True(t, ok)
	assert.Equal(t, 4, v, "Get(d) after reinsert")
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var buf [20]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	return string(buf[i:])
}

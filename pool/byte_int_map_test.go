package pool

import (
	"testing"
)

func TestByteIntMapBasic(t *testing.T) {
	t.Parallel()
	m := NewByteIntMap()
	defer m.PutBack()

	m.Put([]byte("hello"), 1)
	m.Put([]byte("world"), 2)
	m.Put([]byte("foo"), 3)

	if v, ok := m.Get([]byte("hello")); !ok || v != 1 {
		t.Errorf("Get(hello) = %d, %v", v, ok)
	}
	if v, ok := m.Get([]byte("world")); !ok || v != 2 {
		t.Errorf("Get(world) = %d, %v", v, ok)
	}
	if v, ok := m.Get([]byte("foo")); !ok || v != 3 {
		t.Errorf("Get(foo) = %d, %v", v, ok)
	}
	if _, ok := m.Get([]byte("missing")); ok {
		t.Error("Get(missing) should be not-found")
	}
	if m.Len() != 3 {
		t.Errorf("Len = %d, want 3", m.Len())
	}
}

func TestByteIntMapOverwrite(t *testing.T) {
	t.Parallel()
	m := NewByteIntMap()
	defer m.PutBack()
	old, existed := m.Put([]byte("key"), 10)
	if existed || old != 10 {
		t.Fatalf("first Put: old=%d existed=%v", old, existed)
	}
	old, existed = m.Put([]byte("key"), 20)
	if !existed || old != 10 {
		t.Fatalf("second Put: old=%d existed=%v", old, existed)
	}
	if v, _ := m.Get([]byte("key")); v != 20 {
		t.Errorf("Get after overwrite = %d, want 20", v)
	}
	if m.Len() != 1 {
		t.Errorf("Len = %d, want 1", m.Len())
	}
}

func TestByteIntMapPutOrGet(t *testing.T) {
	t.Parallel()
	m := NewByteIntMap()
	defer m.PutBack()
	v, existed := m.PutOrGet([]byte("a"), 0)
	if existed {
		t.Fatal("first PutOrGet should not exist")
	}
	v, existed = m.PutOrGet([]byte("a"), 1)
	if !existed || v != 0 {
		t.Fatalf("second PutOrGet: v=%d existed=%v", v, existed)
	}
	v, existed = m.PutOrGet([]byte("b"), 1)
	if existed {
		t.Fatal("new key should not exist")
	}
}

func TestByteIntMapDeleteProbeChain(t *testing.T) {
	t.Parallel()
	m := NewByteIntMap()
	defer m.PutBack()
	m.Put([]byte("a"), 1)
	m.Put([]byte("b"), 2)
	m.Put([]byte("c"), 3)
	if !m.Delete([]byte("b")) {
		t.Fatal("Delete(b) returned false")
	}
	if _, ok := m.Get([]byte("b")); ok {
		t.Error("Get(b) after delete should be not-found")
	}
	if v, _ := m.Get([]byte("a")); v != 1 {
		t.Errorf("Get(a) after delete(b) = %d", v)
	}
	if v, _ := m.Get([]byte("c")); v != 3 {
		t.Errorf("Get(c) after delete(b) = %d", v)
	}
	if m.Len() != 2 {
		t.Errorf("Len = %d, want 2", m.Len())
	}
	// Delete a key in the middle of a probe chain to exercise backshift re-insert.
	m.Put([]byte("d"), 4)
	m.Put([]byte("e"), 5)
	m.Put([]byte("f"), 6)
	if !m.Delete([]byte("d")) {
		t.Fatal("Delete(d) returned false")
	}
	if v, _ := m.Get([]byte("e")); v != 5 {
		t.Errorf("Get(e) after delete(d) = %d", v)
	}
	if v, _ := m.Get([]byte("f")); v != 6 {
		t.Errorf("Get(f) after delete(d) = %d", v)
	}
	if _, ok := m.Get([]byte("d")); ok {
		t.Error("Get(d) after delete should be not-found")
	}
}

func TestByteIntMapDeleteMissing(t *testing.T) {
	t.Parallel()
	m := NewByteIntMap()
	defer m.PutBack()
	m.Put([]byte("x"), 1)
	if m.Delete([]byte("missing")) {
		t.Fatal("Delete(missing) should return false")
	}
}

func TestByteIntMapHashZeroKey(t *testing.T) {
	t.Parallel()
	m := NewByteIntMap()
	defer m.PutBack()
	// Verify that a key hashing to 0 is handled (hashKey remaps 0→1).
	m.Put([]byte{0}, 42)
	if v, ok := m.Get([]byte{0}); !ok || v != 42 {
		t.Errorf("Get([]byte{0}) = %d, %v", v, ok)
	}
}

func TestByteIntMapGrow(t *testing.T) {
	t.Parallel()
	m := NewByteIntMap()
	defer m.PutBack()
	for i := range 200 {
		m.Put([]byte("key-"+itoa(i)), i)
	}
	for i := range 200 {
		if v, ok := m.Get([]byte("key-" + itoa(i))); !ok || v != i {
			t.Errorf("Get(key-%d) = %d, %v", i, v, ok)
			break
		}
	}
	if m.Len() != 200 {
		t.Errorf("Len = %d, want 200", m.Len())
	}
}

func TestByteIntMapReset(t *testing.T) {
	t.Parallel()
	m := NewByteIntMap()
	defer m.PutBack()
	m.Put([]byte("a"), 1)
	m.Reset()
	if m.Len() != 0 {
		t.Errorf("Len after Reset = %d", m.Len())
	}
	if _, ok := m.Get([]byte("a")); ok {
		t.Error("Get after Reset should be not-found")
	}
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
	if len(seen) != 2 || seen["x"] != 10 || seen["y"] != 20 {
		t.Errorf("ForEach = %v", seen)
	}
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
	if v, _ := m.Get([]byte("alpha")); v != 5 {
		t.Errorf("alpha = %d, want 5 (last Put at i=5 overwrites i=0)", v)
	}
	// "alphabet" was set at i=1 then overwritten at i=4.
	if v, _ := m.Get([]byte("alphabet")); v != 4 {
		t.Errorf("alphabet = %d, want 4", v)
	}
}

func TestBytesEqualUsage(t *testing.T) {
	t.Parallel()
	// Verify the map uses bytes.Equal correctly via Get/Put behavior.
	m := NewByteIntMap()
	defer m.PutBack()
	m.Put([]byte("abc"), 1)
	if _, ok := m.Get([]byte("abc")); !ok {
		t.Error("Get(abc) should find it")
	}
	if _, ok := m.Get([]byte("abd")); ok {
		t.Error("Get(abd) should not find abc")
	}
	if _, ok := m.Get([]byte("ab")); ok {
		t.Error("Get(ab) should not find abc (different length)")
	}
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
	if m.Len() != 0 {
		t.Errorf("Len = %d after deleting all", m.Len())
	}
	m.Put([]byte("d"), 4)
	if v, ok := m.Get([]byte("d")); !ok || v != 4 {
		t.Errorf("Get(d) after reinsert = %d, %v", v, ok)
	}
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

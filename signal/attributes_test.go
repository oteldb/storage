package signal

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// Test helpers: literals are naturally strings; the package API is []byte.
func sv(s string) Value             { return StringValue([]byte(s)) }
func kv(k string, v Value) KeyValue { return KeyValue{Key: []byte(k), Value: v} }
func text(v Value) string           { return string(v.AppendText(nil)) }

func TestNewAttributesSorts(t *testing.T) {
	t.Parallel()

	a := NewAttributes(kv("zzz", sv("1")), kv("aaa", IntValue(2)), kv("mmm", BoolValue(true)))
	assert.Equal(t, []string{"aaa", "mmm", "zzz"}, []string{string(a[0].Key), string(a[1].Key), string(a[2].Key)})
}

func TestHashOrderIndependentByKey(t *testing.T) {
	t.Parallel()

	a := NewAttributes(kv("job", sv("api")), kv("inst", IntValue(1)))
	b := NewAttributes(kv("inst", IntValue(1)), kv("job", sv("api")))
	assert.Equal(t, a.Hash(), b.Hash())
}

// TestHashDistinguishesTypes is the crux of the OTel attribute spec: int 1, string "1",
// double 1.0, bytes and bool are different identities, not the same "label".
func TestHashDistinguishesTypes(t *testing.T) {
	t.Parallel()

	seen := map[SeriesID]string{}
	for _, tc := range []struct {
		name string
		v    Value
	}{
		{"int1", IntValue(1)},
		{"str1", sv("1")},
		{"double1", DoubleValue(1)},
		{"booltrue", BoolValue(true)},
		{"bytes1", BytesValue([]byte("1"))},
		{"empty", EmptyValue()},
	} {
		h := NewAttributes(kv("k", tc.v)).Hash()
		if prev, ok := seen[h]; ok {
			t.Fatalf("hash collision between %s and %s", tc.name, prev)
		}

		seen[h] = tc.name
	}
}

func TestHashEmptyVsEmptyStringDistinct(t *testing.T) {
	t.Parallel()

	empty := NewAttributes(kv("k", EmptyValue())).Hash()
	emptyStr := NewAttributes(kv("k", sv(""))).Hash()
	noKey := NewAttributes().Hash()

	assert.NotEqual(t, empty, emptyStr, "null value differs from empty string")
	assert.NotEqual(t, empty, noKey, "a present empty value differs from an absent key")
}

func TestHashMapOrderIndependent(t *testing.T) {
	t.Parallel()

	m1 := MapValue(kv("a", IntValue(1)), kv("b", IntValue(2)))
	m2 := MapValue(kv("b", IntValue(2)), kv("a", IntValue(1)))
	h1 := NewAttributes(kv("m", m1)).Hash()
	h2 := NewAttributes(kv("m", m2)).Hash()
	assert.Equal(t, h1, h2, "maps compare equal irrespective of order")
}

func TestHashSliceOrderDependent(t *testing.T) {
	t.Parallel()

	h1 := NewAttributes(kv("s", SliceValue(IntValue(1), IntValue(2)))).Hash()
	h2 := NewAttributes(kv("s", SliceValue(IntValue(2), IntValue(1)))).Hash()
	assert.NotEqual(t, h1, h2, "arrays are ordered")
}

func TestValueAccessors(t *testing.T) {
	t.Parallel()

	assert.Equal(t, KindStr, sv("x").Kind())
	assert.Equal(t, []byte("x"), sv("x").Str())
	assert.Nil(t, IntValue(1).Str(), "Str on a non-string is nil")
	assert.True(t, BoolValue(true).Bool())
	assert.Equal(t, int64(-7), IntValue(-7).Int())
	assert.InDelta(t, 3.5, DoubleValue(3.5).Double(), 0)
	assert.Equal(t, []byte("ab"), BytesValue([]byte("ab")).Bytes())
	assert.Nil(t, sv("x").Bytes(), "Bytes on a string is nil")
	assert.Len(t, SliceValue(IntValue(1), IntValue(2)).Slice(), 2)
	assert.Len(t, MapValue(kv("a", IntValue(1))).Map(), 1)
	assert.Equal(t, KindEmpty, EmptyValue().Kind())
}

func TestAttributesGet(t *testing.T) {
	t.Parallel()

	a := NewAttributes(kv("job", sv("api")))
	v, ok := a.Get([]byte("job"))
	assert.True(t, ok)
	assert.Equal(t, []byte("api"), v.Str())

	_, ok = a.Get([]byte("missing"))
	assert.False(t, ok)
}

func TestAttributesEqualAndClone(t *testing.T) {
	t.Parallel()

	a := NewAttributes(
		kv("s", SliceValue(IntValue(1), sv("x"))),
		kv("m", MapValue(kv("k", BytesValue([]byte("v"))))),
	)
	cp := a.Clone()
	assert.True(t, a.Equal(cp))

	// Mutating the clone's value must not affect the original.
	cp[0].Value = sv("changed")
	assert.False(t, a.Equal(cp))

	assert.Nil(t, Attributes(nil).Clone())
	assert.False(t, a.Equal(a[:1]))
}

func TestCloneIsDeep(t *testing.T) {
	t.Parallel()

	key := []byte("k")
	a := Attributes{{Key: key, Value: StringValue([]byte("orig"))}}
	cp := a.Clone()
	// Mutate the original's backing arrays; the clone must be unaffected.
	key[0] = 'X'
	a[0].Value.b[0] = 'Y'
	assert.Equal(t, []byte("k"), cp[0].Key)
	assert.Equal(t, []byte("orig"), cp[0].Value.Str())
}

func TestValueAppendText(t *testing.T) {
	t.Parallel()

	assert.Empty(t, text(EmptyValue()))
	assert.Equal(t, "hi", text(sv("hi")))
	assert.Equal(t, "true", text(BoolValue(true)))
	assert.Equal(t, "-7", text(IntValue(-7)))
	assert.Equal(t, "3.5", text(DoubleValue(3.5)))
	assert.Equal(t, "raw", text(BytesValue([]byte("raw"))))
	assert.Equal(t, "[1,2]", text(SliceValue(IntValue(1), IntValue(2))))
	assert.Equal(t, `{"a":1}`, text(MapValue(kv("a", IntValue(1)))))
	assert.Equal(t, `["x"]`, text(SliceValue(sv("x"))))
}

func FuzzAttributesHash(f *testing.F) {
	f.Add("job", "api", int64(1), true)

	f.Fuzz(func(t *testing.T, k1, v1 string, n int64, b bool) {
		// Distinct keys (the OTel spec requires uniqueness): "k1_*" can never equal
		// "k2"/"k3", so order-independence holds.
		a := NewAttributes(kv("k1_"+k1, sv(v1)), kv("k2", IntValue(n)), kv("k3", BoolValue(b)))
		shuffled := NewAttributes(a[2], a[0], a[1])
		assert.Equal(t, a.Hash(), shuffled.Hash())
		assert.True(t, a.Equal(a.Clone()))
	})
}

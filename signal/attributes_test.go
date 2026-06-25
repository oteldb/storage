package signal

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewAttributesSorts(t *testing.T) {
	t.Parallel()

	a := NewAttributes(
		KeyValue{"zzz", StringValue("1")},
		KeyValue{"aaa", IntValue(2)},
		KeyValue{"mmm", BoolValue(true)},
	)
	assert.Equal(t, []string{"aaa", "mmm", "zzz"}, []string{a[0].Key, a[1].Key, a[2].Key})
}

func TestHashOrderIndependentByKey(t *testing.T) {
	t.Parallel()

	a := NewAttributes(KeyValue{"job", StringValue("api")}, KeyValue{"inst", IntValue(1)})
	b := NewAttributes(KeyValue{"inst", IntValue(1)}, KeyValue{"job", StringValue("api")})
	assert.Equal(t, a.Hash(), b.Hash())
}

// TestHashDistinguishesTypes is the crux of using the OTel attribute spec: int 1,
// string "1", double 1.0 and bool are different identities, not the same "label".
func TestHashDistinguishesTypes(t *testing.T) {
	t.Parallel()

	hashes := map[SeriesID]string{}
	for _, tc := range []struct {
		name string
		v    Value
	}{
		{"int1", IntValue(1)},
		{"str1", StringValue("1")},
		{"double1", DoubleValue(1)},
		{"booltrue", BoolValue(true)},
		{"bytes1", BytesValue([]byte("1"))},
		{"empty", EmptyValue()},
	} {
		h := NewAttributes(KeyValue{"k", tc.v}).Hash()
		if prev, ok := hashes[h]; ok {
			t.Fatalf("hash collision between %s and %s", tc.name, prev)
		}

		hashes[h] = tc.name
	}
}

func TestHashEmptyVsEmptyStringDistinct(t *testing.T) {
	t.Parallel()

	empty := NewAttributes(KeyValue{"k", EmptyValue()}).Hash()
	emptyStr := NewAttributes(KeyValue{"k", StringValue("")}).Hash()
	noKey := NewAttributes().Hash()

	assert.NotEqual(t, empty, emptyStr, "null value differs from empty string")
	assert.NotEqual(t, empty, noKey, "a present empty value differs from an absent key")
}

func TestHashMapOrderIndependent(t *testing.T) {
	t.Parallel()

	m1 := MapValue(KeyValue{"a", IntValue(1)}, KeyValue{"b", IntValue(2)})
	m2 := MapValue(KeyValue{"b", IntValue(2)}, KeyValue{"a", IntValue(1)})
	h1 := NewAttributes(KeyValue{"m", m1}).Hash()
	h2 := NewAttributes(KeyValue{"m", m2}).Hash()
	assert.Equal(t, h1, h2, "maps compare equal irrespective of order")
}

func TestHashSliceOrderDependent(t *testing.T) {
	t.Parallel()

	s1 := SliceValue(IntValue(1), IntValue(2))
	s2 := SliceValue(IntValue(2), IntValue(1))
	h1 := NewAttributes(KeyValue{"s", s1}).Hash()
	h2 := NewAttributes(KeyValue{"s", s2}).Hash()
	assert.NotEqual(t, h1, h2, "arrays are ordered")
}

func TestValueAccessors(t *testing.T) {
	t.Parallel()

	assert.Equal(t, KindStr, StringValue("x").Kind())
	assert.Equal(t, "x", StringValue("x").Str())
	assert.True(t, BoolValue(true).Bool())
	assert.Equal(t, int64(-7), IntValue(-7).Int())
	assert.InDelta(t, 3.5, DoubleValue(3.5).Double(), 0)
	assert.Equal(t, []byte("ab"), BytesValue([]byte("ab")).Bytes())
	assert.Len(t, SliceValue(IntValue(1), IntValue(2)).Slice(), 2)
	assert.Len(t, MapValue(KeyValue{"a", IntValue(1)}).Map(), 1)
	assert.Equal(t, KindEmpty, EmptyValue().Kind())
}

func TestAttributesGet(t *testing.T) {
	t.Parallel()

	a := NewAttributes(KeyValue{"job", StringValue("api")})
	v, ok := a.Get("job")
	assert.True(t, ok)
	assert.Equal(t, "api", v.Str())

	_, ok = a.Get("missing")
	assert.False(t, ok)
}

func TestAttributesEqualAndClone(t *testing.T) {
	t.Parallel()

	a := NewAttributes(
		KeyValue{"s", SliceValue(IntValue(1), StringValue("x"))},
		KeyValue{"m", MapValue(KeyValue{"k", BytesValue([]byte("v"))})},
	)
	cp := a.Clone()
	assert.True(t, a.Equal(cp))

	// Mutating the clone's nested bytes must not affect the original.
	cp[0].Value = StringValue("changed")
	assert.False(t, a.Equal(cp))

	assert.Nil(t, Attributes(nil).Clone())
	assert.False(t, a.Equal(a[:1]))
}

func TestValueAsString(t *testing.T) {
	t.Parallel()

	assert.Empty(t, EmptyValue().AsString())
	assert.Equal(t, "hi", StringValue("hi").AsString())
	assert.Equal(t, "true", BoolValue(true).AsString())
	assert.Equal(t, "-7", IntValue(-7).AsString())
	assert.Equal(t, "3.5", DoubleValue(3.5).AsString())
	assert.Equal(t, "raw", BytesValue([]byte("raw")).AsString())
	assert.Equal(t, "[1,2]", SliceValue(IntValue(1), IntValue(2)).AsString())
	assert.Equal(t, `{"a":1}`, MapValue(KeyValue{"a", IntValue(1)}).AsString())
	assert.Equal(t, `["x"]`, SliceValue(StringValue("x")).AsString())
}

func FuzzAttributesHash(f *testing.F) {
	f.Add("job", "api", int64(1), true)

	f.Fuzz(func(t *testing.T, k1, v1 string, n int64, b bool) {
		// Distinct keys (the OTel spec requires uniqueness): "k1_*" can never equal
		// "k2"/"k3", so order-independence holds.
		a := NewAttributes(
			KeyValue{"k1_" + k1, StringValue(v1)},
			KeyValue{"k2", IntValue(n)},
			KeyValue{"k3", BoolValue(b)},
		)
		// Deterministic and order-independent across input orderings.
		shuffled := NewAttributes(a[2], a[0], a[1])
		assert.Equal(t, a.Hash(), shuffled.Hash())
		// Equal sets compare equal.
		assert.True(t, a.Equal(a.Clone()))
	})
}

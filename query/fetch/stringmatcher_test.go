package fetch_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/signal"
)

func TestStringSetMatcher(t *testing.T) {
	t.Parallel()
	set := fetch.StringSetMatcher{Name: "label", Values: fetch.AnyEqualStrings([]string{"foo", "bar"})}
	for _, tt := range []struct {
		name string
		v    signal.Value
		want bool
	}{
		{"member", signal.StringValue([]byte("foo")), true},
		{"nonmember", signal.StringValue([]byte("foobar")), false},
		{"empty string", signal.StringValue(nil), false},
		{"missing", signal.EmptyValue(), true},
		{"bytes", signal.BytesValue([]byte("other")), true},
		{"number", signal.IntValue(7), true},
		{"boolean", signal.BoolValue(true), true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.want, set.Matcher().Match(tt.v))
		})
	}
	assert.True(t, (fetch.StringSetMatcher{}).Matcher().Match(signal.StringValue([]byte("anything"))))
	assert.True(t, (fetch.StringSetMatcher{Values: fetch.AnyEqualStrings([]string{""})}).Matcher().Match(signal.StringValue(nil)))
}

func TestStringSpecs(t *testing.T) {
	t.Parallel()
	values := [][]byte{[]byte("foo"), []byte("bar"), []byte("foo")}
	matcher := fetch.Matcher{Name: []byte("label"), StringValues: values}
	specs := fetch.StringSpecs([]fetch.Matcher{{Name: []byte("opaque")}, matcher})
	require.Len(t, specs, 1)
	got := specs[0].Matcher()
	assert.Equal(t, fetch.AnyEqualStrings([]string{"bar", "foo"}), got.StringValues)
	assert.Equal(t, "foo", string(values[0]), "normalization does not mutate caller metadata")
	assert.Nil(t, got.Spec, "a necessary hint never claims exact equality")
}

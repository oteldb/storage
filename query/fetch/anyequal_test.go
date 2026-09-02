package fetch_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/signal"
)

func bs(vals ...string) [][]byte {
	out := make([][]byte, len(vals))
	for i, v := range vals {
		out[i] = []byte(v)
	}

	return out
}

func TestAnyEqualSet(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct {
		name string
		in   [][]byte
		want [][]byte
	}{
		{"nil", nil, nil},
		{"empty", [][]byte{}, nil},
		{"single", bs("b"), bs("b")},
		{"sorts", bs("c", "a", "b"), bs("a", "b", "c")},
		{"dedups", bs("b", "a", "b", "a"), bs("a", "b")},
		{"keeps the empty member", bs("b", ""), bs("", "b")},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := fetch.AnyEqualSet(tt.in)
			assert.Equal(t, tt.want, got)
			assert.True(t, fetch.SortedAnyEqual(got))
		})
	}
}

func TestAnyEqualSetDoesNotAliasTheInput(t *testing.T) {
	t.Parallel()

	in := bs("c", "a")
	out := fetch.AnyEqualSet(in)

	require.Equal(t, bs("a", "c"), out)
	assert.Equal(t, bs("c", "a"), in, "the caller's slice keeps its order")
}

func TestInAnyEqual(t *testing.T) {
	t.Parallel()

	set := fetch.AnyEqualStrings([]string{"delta", "alpha", "charlie"})
	require.Equal(t, bs("alpha", "charlie", "delta"), set)

	for _, v := range []string{"alpha", "charlie", "delta"} {
		assert.Truef(t, fetch.InAnyEqual(set, []byte(v)), "%q is a member", v)
	}

	for _, v := range []string{"", "bravo", "echo", "alph", "alphaa"} {
		assert.Falsef(t, fetch.InAnyEqual(set, []byte(v)), "%q is not a member", v)
	}

	assert.False(t, fetch.InAnyEqual(nil, []byte("alpha")), "nothing is a member of the empty set")
}

func TestAnyEqualPredicate(t *testing.T) {
	t.Parallel()

	pred := fetch.AnyEqualPredicate(fetch.AnyEqualStrings([]string{"7", "alpha"}))

	assert.True(t, pred(signal.StringValue([]byte("alpha"))))
	assert.False(t, pred(signal.StringValue([]byte("beta"))))
	assert.True(t, pred(signal.IntValue(7)), "a non-string value matches through its canonical text")
	assert.False(t, pred(signal.EmptyValue()), "an absent value is no member")
}

func TestSortedAnyEqual(t *testing.T) {
	t.Parallel()

	assert.True(t, fetch.SortedAnyEqual(nil))
	assert.True(t, fetch.SortedAnyEqual(bs("a")))
	assert.True(t, fetch.SortedAnyEqual(bs("a", "b")))
	assert.False(t, fetch.SortedAnyEqual(bs("b", "a")))
	assert.False(t, fetch.SortedAnyEqual(bs("a", "a")), "a duplicate is not the normalized shape")
}

func TestNormalizeConditions(t *testing.T) {
	t.Parallel()

	t.Run("returns the input when every set is normalized", func(t *testing.T) {
		t.Parallel()

		conds := []fetch.Condition{
			{Column: "a"},
			{Column: "b", AnyEqual: bs("x", "y")},
		}

		got := fetch.NormalizeConditions(conds)
		require.Len(t, got, 2)
		assert.Equal(t, conds[1].AnyEqual, got[1].AnyEqual)
	})

	t.Run("normalizes without touching the caller's slices", func(t *testing.T) {
		t.Parallel()

		unsorted := bs("y", "x", "y")
		conds := []fetch.Condition{{Column: "a", AnyEqual: unsorted}}

		got := fetch.NormalizeConditions(conds)
		require.Len(t, got, 1)
		assert.Equal(t, bs("x", "y"), got[0].AnyEqual)
		assert.Equal(t, bs("y", "x", "y"), unsorted, "the caller's set is untouched")
		assert.Equal(t, bs("y", "x", "y"), conds[0].AnyEqual, "the caller's conditions are untouched")
	})
}

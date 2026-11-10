package seriesindex

import (
	"bytes"
	"slices"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/signal"
)

func TestBuilderOwnsRepeatedPostingKeys(t *testing.T) {
	t.Parallel()
	var builder Builder
	src := make([]signal.Series, 4)
	borrowed := make([][]byte, 0, 12)
	for i := range src {
		name := []byte{'l', 0, 'n'}
		value := []byte{'v', 0, 'a'}
		borrowed = append(borrowed, name, value)
		typed := signal.StringValue(value)
		if i%2 != 0 {
			typed = signal.BytesValue(value)
		}
		src[i] = signal.Series{Attributes: signal.NewAttributes(
			signal.KeyValue{Key: name, Value: typed},
			signal.KeyValue{Key: []byte("id"), Value: signal.IntValue(int64(i))},
		)}
	}
	slices.SortFunc(src, func(a, b signal.Series) int { return a.Hash().Compare(b.Hash()) })
	ids := make([]signal.SeriesID, len(src))
	canonical := make([][]byte, len(src))
	var stringsOnly []signal.SeriesID
	for i, s := range src {
		ids[i] = s.Hash()
		canonical[i] = s.AppendHashInput(nil)
		for _, kv := range s.Attributes {
			if bytes.Equal(kv.Key, []byte{'l', 0, 'n'}) && kv.Value.Kind() == signal.KindStr {
				stringsOnly = append(stringsOnly, ids[i])
			}
		}
		require.NoError(t, builder.Add(ids[i], s, Metadata{Covered: int64(i)}))
	}
	for _, input := range borrowed {
		for i := range input {
			input[i] = 255
		}
	}
	for _, scratch := range [3][]byte{builder.canonicalScratch, builder.valueScratch, builder.postingScratch} {
		for i := range scratch {
			scratch[i] = 255
		}
	}
	var encoded bytes.Buffer
	require.NoError(t, builder.Write(&encoded))
	require.GreaterOrEqual(t, builder.EncodedBound(), int64(encoded.Len()))
	be := backend.Memory()
	require.NoError(t, be.Write(t.Context(), "owned", encoded.Bytes()))
	index, err := Open(t.Context(), be, "owned")
	require.NoError(t, err)
	for i, id := range ids {
		got, _, found, err := index.Lookup(t.Context(), id)
		require.NoError(t, err)
		require.True(t, found)
		require.Equal(t, canonical[i], got.AppendHashInput(nil))
	}
	refs, err := index.Resolve(t.Context(), []fetch.Matcher{{Name: []byte{'l', 0, 'n'}, Match: func(value signal.Value) bool {
		return value.Kind() == signal.KindStr && bytes.Equal(value.Str(), []byte{'v', 0, 'a'})
	}}})
	require.NoError(t, err)
	var selected []signal.SeriesID
	require.NoError(t, index.Visit(t.Context(), refs, func(id signal.SeriesID, _ []byte, _ Metadata) error { selected = append(selected, id); return nil }))
	require.Equal(t, stringsOnly, selected)
	require.NoError(t, refs.Err())
	require.Len(t, builder.names, 2)
	require.Len(t, builder.values, 6, "typed values and unique IDs retain separate postings")
}

func TestBuilderScratchCapacityIsPriced(t *testing.T) {
	t.Parallel()
	for _, name := range []string{"value", "posting"} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			var builder Builder
			before, encodedBefore := builder.Bytes(), builder.EncodedBound()
			const capacity = 4096
			if name == "value" {
				builder.valueScratch = make([]byte, 0, capacity)
			} else {
				builder.postingScratch = make([]byte, 0, capacity)
			}
			require.GreaterOrEqual(t, builder.Bytes()-before, int64(capacity))
			require.Equal(t, encodedBefore, builder.EncodedBound(), "unserialized scratch belongs to retained memory, not encoded output")
		})
	}
}

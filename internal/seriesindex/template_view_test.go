package seriesindex

import (
	"bytes"
	"slices"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/index/identity"
	"github.com/oteldb/storage/index/series"
	"github.com/oteldb/storage/signal"
)

func TestBuilderTemplateViewPreservesDiskBytes(t *testing.T) {
	t.Parallel()
	pool := series.NewAttributeTemplates(8<<20, []byte("instance"), []byte("cpu"))
	registry := series.NewWithAttributeTemplates(nil, pool)
	input := make([]signal.Series, 0, 50)
	for i := range 50 {
		s := signal.Series{Attributes: signal.NewAttributes(
			signal.KeyValue{Key: []byte("__name__"), Value: signal.StringValue([]byte("node_cpu_seconds_total"))},
			signal.KeyValue{Key: []byte("instance"), Value: signal.StringValue([]byte(strconv.Itoa(i)))},
			signal.KeyValue{Key: []byte("cpu"), Value: signal.IntValue(int64(i % 4))},
			signal.KeyValue{Key: []byte("mode"), Value: signal.StringValue([]byte("idle"))},
		)}
		input = append(input, s)
		registry.Add(s)
	}
	slices.SortFunc(input, func(a, b signal.Series) int { return a.Hash().Compare(b.Hash()) })
	var expected, actual Builder
	refs := make([]int, len(input))
	for i, s := range input {
		ref, ok := registry.Ref(s.Hash())
		require.True(t, ok)
		refs[i] = int(ref)
		require.NoError(t, expected.Add(s.Hash(), s, Metadata{Covered: 123}))
		require.NoError(t, actual.AddView(registry.View(), int(ref), Metadata{Covered: 123}))
	}
	var want, got bytes.Buffer
	require.NoError(t, expected.Write(&want))
	require.NoError(t, actual.Write(&got))
	require.Equal(t, want.Bytes(), got.Bytes(), "descriptor and posting format must remain identical")
	want.Reset()
	got.Reset()
	_, err := identity.WriteSorted(&want, len(input), func(i int, _ signal.Attributes) series.Entry {
		return series.Entry{ID: input[i].Hash(), Series: input[i]}
	})
	require.NoError(t, err)
	_, err = identity.WriteViewSorted(&got, len(input), registry.View(), func(i int) int { return refs[i] })
	require.NoError(t, err)
	require.Equal(t, want.Bytes(), got.Bytes(), "legacy identity output must remain identical")
}

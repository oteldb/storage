package seriesindex

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/index/series"
)

func TestBuilderCheckedHashDoesNotPublishMismatch(t *testing.T) {
	t.Parallel()
	input := builderViewFixture()
	var builder Builder
	s := input[0]
	wrong := s.Hash()
	wrong.Lo ^= 1
	require.ErrorContains(t, builder.Add(wrong, s, Metadata{}), "hash mismatch")
	require.Zero(t, builder.Count())
	require.Zero(t, builder.registry.Len())
	require.Empty(t, builder.names)
	require.Empty(t, builder.values)
	var reference Builder
	registry := series.New()
	for i := range input {
		registry.Add(input[i])
		meta := Metadata{Covered: int64(i) - 1, HasStats: true, Stats: Stats{Count: 2, Sum: 3, Min: 1, Max: 2}}
		require.NoError(t, builder.Add(input[i].Hash(), input[i], meta))
		require.NoError(t, reference.AddView(registry.View(), i, meta))
	}
	var actual, expected bytes.Buffer
	require.NoError(t, builder.Write(&actual))
	require.NoError(t, reference.Write(&expected))
	require.Equal(t, expected.Bytes(), actual.Bytes())
	require.Equal(t, reference.EncodedBound(), builder.EncodedBound())
	require.GreaterOrEqual(t, builder.Bytes(), int64(actual.Len()))
}

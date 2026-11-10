package seriesindex

import (
	"bytes"
	"math"
	"slices"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/index/identity"
	"github.com/oteldb/storage/index/series"
	"github.com/oteldb/storage/signal"
)

func builderViewFixture() []signal.Series {
	out := indexSeries()
	out = append(out, signal.Series{
		Resource: signal.Resource{SchemaURL: []byte("resource-schema"), Attributes: signal.NewAttributes(
			signal.KeyValue{Key: []byte("shared"), Value: signal.IntValue(5)},
		)},
		Scope: signal.Scope{Name: []byte("scope"), Version: []byte("v1"), SchemaURL: []byte("scope-schema"), Attributes: signal.NewAttributes(
			signal.KeyValue{Key: []byte("shared"), Value: signal.StringValue([]byte("5"))},
		)},
		Attributes: signal.NewAttributes(
			signal.KeyValue{Key: []byte("shared"), Value: signal.DoubleValue(5)},
			signal.KeyValue{Key: []byte("nested"), Value: signal.SliceValue(
				signal.EmptyValue(), signal.BoolValue(false), signal.BoolValue(true), signal.IntValue(math.MinInt64),
				signal.DoubleValue(math.Float64frombits(1<<63)), signal.DoubleValue(math.Float64frombits(0x7ff8000000001234)),
				signal.BytesValue([]byte{0, 255}), signal.MapValue(
					signal.KeyValue{Key: []byte("key"), Value: signal.StringValue([]byte("value"))},
				),
			)},
		),
	})
	slices.SortFunc(out, func(a, b signal.Series) int { return a.Hash().Compare(b.Hash()) })
	return out
}

func TestBuilderViewMatchesMaterialized(t *testing.T) {
	t.Parallel()
	for _, input := range [][]signal.Series{nil, {{}}, builderViewFixture()} {
		var expected, direct, mixed Builder
		registry := series.New()
		for i := range slices.Backward(input) {
			registry.Add(input[i])
		}
		view := registry.View()
		for i, s := range input {
			meta := Metadata{Covered: int64(i) - 5, HasStats: true, Stats: Stats{Count: 3, Sum: 6, Min: 1, Max: 3}}
			require.NoError(t, expected.Add(s.Hash(), s, meta))
			ref, ok := registry.Ref(s.Hash())
			require.True(t, ok)
			require.NoError(t, direct.AddView(view, int(ref), meta))
			if i%2 == 0 {
				require.NoError(t, mixed.AddView(view, int(ref), meta))
			} else {
				require.NoError(t, mixed.Add(s.Hash(), s, meta))
			}
		}
		var want, got bytes.Buffer
		require.NoError(t, expected.Write(&want))
		require.NoError(t, direct.Write(&got))
		require.Equal(t, want.Bytes(), got.Bytes(), "all descriptor, metadata and posting bytes stay identical")
		var interleaved bytes.Buffer
		require.NoError(t, mixed.Write(&interleaved))
		require.Equal(t, want.Bytes(), interleaved.Bytes(), "mixed insertion paths preserve row and symbol order")
		require.Equal(t, expected.EncodedBound(), direct.EncodedBound())
		require.GreaterOrEqual(t, direct.EncodedBound(), int64(got.Len()))
		require.GreaterOrEqual(t, direct.Bytes(), int64(got.Len()))
		want.Reset()
		got.Reset()
		_, err := identity.WriteSorted(&want, expected.Count(), expected.Entry)
		require.NoError(t, err)
		_, err = identity.WriteViewSorted(&got, direct.Count(), direct.View(), nil)
		require.NoError(t, err)
		require.Equal(t, want.Bytes(), got.Bytes(), "legacy identity format stays byte-identical too")
	}
}

func TestBuilderViewRejectsUnorderedReferences(t *testing.T) {
	t.Parallel()
	input := builderViewFixture()
	registry := series.New()
	for _, s := range input {
		registry.Add(s)
	}
	var builder Builder
	view := registry.View()
	require.NoError(t, builder.AddView(view, 1, Metadata{}))
	require.ErrorContains(t, builder.AddView(view, 1, Metadata{}), "not ordered")
	require.ErrorContains(t, builder.AddView(view, 0, Metadata{}), "not ordered")
	require.Equal(t, 1, builder.Count())
}

func FuzzBuilderViewMatchesMaterialized(f *testing.F) {
	input := builderViewFixture()
	for i := range input {
		f.Add(input[i].AppendHashInput(nil))
	}
	f.Fuzz(func(t *testing.T, raw []byte) {
		if len(raw) > 1<<16 {
			return
		}
		s, n, err := signal.DecodeSeries(raw)
		if err != nil || n != len(raw) {
			return
		}
		registry := series.New()
		id := registry.Add(s)
		var expected, direct Builder
		require.NoError(t, expected.Add(id, s, Metadata{Covered: 5}))
		require.NoError(t, direct.AddView(registry.View(), 0, Metadata{Covered: 5}))
		var want, got bytes.Buffer
		require.NoError(t, expected.Write(&want))
		require.NoError(t, direct.Write(&got))
		require.Equal(t, want.Bytes(), got.Bytes())
	})
}

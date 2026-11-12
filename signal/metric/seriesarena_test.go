package metric

import (
	"bytes"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/internal/memsize"
	"github.com/oteldb/storage/signal"
)

func legacyMetricSeries(id Identity) signal.Series {
	attrs := append(signal.Attributes(nil), id.Series.Attributes...)
	attrs = append(attrs,
		signal.KeyValue{Key: LabelName, Value: signal.StringValue(id.Name)},
		signal.KeyValue{Key: LabelUnit, Value: signal.StringValue(id.Unit)},
		signal.KeyValue{Key: LabelKind, Value: signal.IntValue(int64(id.Kind))},
		signal.KeyValue{Key: LabelTemporality, Value: signal.IntValue(int64(id.Temporality))},
		signal.KeyValue{Key: LabelMonotonic, Value: signal.BoolValue(id.Monotonic)})
	s := id.Series
	s.Attributes = signal.NewAttributes(attrs...)
	return s
}

func TestMetricAttributeMerge(t *testing.T) {
	t.Parallel()
	for _, attrs := range []signal.Attributes{
		nil,
		signal.NewAttributes(kv("instance", str("host")), kv("cpu", str("0"))),
		{kv("z", str("last")), kv("a", str("first"))},
		signal.NewAttributes(kv("__name__", str("point name")), kv("__name__", str("another")), kv("__unit__", str("point unit"))),
	} {
		id := Identity{Name: []byte("metric"), Unit: []byte("s"), Kind: KindSum, Temporality: TemporalityCumulative, Monotonic: true, Series: signal.Series{Attributes: attrs}}
		before := append(signal.Attributes(nil), attrs...)
		got := id.ToSeries()
		want := legacyMetricSeries(id)
		require.True(t, want.Equal(got))
		require.Equal(t, want.Hash(), got.Hash())
		require.Equal(t, before, attrs)
		prefix := signal.Attributes{kv("prefix", str("keep"))}
		result := id.AppendAttributes(prefix)
		require.Equal(t, prefix[0], result[0])
		require.Equal(t, want.Attributes, result[1:])
	}
}

func TestSeriesArenaBatchLifetime(t *testing.T) {
	t.Parallel()
	var arena SeriesArena
	got, want := make([]signal.Series, 0, 4096), make([]signal.Series, 0, 4096)
	for i := range 4096 {
		id := Identity{Name: []byte("metric-" + strconv.Itoa(i)), Series: signal.Series{Attributes: signal.NewAttributes(kv("host", str(strconv.Itoa(i))))}}
		got = append(got, arena.Materialize(id))
		want = append(want, legacyMetricSeries(id))
	}
	for i := range got {
		require.True(t, want[i].Equal(got[i]))
		require.Equal(t, want[i].Hash(), got[i].Hash())
	}
	arena.Reset()
	require.Empty(t, arena.attrs)
	require.Positive(t, cap(arena.attrs))
	for _, kv := range arena.attrs[:cap(arena.attrs)] {
		require.Equal(t, signal.KeyValue{}, kv, "pool must release input references")
	}
	arena.attrs = make(signal.Attributes, (8<<20)/memsize.Of[signal.KeyValue]()+1)
	arena.Reset()
	require.Nil(t, arena.attrs)
}

//nolint:paralleltest // AllocsPerRun changes process GOMAXPROCS.
func TestSeriesArenaWarmAllocation(t *testing.T) {
	id := Identity{Name: []byte("cpu"), Series: signal.Series{Attributes: signal.NewAttributes(kv("host", str("a")))}}
	var arena SeriesArena
	arena.Materialize(id)
	arena.Reset()
	require.Zero(t, testing.AllocsPerRun(100, func() { arena.Materialize(id); arena.Reset() }))
}

func FuzzMetricAttributeMerge(f *testing.F) {
	f.Add([]byte("host\x00__name__\x00host"))
	f.Add([]byte{0, 1, 3, 4, 6, 7, 8, 9})
	f.Fuzz(func(t *testing.T, input []byte) {
		if len(input) > 512 {
			t.Skip()
		}
		keys := [][]byte{[]byte("host"), LabelName, LabelUnit, LabelKind, LabelMonotonic, LabelTemporality, []byte("z")}
		attrs := make(signal.Attributes, 0, len(input))
		for i, b := range input {
			attrs = append(attrs, signal.KeyValue{Key: keys[int(b)%len(keys)], Value: signal.IntValue(int64(i))})
		}
		id := Identity{Name: input, Unit: []byte("s"), Kind: KindSum, Monotonic: true, Series: signal.Series{Attributes: attrs}}
		got, want := id.ToSeries(), legacyMetricSeries(id)
		if !bytes.Equal(got.AppendHashInput(nil), want.AppendHashInput(nil)) {
			t.Fatal("canonical labels differ")
		}
	})
}

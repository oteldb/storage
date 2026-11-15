package buildbudget

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/signal"
)

func seriesEncodingCases() []struct {
	name  string
	value signal.Value
} {
	return []struct {
		name  string
		value signal.Value
	}{
		{name: "empty"},
		{name: "string", value: signal.StringValue([]byte(strings.Repeat("x", 129)))},
		{name: "bytes", value: signal.BytesValue([]byte{0, 1, 255})},
		{name: "bool", value: signal.BoolValue(true)},
		{name: "int", value: signal.IntValue(-123)},
		{name: "double", value: signal.DoubleValue(1.25)},
		{name: "nested", value: signal.SliceValue(signal.IntValue(1), signal.MapValue(
			signal.KeyValue{Key: []byte("key"), Value: signal.StringValue([]byte("value"))},
		))},
	}
}

func budgetSeries(v signal.Value) signal.Series {
	attrs := signal.NewAttributes(signal.KeyValue{Key: []byte("field"), Value: v})
	return signal.Series{
		Resource:   signal.Resource{SchemaURL: []byte("resource-schema"), Attributes: attrs},
		Scope:      signal.Scope{Name: []byte("scope"), Version: []byte("version"), SchemaURL: []byte("scope-schema"), Attributes: attrs},
		Attributes: attrs,
	}
}

func TestSeriesEncodingBound(t *testing.T) {
	t.Parallel()
	for _, tc := range seriesEncodingCases() {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			s := budgetSeries(tc.value)
			require.GreaterOrEqual(t, SeriesEncoding(s), int64(len(s.AppendHashInput(nil))))
		})
	}
	require.GreaterOrEqual(t, SeriesEncoding(signal.Series{}), int64(len((signal.Series{}).AppendHashInput(nil))))
}

//nolint:paralleltest // AllocsPerRun changes the process concurrency setting.
func TestSeriesEncodingAllocations(t *testing.T) {
	for _, tc := range seriesEncodingCases() {
		s := budgetSeries(tc.value)
		require.Zero(t, testing.AllocsPerRun(100, func() { _ = SeriesEncoding(s) }), tc.name)
	}
}

func FuzzSeriesEncodingBound(f *testing.F) {
	for _, tc := range seriesEncodingCases() {
		f.Add(budgetSeries(tc.value).AppendHashInput(nil))
	}
	f.Add((signal.Series{}).AppendHashInput(nil))
	f.Fuzz(func(t *testing.T, data []byte) {
		s, _, err := signal.DecodeSeries(data)
		if err != nil {
			return
		}
		if got, want := SeriesEncoding(s), int64(len(s.AppendHashInput(nil))); got < want {
			t.Fatalf("encoding bound %d is smaller than %d encoded bytes", got, want)
		}
	})
}

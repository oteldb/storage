package series

import (
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/signal"
)

func bucketInput() []signal.Series {
	bounds := []string{"0.005", "0.01", "0.025", "0.05", "0.1", "0.5", "1", "+Inf"}
	input := make([]signal.Series, 0, len(bounds)*1024)
	for host := range 1024 {
		for _, bound := range bounds {
			input = append(input, signal.Series{Attributes: signal.NewAttributes(
				signal.KeyValue{Key: []byte("__name__"), Value: signal.StringValue([]byte("request_duration_seconds_bucket"))},
				signal.KeyValue{Key: []byte("instance"), Value: signal.StringValue([]byte(strconv.Itoa(host)))},
				signal.KeyValue{Key: []byte("job"), Value: signal.StringValue([]byte("synthetic-histogram"))},
				signal.KeyValue{Key: []byte("le"), Value: signal.StringValue([]byte(bound))},
				signal.KeyValue{Key: []byte("method"), Value: signal.StringValue([]byte("GET"))},
			)})
		}
	}
	return input
}

func TestTemplateBucketVariability(t *testing.T) {
	t.Parallel()
	pool := NewAttributeTemplates(8<<20, []byte("instance"), []byte("le"))
	input := bucketInput()
	trainExporter(pool, input, false)
	ix := NewWithAttributeTemplates(nil, pool)
	for i := range input {
		id := ix.Add(input[i])
		require.Equal(t, input[i].Hash(), id)
		require.Equal(t, input[i].AppendHashInput(nil), ix.View().AppendCanonical(nil, i))
	}
	require.Len(t, ix.templateRows, 1)
	require.EqualValues(t, 2, ix.templateRows[0].dynamic)
}

// BenchmarkTemplateBucketAxis uses a labeled synthetic histogram, separate from the capture.
func BenchmarkTemplateBucketAxis(b *testing.B) {
	input := bucketInput()
	for _, variable := range []bool{false, true} {
		pool := NewAttributeTemplates(8<<20, []byte("instance"))
		if variable {
			pool = NewAttributeTemplates(8<<20, []byte("instance"), []byte("le"))
		}
		trainExporter(pool, input, false)
		b.Run("variable-bucket="+strconv.FormatBool(variable), func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				ix := NewWithAttributeTemplates(nil, pool)
				for i := range input {
					ix.Add(input[i])
				}
			}
		})
	}
}

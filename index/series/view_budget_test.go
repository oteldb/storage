package series

import (
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/signal"
)

func TestViewEncodingBudgetIncludesSharedPayloads(t *testing.T) {
	t.Parallel()
	for _, size := range []int{16, 64 << 10} {
		t.Run(strconv.Itoa(size), func(t *testing.T) {
			t.Parallel()
			ix := New()
			resource := signal.Resource{Attributes: signal.NewAttributes(signal.KeyValue{
				Key: []byte("shared"), Value: signal.StringValue([]byte(strings.Repeat("x", size))),
			})}
			for i := range 100 {
				ix.Add(signal.Series{Resource: resource, Attributes: signal.NewAttributes(signal.KeyValue{
					Key: []byte("id"), Value: signal.IntValue(int64(i)),
				})})
			}
			view := ix.View()
			require.Equal(t, 100, view.Len())
			assert.Greater(t, view.EncodingWorkspace(), int64(size))
			assert.Equal(t, view.EncodingBytes(0), view.EncodingBytes(99), "shared contexts still have per-series references")
			assert.Less(t, view.EncodingBytes(0), int64(1024), "raw shared payload is charged once, separately")
		})
	}
}

func TestViewMaterializationBudgetIncludesNestedValues(t *testing.T) {
	t.Parallel()
	ix := New()
	values := make([]signal.Value, 1024)
	for i := range values {
		values[i] = signal.IntValue(int64(i))
	}
	ix.Add(signal.Series{Attributes: signal.NewAttributes(signal.KeyValue{
		Key: []byte("nested"), Value: signal.SliceValue(values...),
	})})
	assert.Greater(t, ix.View().MaterializationBytes(0), int64(len(values))*64)
}

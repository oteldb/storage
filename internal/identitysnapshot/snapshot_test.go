package identitysnapshot

import (
	"slices"
	"strconv"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/oteldb/storage/index/series"
	"github.com/oteldb/storage/signal"
)

func TestCaptureAllowsRegistrationBetweenBatches(t *testing.T) {
	t.Parallel()
	var mu sync.RWMutex
	index := series.New()
	identity := func(i int) signal.Series {
		return signal.Series{Attributes: signal.NewAttributes(signal.KeyValue{
			Key: []byte("id"), Value: signal.StringValue([]byte(strconv.Itoa(i))),
		})}
	}
	ids := make([]signal.SeriesID, 3*batchSize+1)
	for i := range ids {
		ids[i] = index.Add(identity(i))
	}
	var got []signal.SeriesID
	Capture(mu.RLocker(), func() *series.Index { return index }, slices.Values(ids),
		func(id signal.SeriesID, view series.View, ref uint32) {
			assert.Equal(t, len(ids), view.Len())
			assert.Equal(t, id, view.At(int(ref), nil).Hash())
			// emit is outside the reader lock; additions cannot change captured identities.
			mu.Lock()
			index.Add(identity(len(ids) + len(got)))
			mu.Unlock()
			got = append(got, id)
		})
	assert.Equal(t, ids, got)
}

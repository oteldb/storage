package metric

import (
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/signal"
)

func TestSourceSurvivesProjectionReuse(t *testing.T) {
	t.Parallel()
	var md Metrics
	for resource := range 2 {
		rm := md.AddResource()
		rm.Resource.Attributes = signal.NewAttributes(signal.KeyValue{
			Key: []byte("resource"), Value: signal.StringValue([]byte(strconv.Itoa(resource))),
		})
		for scope := range 2 {
			sm := rm.AddScope()
			sm.Scope.Name = []byte("scope-" + strconv.Itoa(scope))
			for name := range 3 {
				m := sm.AddMetric()
				m.Name = []byte("metric-" + strconv.Itoa(name))
				m.Unit = []byte("s")
				m.Kind = KindSum
				m.Temporality = TemporalityCumulative
				m.Monotonic = true
				for point := range 2 {
					p := m.AddPoint()
					p.Attributes = signal.NewAttributes(signal.KeyValue{
						Key: []byte("point"), Value: signal.IntValue(int64(point)),
					})
					p.Ts, p.Value = int64(point), float64(point)
				}
			}
		}
	}
	type captured struct {
		source Source
		ids    []signal.SeriesID
	}
	var captures []captured
	Project(md, func(batch *Batch) {
		captures = append(captures, captured{source: batch.Source(), ids: append([]signal.SeriesID(nil), batch.IDs...)})
	})
	require.Len(t, captures, 12)
	for _, c := range captures {
		for i, id := range c.ids {
			assert.Equal(t, id, c.source.Series(i).Hash())
		}
	}
}

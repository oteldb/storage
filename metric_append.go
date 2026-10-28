package storage

import (
	"sync"

	"github.com/oteldb/storage/engine"
	"github.com/oteldb/storage/signal"
	"github.com/oteldb/storage/signal/metric"
)

const metricAppendPoints = 4096

type metricPointRef struct {
	source int
	point  int
}

type metricAppendBuffer struct {
	ids     []signal.SeriesID
	ts      []int64
	values  []float64
	refs    []metricPointRef
	sources []metric.Source
}

var metricAppendPool = sync.Pool{New: func() any { return &metricAppendBuffer{} }}

func (b *metricAppendBuffer) reset() {
	clear(b.sources)
	b.sources = b.sources[:0]
	b.ids, b.ts, b.values, b.refs = b.ids[:0], b.ts[:0], b.values[:0], b.refs[:0]
}

func (b *metricAppendBuffer) append(batch *metric.Batch) {
	n := batch.Len()
	source := len(b.sources)
	b.sources = append(b.sources, batch.Source())
	b.ids = append(b.ids, batch.IDs...)
	b.ts = append(b.ts, batch.Ts...)
	b.values = append(b.values, batch.Values...)
	for i := range n {
		b.refs = append(b.refs, metricPointRef{source: source, point: i})
	}
}

func (b *metricAppendBuffer) series(i int) signal.Series {
	r := b.refs[i]
	return b.sources[r.source].Series(r.point)
}

func (b *metricAppendBuffer) flush(eng *engine.Engine) (engine.AppendResult, error) {
	res, err := eng.AppendBatch(b.ids, b.ts, b.values, nil, b.series, engine.AppendLimits{})
	b.reset()
	return res, err
}

func (r metricRun) canBatch() bool {
	rateOrSampling := r.limits.IngestBytesPerSecond != 0 || r.sampling != 0
	boundedAdmission := r.limits.MaxSeries != 0 || r.limits.MaxSeriesSoft != 0 || r.limits.MaxInFlightBytes != 0
	return !rateOrSampling && !boundedAdmission
}

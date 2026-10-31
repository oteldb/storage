package pdataconv

import (
	"go.opentelemetry.io/collector/pdata/pmetric"

	"github.com/oteldb/storage/internal/memsize"
	"github.com/oteldb/storage/signal"
	"github.com/oteldb/storage/signal/metric"
)

// MetricsProjector hashes pdata points before materializing their attributes on demand.
// Its zero value is usable; input and projector must not change until the storage write returns.
type MetricsProjector struct {
	input       pmetric.Metrics
	prepared    bool
	dropped     Dropped
	hash        metricsHash
	batch       metric.Batch
	groups      []metricContext
	metrics     []metricInput
	points      []int
	materialize *metric.Materializer
}

const maxProjectorRetainedBytes = 4 << 20

type metricContext struct {
	resource signal.Resource
	scope    signal.Scope
}

type metricInput struct {
	metric      pmetric.Metric
	group       int
	pointOffset int
	name        []byte
	unit        []byte
	metadata    bool
}

// Prepare borrows md, rejecting the entire request if any metric is not gauge/sum or has exemplars.
// Call Reset after the enclosing write; a false result requires the existing AppendMetrics path.
func (p *MetricsProjector) Prepare(md pmetric.Metrics) bool {
	p.Reset()
	var dropped Dropped
	rms := md.ResourceMetrics()
	for ri := range rms.Len() {
		scopes := rms.At(ri).ScopeMetrics()
		for si := range scopes.Len() {
			metrics := scopes.At(si).Metrics()
			for mi := range metrics.Len() {
				m := metrics.At(mi)
				if m.Type() != pmetric.MetricTypeGauge && m.Type() != pmetric.MetricTypeSum {
					return false
				}
				points := numberPoints(m)
				for pi := range points.Len() {
					point := points.At(pi)
					if point.Exemplars().Len() != 0 {
						return false
					}
					if _, ok := numberValue(point); !ok {
						dropped.Points++
					}
				}
			}
		}
	}
	p.dropped = dropped
	p.input = md
	p.prepared = true
	if p.materialize == nil {
		p.materialize = metric.NewMaterializer(p.identity)
	}
	return true
}

// Dropped reports value-less points in the prepared request; projection rejects no additional points.
func (p *MetricsProjector) Dropped() Dropped { return p.dropped }

// Reset releases borrowed input and materializer contexts; oversized scratch is discarded.
// It invalidates every Source previously emitted by Project.
func (p *MetricsProjector) Reset() {
	p.input = pmetric.Metrics{}
	p.prepared = false
	p.dropped = Dropped{}
	clear(p.groups)
	p.groups = p.groups[:0]
	clear(p.metrics)
	p.metrics = p.metrics[:0]
	p.points = p.points[:0]
	p.batch.SetSource(signal.Resource{}, signal.Scope{}, metric.Source{})
	p.batch.IDs = p.batch.IDs[:0]
	p.batch.Ts = p.batch.Ts[:0]
	p.batch.Values = p.batch.Values[:0]
	p.batch.StartTs = p.batch.StartTs[:0]
	clear(p.hash.entries)
	p.hash.entries = p.hash.entries[:0]
	if p.retainedBytes() > maxProjectorRetainedBytes {
		*p = MetricsProjector{}
	}
}

// Project emits each nonempty metric once, borrowing columns until emit returns and Sources until Reset.
// Prepare must succeed first; Project may run only once per Prepare.
func (p *MetricsProjector) Project(emit func(*metric.Batch)) (emitted int) {
	if !p.prepared {
		return 0
	}
	p.prepared = false
	rms := p.input.ResourceMetrics()
	for ri := range rms.Len() {
		rm := rms.At(ri)
		resource := signal.Resource{SchemaURL: []byte(rm.SchemaUrl()), Attributes: convertMap(rm.Resource().Attributes())}
		scopes := rm.ScopeMetrics()
		for si := range scopes.Len() {
			sm := scopes.At(si)
			scope := signal.Scope{
				Name:       []byte(sm.Scope().Name()),
				Version:    []byte(sm.Scope().Version()),
				SchemaURL:  []byte(sm.SchemaUrl()),
				Attributes: convertMap(sm.Scope().Attributes()),
			}
			group := len(p.groups)
			p.groups = append(p.groups, metricContext{resource: resource, scope: scope})
			p.hash.setGroup(resource, scope)
			metrics := sm.Metrics()
			for mi := range metrics.Len() {
				m := metrics.At(mi)
				points := numberPoints(m)
				p.batch.IDs = p.batch.IDs[:0]
				p.batch.Ts = p.batch.Ts[:0]
				p.batch.Values = p.batch.Values[:0]
				p.batch.StartTs = p.batch.StartTs[:0]
				index := len(p.metrics)
				p.metrics = append(p.metrics, metricInput{metric: m, group: group, pointOffset: len(p.points)})
				p.batch.SetSource(resource, scope, metric.NewSource(p.materialize, index))
				p.hash.setMetric(m)
				for pi := range points.Len() {
					point := points.At(pi)
					value, ok := numberValue(point)
					if !ok {
						continue
					}
					p.points = append(p.points, pi)
					p.batch.IDs = append(p.batch.IDs, p.hash.id(point.Attributes()))
					p.batch.Ts = append(p.batch.Ts, int64(point.Timestamp()))
					p.batch.Values = append(p.batch.Values, value)
					p.batch.StartTs = append(p.batch.StartTs, int64(point.StartTimestamp()))
				}
				if p.batch.Len() == 0 {
					continue
				}
				emitted += p.batch.Len()
				emit(&p.batch)
			}
		}
	}
	return emitted
}

func (p *MetricsProjector) identity(index, point int) metric.Identity {
	input := &p.metrics[index]
	if !input.metadata {
		input.name = []byte(input.metric.Name())
		input.unit = []byte(input.metric.Unit())
		input.metadata = true
	}
	context := p.groups[input.group]
	m := input.metric
	kind, temporality, monotonic := metricMetadata(m)
	attrs := numberPoints(m).At(p.points[input.pointOffset+point]).Attributes()
	return metric.Identity{
		Series: signal.Series{Resource: context.resource, Scope: context.scope, Attributes: convertMap(attrs)},
		Name:   input.name, Unit: input.unit, Kind: kind, Temporality: temporality, Monotonic: monotonic,
	}
}

func numberPoints(m pmetric.Metric) pmetric.NumberDataPointSlice {
	if m.Type() == pmetric.MetricTypeSum {
		return m.Sum().DataPoints()
	}
	return m.Gauge().DataPoints()
}

func (p *MetricsProjector) retainedBytes() int64 {
	return memsize.Slice(p.groups) + memsize.Slice(p.metrics) + memsize.Slice(p.points) +
		memsize.Slice(p.batch.IDs) + memsize.Slice(p.batch.Ts) + memsize.Slice(p.batch.Values) + memsize.Slice(p.batch.StartTs) +
		memsize.Slice(p.hash.buf) + memsize.Slice(p.hash.reserved) + memsize.Slice(p.hash.entries)
}

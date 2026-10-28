package metric

import "github.com/oteldb/storage/signal"

// Source borrows a metric's identity context until its input [Metrics] is released or changed.
type Source struct {
	resource *signal.Resource
	scope    *signal.Scope
	metric   *Metric
}

// Source captures the materializer independently of the reused projection buffers.
func (b *Batch) Source() Source { return b.source }

// Series materializes point i's canonical identity from the borrowed input.
func (s Source) Series(i int) signal.Series {
	return (Identity{
		Series: signal.Series{
			Resource:   *s.resource,
			Scope:      *s.scope,
			Attributes: s.metric.Points[i].Attributes,
		},
		Name:        s.metric.Name,
		Unit:        s.metric.Unit,
		Kind:        s.metric.Kind,
		Temporality: s.metric.Temporality,
		Monotonic:   s.metric.Monotonic,
	}).ToSeries()
}

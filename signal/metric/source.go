package metric

import "github.com/oteldb/storage/signal"

// Materializer shares an identity resolver across projected metric groups.
// The callback must return immutable identities valid until the enclosing write returns.
type Materializer struct {
	identity func(group, point int) Identity
}

// NewMaterializer creates a reusable resolver; allocate one per producer, not per metric.
func NewMaterializer(identity func(group, point int) Identity) *Materializer {
	return &Materializer{identity: identity}
}

// Source borrows a metric's identity context until its input [Metrics] is released or changed.
type Source struct {
	materializer *Materializer
	group        int
	resource     *signal.Resource
	scope        *signal.Scope
	metric       *Metric
}

// NewSource borrows a stable materializer and group index until the enclosing write returns.
// Each returned identity must remain immutable until that return, including across later calls.
func NewSource(materializer *Materializer, group int) Source {
	return Source{materializer: materializer, group: group}
}

// Source captures the materializer independently of the reused projection buffers.
func (b *Batch) Source() Source { return b.source }

// Identity returns point i's identity from the borrowed input.
func (s Source) Identity(i int) Identity {
	if s.materializer != nil {
		return s.materializer.identity(s.group, i)
	}
	return Identity{
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
	}
}

// Series materializes point i's canonical identity from the borrowed input.
func (s Source) Series(i int) signal.Series { return s.Identity(i).ToSeries() }

// SetSource sets the context and lazy source of a projected batch; it does not change its columns.
// The caller supplies IDs, Ts, Values and optional StartTs; projected batches have no exemplars.
func (b *Batch) SetSource(resource signal.Resource, scope signal.Scope, source Source) {
	b.base = Identity{Series: signal.Series{Resource: resource, Scope: scope}}
	b.points = nil
	b.source = source
}

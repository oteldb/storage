package metric

import (
	"bytes"
	"encoding/binary"
	"sync"

	"github.com/oteldb/storage/signal"
)

// PointKind is the metric point kind that contributes to identity.
type PointKind uint8

const (
	// KindGauge is a gauge (instantaneous value).
	KindGauge PointKind = iota
	// KindSum is a sum/counter (with temporality + monotonicity).
	KindSum
)

// Temporality is the aggregation temporality (part of a sum's identity).
type Temporality uint8

const (
	// TemporalityUnspecified is an unset temporality.
	TemporalityUnspecified Temporality = iota
	// TemporalityDelta aggregates over the (start, time] window.
	TemporalityDelta
	// TemporalityCumulative aggregates since start.
	TemporalityCumulative
)

// Reserved attribute keys: the metric-specific identity fields are folded into the
// series' point attributes as reserved labels, so the unified [signal.Series] identity +
// the index machinery handle metrics with no metric-specific code, and queries can match
// `__name__` etc. (Prometheus convention). Resource and Scope stay structured.
var (
	LabelName        = []byte("__name__")
	LabelUnit        = []byte("__unit__")
	LabelKind        = []byte("__kind__")
	LabelTemporality = []byte("__temporality__")
	LabelMonotonic   = []byte("__monotonic__")
)

// Identity is a metric series' full identity: the [signal.Series] backbone (Resource +
// Scope + data-point attributes) plus the metric-specific fields name, unit, kind,
// temporality and monotonicity.
type Identity struct {
	Series      signal.Series
	Name        []byte
	Unit        []byte
	Kind        PointKind
	Temporality Temporality
	Monotonic   bool
}

// ToSeries folds the metric-specific fields into the point attributes as reserved labels
// and returns the full [signal.Series] — the value stored, indexed, and returned in
// fetch batches. Two metrics differing only in name/unit/kind/temporality/monotonicity
// produce distinct series (and thus distinct [signal.SeriesID]).
func (id Identity) ToSeries() signal.Series {
	pts := id.Series.Attributes
	merged := make([]signal.KeyValue, 0, len(pts)+5)
	merged = append(merged, pts...)
	merged = append(merged,
		signal.KeyValue{Key: LabelName, Value: signal.StringValue(id.Name)},
		signal.KeyValue{Key: LabelUnit, Value: signal.StringValue(id.Unit)},
		signal.KeyValue{Key: LabelKind, Value: signal.IntValue(int64(id.Kind))},
		signal.KeyValue{Key: LabelTemporality, Value: signal.IntValue(int64(id.Temporality))},
		signal.KeyValue{Key: LabelMonotonic, Value: signal.BoolValue(id.Monotonic)},
	)

	return signal.Series{
		Resource:   id.Series.Resource,
		Scope:      id.Series.Scope,
		Attributes: signal.NewAttributes(merged...),
	}
}

// SeriesID is the content-addressed id of the full identity ([ToSeries] hashed).
func (id Identity) SeriesID() signal.SeriesID { return id.ToSeries().Hash() }

// Sample is a projected number data point: its (start, time] timestamps and value.
type Sample struct {
	StartTs int64
	Ts      int64
	Value   float64
}

// Batch is the projection of a single metric's points: the per-point series ids and the
// timestamp/value columns the engine ingests, plus the metric context needed to materialize
// a full [signal.Series] lazily (only for a series the engine has not seen). Emitting a
// whole metric at once lets the engine take its lock and resolve the tenant once per metric
// rather than once per point.
//
// A Batch is reused across metrics within one [Project] pass: its slices and the data they
// alias (point attributes live in the source [Metrics]) are valid only for the duration of
// the emit call. Do not retain it.
type Batch struct {
	// IDs[i] is the content-addressed id of point i; Ts[i]/Values[i] are its timestamp and
	// value. The three slices share one length, [Batch.Len].
	IDs    []signal.SeriesID
	Ts     []int64
	Values []float64

	base   Identity      // resource/scope/metric fields shared by every point
	points []NumberPoint // the metric's points (aliases the source Metrics)
}

var batchPool = sync.Pool{New: func() any { return &Batch{} }}

// Len returns the number of points in the batch.
func (b *Batch) Len() int { return len(b.IDs) }

// Resource is the batch's source resource (for tenant routing).
func (b *Batch) Resource() signal.Resource { return b.base.Series.Resource }

// Scope is the batch's source scope (for tenant routing).
func (b *Batch) Scope() signal.Scope { return b.base.Series.Scope }

// Identity returns the i-th point's full [Identity]. The returned value aliases the source
// batch (zero-copy); clone it if a durable copy is needed.
func (b *Batch) Identity(i int) Identity {
	id := b.base
	id.Series.Attributes = b.points[i].Attributes

	return id
}

// Series materializes the i-th point's full [signal.Series] (the folded identity). It is the
// lazy materializer the engine calls only when registering a newly-seen series.
func (b *Batch) Series(i int) signal.Series { return b.Identity(i).ToSeries() }

// Sample returns the i-th point's [Sample].
func (b *Batch) Sample(i int) Sample {
	pt := &b.points[i]

	return Sample{StartTs: pt.StartTs, Ts: pt.Ts, Value: pt.Value}
}

// Project iterates an internal [Metrics] batch and calls emit once per metric with a [Batch]
// of that metric's projected points (Gauge and Sum). It returns how many points were
// emitted. Every point in a [Metrics] batch is well-formed by construction (value-less and
// unsupported OTLP points are filtered by the producer — e.g. the otlp/pdataconv bridge), so
// projection rejects nothing; out-of-order rejection is the engine's concern downstream.
//
// Each point's SeriesID is computed without allocating: the resource‖scope hash pre-image is
// hoisted once per scope group, the five folded reserved labels once per metric, and only
// the point attributes are merged in per point, into a reused buffer. The id equals
// [Identity.SeriesID] (id == [Batch.Series](i).Hash()).
func Project(md Metrics, emit func(*Batch)) (accepted int) {
	var p projector

	// Pool the Batch so its id/ts/value column buffers persist across Project calls instead
	// of being reallocated each ingest.
	b, _ := batchPool.Get().(*Batch)
	defer func() {
		b.base = Identity{}
		b.points = nil
		batchPool.Put(b)
	}()

	for ri := range md.Resources {
		rm := &md.Resources[ri]
		b.base.Series.Resource = rm.Resource

		for si := range rm.Scopes {
			sm := &rm.Scopes[si]
			b.base.Series.Scope = sm.Scope
			p.setGroup(rm.Resource, sm.Scope)

			for mi := range sm.Metrics {
				m := &sm.Metrics[mi]
				if len(m.Points) == 0 {
					continue
				}

				b.base.Name, b.base.Unit, b.base.Kind = m.Name, m.Unit, m.Kind
				b.base.Temporality, b.base.Monotonic = m.Temporality, m.Monotonic
				p.setMetric(m)

				b.IDs, b.Ts, b.Values = b.IDs[:0], b.Ts[:0], b.Values[:0]
				for pi := range m.Points {
					pt := &m.Points[pi]
					b.IDs = append(b.IDs, p.id(pt.Attributes))
					b.Ts = append(b.Ts, pt.Ts)
					b.Values = append(b.Values, pt.Value)
				}

				b.points = m.Points
				emit(b)
				accepted += len(m.Points)
			}
		}
	}

	return accepted
}

// projector holds the reusable scratch buffers for one [Project] pass so per-point identity
// hashing allocates nothing. buf[:prefixLen] holds the resource‖scope hash pre-image
// (hoisted per scope group and kept resident, so it is never re-copied per point); each
// point's attributes are appended after it in place. reserved is the metric's five folded
// labels in sorted-by-key order (hoisted per metric).
type projector struct {
	buf       []byte
	prefixLen int
	reserved  [5]signal.KeyValue
}

// setGroup rebuilds the hoisted resource‖scope prefix at the front of buf for a new scope
// group; subsequent per-point ids reuse it without copying.
func (p *projector) setGroup(r signal.Resource, sc signal.Scope) {
	p.buf = sc.AppendHashInput(r.AppendHashInput(p.buf[:0]))
	p.prefixLen = len(p.buf)
}

// setMetric fills the five reserved labels for a metric, in sorted-by-key order:
// __kind__ < __monotonic__ < __name__ < __temporality__ < __unit__.
func (p *projector) setMetric(m *Metric) {
	p.reserved[0] = signal.KeyValue{Key: LabelKind, Value: signal.IntValue(int64(m.Kind))}
	p.reserved[1] = signal.KeyValue{Key: LabelMonotonic, Value: signal.BoolValue(m.Monotonic)}
	p.reserved[2] = signal.KeyValue{Key: LabelName, Value: signal.StringValue(m.Name)}
	p.reserved[3] = signal.KeyValue{Key: LabelTemporality, Value: signal.IntValue(int64(m.Temporality))}
	p.reserved[4] = signal.KeyValue{Key: LabelUnit, Value: signal.StringValue(m.Unit)}
}

// id computes the series id for a point with the given already-sorted attributes, reusing
// buf. It builds the same hash pre-image as [Identity.ToSeries] hashed — prefix, then the
// attribute set (point attributes ∪ the five reserved labels) in sorted order — but merges
// the two sorted sources in one pass instead of allocating and sorting a combined slice.
func (p *projector) id(attrs signal.Attributes) signal.SeriesID {
	// Reuse buf, truncated to the resident prefix; the attribute bytes are appended after it
	// in place (keeping the grown capacity), so the prefix is never re-copied.
	buf := p.buf[:p.prefixLen]
	buf = binary.AppendUvarint(buf, uint64(len(attrs)+len(p.reserved)))
	buf = appendMergedHashInput(buf, attrs, p.reserved[:])
	p.buf = buf

	return signal.HashBytes(buf)
}

// appendMergedHashInput appends the attribute hash pre-image for the merge of two
// already-sorted, individually-unique key/value sequences. Ties (equal keys) resolve to a
// first, which matches the stable sort of (a ‖ b) that [Identity.ToSeries] performs, so the
// resulting id is byte-identical.
func appendMergedHashInput(dst []byte, a signal.Attributes, b []signal.KeyValue) []byte {
	i, j := 0, 0
	for i < len(a) && j < len(b) {
		if bytes.Compare(a[i].Key, b[j].Key) <= 0 {
			dst = signal.AppendKeyValueHashInput(dst, a[i].Key, a[i].Value)
			i++
		} else {
			dst = signal.AppendKeyValueHashInput(dst, b[j].Key, b[j].Value)
			j++
		}
	}

	for ; i < len(a); i++ {
		dst = signal.AppendKeyValueHashInput(dst, a[i].Key, a[i].Value)
	}

	for ; j < len(b); j++ {
		dst = signal.AppendKeyValueHashInput(dst, b[j].Key, b[j].Value)
	}

	return dst
}

package metric

import (
	"bytes"
	"encoding/binary"

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

// Project iterates an internal [Metrics] batch and calls emit for every number data point
// with its content-addressed [signal.SeriesID], its [Identity], and its [Sample]. It
// returns how many points were emitted. Every point in a [Metrics] batch is well-formed by
// construction (value-less and unsupported OTLP points are filtered by the producer — e.g.
// the otlp/pdataconv bridge), so projection rejects nothing; out-of-order rejection is the
// engine's concern downstream.
//
// The SeriesID is computed on the hot path without allocating: the resource‖scope hash
// pre-image is hoisted once per scope group, the five folded reserved labels once per
// metric, and only the point attributes are merged in per point, into a reused buffer. The
// emitted id equals [Identity.SeriesID] (i.e. emit's id == id.ToSeries().Hash()). The
// *Identity passed to emit is reused across points: emit must not retain it past the call;
// materialize a [signal.Series] via [Identity.ToSeries] if a durable copy is needed.
func Project(md Metrics, emit func(signal.SeriesID, *Identity, Sample)) (accepted int) {
	var (
		p  projector
		id Identity
	)

	for ri := range md.Resources {
		rm := &md.Resources[ri]
		id.Series.Resource = rm.Resource

		for si := range rm.Scopes {
			sm := &rm.Scopes[si]
			id.Series.Scope = sm.Scope
			p.setGroup(rm.Resource, sm.Scope)

			for mi := range sm.Metrics {
				m := &sm.Metrics[mi]
				id.Name, id.Unit, id.Kind = m.Name, m.Unit, m.Kind
				id.Temporality, id.Monotonic = m.Temporality, m.Monotonic
				p.setMetric(m)

				for pi := range m.Points {
					pt := &m.Points[pi]
					id.Series.Attributes = pt.Attributes
					sid := p.id(pt.Attributes)
					emit(sid, &id, Sample{StartTs: pt.StartTs, Ts: pt.Ts, Value: pt.Value})
					accepted++
				}
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

package metric

import (
	"sync"

	"github.com/oteldb/storage/signal"
)

// Metrics is the []byte-based, OTLP-shaped metrics ingest batch — the zero-alloc
// representation accepted at the storage boundary in place of the OTel-Go
// pmetric.Metrics. It mirrors the OTel Resource→Scope→Metric→point hierarchy but holds
// all identity as []byte (never string), so an embedder that decodes OTLP protobuf can
// build it by aliasing the decode buffer, and projecting it into the internal columnar
// model copies nothing.
//
// Metrics is resettable and pool-friendly: [Metrics.Reset] keeps every backing array, so
// a batch fetched from [GetMetrics] and returned with [PutMetrics] recycles across ingest
// calls with no allocation. Build it with the Add* helpers, which reuse the retained
// capacity of the nested slices.
//
// A pdata bridge for OTel-Go users lives in the optional otlp/pdataconv package, off this
// hot path.
type Metrics struct {
	Resources []ResourceMetrics
}

// ResourceMetrics groups the metrics emitted under one [signal.Resource].
type ResourceMetrics struct {
	Resource signal.Resource
	Scopes   []ScopeMetrics
}

// ScopeMetrics groups the metrics emitted under one [signal.Scope]
// (InstrumentationScope).
type ScopeMetrics struct {
	Scope   signal.Scope
	Metrics []Metric
}

// Metric is a single metric stream: its name, unit, kind, the sum-only temporality and
// monotonicity, and its number data points. Only gauge and sum number points are modeled
// (the metrics-first vertical); histogram/exp-histogram/summary are added with their
// support.
type Metric struct {
	Name        []byte
	Unit        []byte
	Kind        PointKind
	Temporality Temporality // KindSum only; TemporalityUnspecified otherwise
	Monotonic   bool        // KindSum only
	Points      []NumberPoint
}

// NumberPoint is a gauge/sum number data point: its data-point attributes, the
// (start, time] timestamps in unix nanoseconds, and its value (int values are widened to
// float64 at ingest). A point present in a [Metrics] batch is well-formed by construction;
// value-less or unsupported OTLP points are filtered (and counted) by the producer, never
// represented here.
type NumberPoint struct {
	Attributes signal.Attributes
	StartTs    int64
	Ts         int64
	Value      float64
}

// Reset clears the batch for reuse while retaining the capacity of all backing arrays
// (the resource/scope/metric/point slices). The Add* helpers reuse that retained capacity,
// so a Reset-then-rebuild cycle allocates nothing.
func (m *Metrics) Reset() { m.Resources = m.Resources[:0] }

// AddResource appends a fresh [ResourceMetrics] and returns a pointer to it for the caller
// to populate. The returned slot's Resource is zeroed and its Scopes are emptied (capacity
// retained); set the Resource field and call [ResourceMetrics.AddScope].
func (m *Metrics) AddResource() *ResourceMetrics {
	m.Resources = grow(m.Resources)
	rm := &m.Resources[len(m.Resources)-1]
	rm.Resource = signal.Resource{}
	rm.Scopes = rm.Scopes[:0]

	return rm
}

// AddScope appends a fresh [ScopeMetrics] under the resource and returns a pointer to it.
// The slot's Scope is zeroed and its Metrics emptied (capacity retained).
func (rm *ResourceMetrics) AddScope() *ScopeMetrics {
	rm.Scopes = grow(rm.Scopes)
	sm := &rm.Scopes[len(rm.Scopes)-1]
	sm.Scope = signal.Scope{}
	sm.Metrics = sm.Metrics[:0]

	return sm
}

// AddMetric appends a fresh [Metric] under the scope and returns a pointer to it. The
// slot's identity fields are zeroed and its Points emptied (capacity retained); set Name,
// Unit, Kind (and for sums Temporality/Monotonic) and call [Metric.AddPoint].
func (sm *ScopeMetrics) AddMetric() *Metric {
	sm.Metrics = grow(sm.Metrics)
	mt := &sm.Metrics[len(sm.Metrics)-1]
	mt.Name = nil
	mt.Unit = nil
	mt.Kind = KindGauge
	mt.Temporality = TemporalityUnspecified
	mt.Monotonic = false
	mt.Points = mt.Points[:0]

	return mt
}

// AddPoint appends a fresh [NumberPoint] to the metric and returns a pointer to it for the
// caller to populate (Attributes, StartTs, Ts, Value).
func (mt *Metric) AddPoint() *NumberPoint {
	mt.Points = grow(mt.Points)
	p := &mt.Points[len(mt.Points)-1]
	*p = NumberPoint{}

	return p
}

// grow extends s by one element, reusing the retained backing array when len < cap (the
// resettable-arena trick) and only allocating when the slice is at capacity. The reused
// slot keeps its previous value so callers can recycle its nested slices before clearing.
func grow[T any](s []T) []T {
	if len(s) < cap(s) {
		return s[:len(s)+1]
	}

	var zero T

	return append(s, zero)
}

var metricsPool = sync.Pool{New: func() any { return &Metrics{} }}

// GetMetrics returns a reset [Metrics] from a shared pool. Pair it with [PutMetrics] to
// recycle the batch (and its backing arrays) across ingest calls.
func GetMetrics() *Metrics {
	m, _ := metricsPool.Get().(*Metrics)

	return m
}

// PutMetrics resets m and returns it to the pool. Do not use m after this call. The byte
// payloads referenced by m's attributes are not owned by the pool; the caller must ensure
// they are no longer aliased.
func PutMetrics(m *Metrics) {
	m.Reset()
	metricsPool.Put(m)
}

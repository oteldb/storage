// Package exemplar is the exemplars vertical: the record-engine schema and projection for metric
// exemplars.
//
// An exemplar is record-shaped, not series-shaped — zero or more per data point, each with a
// variable-width payload — so exemplars are stored by the shared record engine rather than the
// dense metrics engine, whose (series, ts, value) layout and downsampling merge assume exactly one
// row per point.
//
// The load-bearing property is that an exemplar stream *is* its metric series: the stream id is the
// [metric.Identity.SeriesID] of the point the exemplar hangs off, byte for byte. It is not
// recomputed here — [Project] reads the id the metrics path already resolved — so the two cannot
// drift. That identity is what makes the series index, tenant routing, and cluster shard placement
// fall out with no exemplar-specific machinery: a matcher set that selects metric series selects
// exactly the matching exemplar streams.
package exemplar

import (
	"math"
	"sync"

	"github.com/oteldb/storage/encoding/chunk"
	"github.com/oteldb/storage/recordengine"
	"github.com/oteldb/storage/signal"
	"github.com/oteldb/storage/signal/metric"
)

// Column names of the exemplars schema. The exemplar's timestamp is the implicit primary
// timestamp / sort key, and the stream id is the metric series id — neither is a column.
const (
	ColValue   = "value"    // float64 value, reinterpreted (see [EncodeValue])
	ColTraceID = "trace_id" // sampled span's trace id (16 bytes) or empty
	ColSpanID  = "span_id"  // sampled span's span id (8 bytes) or empty
	ColAttrs   = "attrs"    // serialized filtered (aggregation-dropped) attributes
)

// Schema is the exemplars vertical's record-engine column schema.
//
// trace_id is stored raw rather than dictionary-coded: exemplar trace ids are near-unique by
// construction (one exemplar is one sampled request), so a dictionary is pure overhead. Its
// equality bloom is what prunes parts for a trace-to-metrics lookup.
var Schema = recordengine.NewSchema(
	recordengine.Column{Name: ColValue, Kind: recordengine.KindInt64, Codec: chunk.CodecT64},
	recordengine.Column{Name: ColTraceID, Kind: recordengine.KindBytes, Codec: chunk.CodecBytesRaw, Bloom: recordengine.BloomEquality},
	recordengine.Column{Name: ColSpanID, Kind: recordengine.KindBytes, Codec: chunk.CodecBytesRaw},
	recordengine.Column{Name: ColAttrs, Kind: recordengine.KindBytes, Codec: chunk.CodecDict, Bloom: recordengine.BloomAttrs},
)

// Schema int/byte column indices, in the declaration order above.
const iValue = 0

const (
	bTraceID = iota
	bSpanID
	bAttrs
)

// EncodeValue reinterprets an exemplar value as the int64 the [ColValue] column stores;
// [DecodeValue] inverts it. The record engine has no float column kind, so the value's bit pattern
// rides in an int64 one. This is lossless but costs compression (a float64 bit pattern has its
// exponent bits set across the whole range, leaving CodecT64 nothing to crop) and keeps exemplars
// out of the float-precision recompression policy — see issue #251, which adds a float kind and
// removes this.
func EncodeValue(v float64) int64 { return int64(math.Float64bits(v)) }

// DecodeValue inverts [EncodeValue], recovering the exemplar value from a [ColValue] cell.
func DecodeValue(v int64) float64 { return math.Float64frombits(uint64(v)) }

// projector holds the reusable per-stream column buffers so a steady-state [Project] allocates only
// the per-record attribute blobs (which the head must own).
type projector struct {
	b recordengine.Batch
}

var projectorPool = sync.Pool{New: func() any {
	p := &projector{}
	p.b.Ints = make([][]int64, 1)
	p.b.Bytes = make([][][]byte, 3)

	return p
}}

// Project projects the exemplars of one already-projected metric batch, calling emit once per point
// that carries any, with a [recordengine.Batch] of that point's exemplar rows in the exemplars
// [Schema]'s column order. It returns how many rows were emitted.
//
// One emit is one stream: within a metric, OTLP data points have distinct attribute sets, so each
// point is its own series. Points with no exemplars — the overwhelming majority — are skipped
// without touching the engine.
//
// The batch and its column buffers are reused across emit calls, and the rows alias the source
// [metric.Metrics]; do not retain either past the call.
func Project(mb *metric.Batch, emit func(*recordengine.Batch)) (accepted int) {
	p, _ := projectorPool.Get().(*projector)
	defer projectorPool.Put(p)

	for i := range mb.Len() {
		exs := mb.Exemplars(i)
		if len(exs) == 0 {
			continue
		}

		p.fill(mb, i, exs)
		emit(&p.b)

		accepted += len(exs)
	}

	return accepted
}

// fill resets the reusable batch and populates it from one point's exemplars.
func (p *projector) fill(mb *metric.Batch, i int, exs []metric.Exemplar) {
	p.b.Stream = mb.IDs[i]
	p.b.Identity = func() signal.Series { return mb.Series(i) }

	p.b.Ts = p.b.Ts[:0]
	for k := range p.b.Ints {
		p.b.Ints[k] = p.b.Ints[k][:0]
	}

	for k := range p.b.Bytes {
		p.b.Bytes[k] = p.b.Bytes[k][:0]
	}

	for j := range exs {
		e := &exs[j]
		p.b.Ts = append(p.b.Ts, e.Ts)
		p.b.Ints[iValue] = append(p.b.Ints[iValue], EncodeValue(e.Value))
		p.b.Bytes[bTraceID] = append(p.b.Bytes[bTraceID], e.TraceID)
		p.b.Bytes[bSpanID] = append(p.b.Bytes[bSpanID], e.SpanID)
		p.b.Bytes[bAttrs] = append(p.b.Bytes[bAttrs], e.FilteredAttributes.AppendHashInput(nil))
	}
}

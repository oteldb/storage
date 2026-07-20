package log

import (
	"sync"

	"github.com/oteldb/storage/encoding/chunk"
	"github.com/oteldb/storage/recordengine"
	"github.com/oteldb/storage/signal"
)

// Column names of the logs schema (the per-record columns; the primary timestamp and the stream id
// are implicit in the record engine).
const (
	ColObserved     = "observed"
	ColSeverity     = "severity"
	ColFlags        = "flags"
	ColDropped      = "dropped"
	ColSeverityText = "severity_text"
	ColBody         = "body"
	ColTraceID      = "trace_id"
	ColSpanID       = "span_id"
	ColAttrs        = "attrs"
)

// Schema is the logs vertical's record-engine column schema: four small int columns, the body
// (full-text bloom), the trace/span ids (trace_id carries an equality bloom for logs-by-trace-id
// pruning), severity text, and the serialized per-record attributes (attribute bloom). The primary
// timestamp (the record's event time) is the implicit sort key.
var Schema = recordengine.NewSchema(
	recordengine.Column{Name: ColObserved, Kind: recordengine.KindInt64, Codec: chunk.CodecT64},
	recordengine.Column{Name: ColSeverity, Kind: recordengine.KindInt64, Codec: chunk.CodecT64},
	recordengine.Column{Name: ColFlags, Kind: recordengine.KindInt64, Codec: chunk.CodecT64},
	recordengine.Column{Name: ColDropped, Kind: recordengine.KindInt64, Codec: chunk.CodecT64},
	recordengine.Column{Name: ColSeverityText, Kind: recordengine.KindBytes, Codec: chunk.CodecDict},
	recordengine.Column{Name: ColBody, Kind: recordengine.KindBytes, Codec: chunk.CodecDict, Bloom: recordengine.BloomFullText},
	recordengine.Column{Name: ColTraceID, Kind: recordengine.KindBytes, Codec: chunk.CodecDict, Bloom: recordengine.BloomEquality},
	recordengine.Column{Name: ColSpanID, Kind: recordengine.KindBytes, Codec: chunk.CodecDict},
	recordengine.Column{Name: ColAttrs, Kind: recordengine.KindBytes, Codec: chunk.CodecDict, Bloom: recordengine.BloomAttrs},
)

// Schema int/byte column indices, in the declaration order above (used to fill the engine batch).
const (
	iObserved = iota
	iSeverity
	iFlags
	iDropped
)

const (
	bSeverityText = iota
	bBody
	bTraceID
	bSpanID
	bAttrs
)

// projector holds the reusable per-stream column buffers and hash scratch so a steady-state
// [Project] allocates only the per-record attribute blobs (which the head must own).
type projector struct {
	b     recordengine.Batch
	hash  []byte
	res   signal.Resource
	scope signal.Scope
}

var projectorPool = sync.Pool{New: func() any {
	p := &projector{}
	p.b.Ints = make([][]int64, 4)
	p.b.Bytes = make([][][]byte, 5)

	return p
}}

// Project iterates a [Logs] batch and calls emit once per stream (each Resource+Scope group) with a
// [recordengine.Batch] of that stream's records laid out in the logs [Schema]'s column order. It
// returns how many records were emitted. The batch and its column buffers are reused across emit
// calls — do not retain them.
func Project(ld Logs, emit func(*recordengine.Batch)) (accepted int) {
	p, _ := projectorPool.Get().(*projector)
	defer projectorPool.Put(p)

	for ri := range ld.Resources {
		rl := &ld.Resources[ri]
		p.res = rl.Resource

		for si := range rl.Scopes {
			sl := &rl.Scopes[si]
			if len(sl.Records) == 0 {
				continue
			}

			p.scope = sl.Scope
			p.fill(sl.Records)
			emit(&p.b)
			accepted += len(sl.Records)
		}
	}

	return accepted
}

// fill resets the reusable batch and populates it from the stream's records.
func (p *projector) fill(recs []Record) {
	p.hash = (signal.Series{Resource: p.res, Scope: p.scope}).AppendHashInput(p.hash[:0])
	p.b.Stream = signal.HashBytes(p.hash)

	res, scope := p.res, p.scope
	p.b.Identity = func() signal.Series { return signal.Series{Resource: res, Scope: scope} }

	p.b.Ts = p.b.Ts[:0]
	for k := range p.b.Ints {
		p.b.Ints[k] = p.b.Ints[k][:0]
	}

	for k := range p.b.Bytes {
		p.b.Bytes[k] = p.b.Bytes[k][:0]
	}

	for i := range recs {
		r := &recs[i]
		p.b.Ts = append(p.b.Ts, r.Timestamp)
		p.b.Ints[iObserved] = append(p.b.Ints[iObserved], r.ObservedTimestamp)
		p.b.Ints[iSeverity] = append(p.b.Ints[iSeverity], int64(r.SeverityNumber))
		p.b.Ints[iFlags] = append(p.b.Ints[iFlags], int64(r.Flags))
		p.b.Ints[iDropped] = append(p.b.Ints[iDropped], int64(r.Dropped))
		p.b.Bytes[bSeverityText] = append(p.b.Bytes[bSeverityText], r.SeverityText)
		p.b.Bytes[bBody] = append(p.b.Bytes[bBody], r.Body)
		p.b.Bytes[bTraceID] = append(p.b.Bytes[bTraceID], r.TraceID)
		p.b.Bytes[bSpanID] = append(p.b.Bytes[bSpanID], r.SpanID)
		p.b.Bytes[bAttrs] = append(p.b.Bytes[bAttrs], r.Attributes.AppendHashInput(nil))
	}
}

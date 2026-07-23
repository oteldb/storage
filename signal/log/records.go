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
	ColResource     = "resource"
)

// Schema is the logs vertical's record-engine column schema: four small int columns, the body
// (full-text bloom), the trace/span ids (trace_id carries an equality bloom for logs-by-trace-id
// pruning), severity text, the serialized per-record attributes, and the stream's serialized
// resource attributes (both attribute blooms). The primary timestamp (the record's event time) is
// the implicit sort key.
//
// The resource column repeats the stream's complete resource attribute set on every record. It is
// what lets a tenant keep a high-churn attribute out of the stream key without losing it: the
// attribute is still stored, still bloom-pruned, still matched by a column condition, and still
// reassembled into the OTLP resource on read. The repetition is near-free — record parts are sorted
// by (stream, ts) and the column is dictionary-coded, so a part holds roughly one entry per stream.
// It is declared after attrs so a record attribute shadows a resource attribute of the same name.
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
	recordengine.Column{
		Name: ColResource, Kind: recordengine.KindBytes, Codec: chunk.CodecDict,
		Bloom: recordengine.BloomAttrs, KeyScope: recordengine.KeyScopeResource,
	},
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
	bResource
)

// StreamFields reports which of a resource's attributes identify its stream — the sorted keys that
// are hashed into the stream id and resolved by a label matcher — for the tenant owning the given
// Resource and Scope. all ⇒ every resource attribute identifies the stream (fields is ignored).
// Attributes outside the set are still stored on every record and answered by a column condition.
//
// It is called once per stream, immediately before that stream's emit, so an implementation may
// derive the tenant from the same Resource and Scope the batch is about to be routed by.
type StreamFields func(res signal.Resource, scope signal.Scope) (fields []string, all bool)

// projector holds the reusable per-stream column buffers and hash scratch so a steady-state
// [Project] allocates only the per-record attribute blobs (which the head must own).
type projector struct {
	b       recordengine.Batch
	hash    []byte
	resBlob []byte
	idAttrs signal.Attributes
	res     signal.Resource
	scope   signal.Scope
}

var projectorPool = sync.Pool{New: func() any {
	p := &projector{}
	p.b.Ints = make([][]int64, 4)
	p.b.Bytes = make([][][]byte, 6)

	return p
}}

// Project iterates a [Logs] batch and calls emit once per stream (each Resource+Scope group) with a
// [recordengine.Batch] of that stream's records laid out in the logs [Schema]'s column order. It
// returns how many records were emitted. The batch and its column buffers are reused across emit
// calls — do not retain them.
//
// fields classifies each stream's resource attributes into identity and stored-only (see
// [StreamFields]); a nil fields keeps every resource attribute in the stream key. The complete
// resource is written to the [ColResource] column either way, and reported by
// [recordengine.Batch.RoutingIdentity] for tenant derivation.
func Project(ld Logs, fields StreamFields, emit func(*recordengine.Batch)) (accepted int) {
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

			keys, all := []string(nil), true
			if fields != nil {
				keys, all = fields(p.res, p.scope)
			}

			p.fill(sl.Records, keys, all)
			emit(&p.b)
			accepted += len(sl.Records)
		}
	}

	return accepted
}

// fill resets the reusable batch and populates it from the stream's records, narrowing the stream
// identity to the resource attributes named by fields (unless all).
func (p *projector) fill(recs []Record, fields []string, all bool) {
	res, scope := p.res, p.scope

	id := res
	if !all {
		p.idAttrs = filterAttrs(p.idAttrs[:0], res.Attributes, fields)
		id.Attributes = p.idAttrs
	}

	p.hash = (signal.Series{Resource: id, Scope: scope}).AppendHashInput(p.hash[:0])
	p.b.Stream = signal.HashBytes(p.hash)
	p.b.Identity = func() signal.Series { return signal.Series{Resource: id, Scope: scope} }

	p.b.Route = nil
	if !all {
		p.b.Route = func() signal.Series { return signal.Series{Resource: res, Scope: scope} }
	}

	// One blob per stream, repeated per record: the dictionary codec collapses it back to a single
	// entry at flush, and the head's per-row clone is what makes it independently addressable.
	p.resBlob = res.Attributes.AppendHashInput(p.resBlob[:0])

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
		p.b.Bytes[bResource] = append(p.b.Bytes[bResource], p.resBlob)
	}
}

// filterAttrs appends to dst the attributes of a whose key is in fields. Both inputs are sorted by
// key ([signal.NewAttributes] sorts, [tenant.Streams.Fields] is documented sorted), so it is one
// linear merge with no allocation beyond dst's growth.
func filterAttrs(dst, a signal.Attributes, fields []string) signal.Attributes {
	for i, j := 0, 0; i < len(a) && j < len(fields); {
		switch c := compareKey(a[i].Key, fields[j]); {
		case c == 0:
			dst = append(dst, a[i])
			i++
			j++
		case c < 0:
			i++
		default:
			j++
		}
	}

	return dst
}

// compareKey compares an attribute key against a field name without converting either — a
// []byte→string conversion would allocate on this per-stream path.
func compareKey(key []byte, field string) int {
	for i := range min(len(key), len(field)) {
		if key[i] != field[i] {
			return int(key[i]) - int(field[i])
		}
	}

	return len(key) - len(field)
}

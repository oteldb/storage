package trace

import (
	"sort"

	"github.com/oteldb/storage/encoding/chunk"
	"github.com/oteldb/storage/recordengine"
	"github.com/oteldb/storage/signal"
)

// Column names of the traces schema (per-record span columns; the span start time is the implicit
// primary timestamp / sort key, and the stream id is the Resource+Scope hash).
const (
	ColDuration     = "duration"
	ColKind         = "kind"
	ColStatusCode   = "status_code"
	ColParentID     = "parent_id"        // nested-set parent id (the parent's left), 0 for a root
	ColNestedLeft   = "nested_set_left"  // preorder enter index
	ColNestedRight  = "nested_set_right" // preorder exit index (descendant iff a.left<b.left && b.right<a.right)
	ColTraceID      = "trace_id"
	ColSpanID       = "span_id"
	ColParentSpanID = "parent_span_id"
	ColName         = "name"
	ColStatusMsg    = "status_message"
	ColAttrs        = "attrs"
	ColEvents       = "events"
	ColLinks        = "links"
)

// Schema is the traces vertical's record-engine column schema. trace_id carries an equality bloom
// (trace-by-id pruning); name carries a full-text bloom; attrs the attribute bloom.
var Schema = recordengine.NewSchema(
	recordengine.Column{Name: ColDuration, Kind: recordengine.KindInt64, Codec: chunk.CodecT64},
	recordengine.Column{Name: ColKind, Kind: recordengine.KindInt64, Codec: chunk.CodecT64},
	recordengine.Column{Name: ColStatusCode, Kind: recordengine.KindInt64, Codec: chunk.CodecT64},
	recordengine.Column{Name: ColParentID, Kind: recordengine.KindInt64, Codec: chunk.CodecT64},
	recordengine.Column{Name: ColNestedLeft, Kind: recordengine.KindInt64, Codec: chunk.CodecT64},
	recordengine.Column{Name: ColNestedRight, Kind: recordengine.KindInt64, Codec: chunk.CodecT64},
	recordengine.Column{Name: ColTraceID, Kind: recordengine.KindBytes, Codec: chunk.CodecDict, Bloom: recordengine.BloomEquality},
	recordengine.Column{Name: ColSpanID, Kind: recordengine.KindBytes, Codec: chunk.CodecBytesRaw},
	recordengine.Column{Name: ColParentSpanID, Kind: recordengine.KindBytes, Codec: chunk.CodecDict},
	recordengine.Column{Name: ColName, Kind: recordengine.KindBytes, Codec: chunk.CodecDict, Bloom: recordengine.BloomFullText},
	recordengine.Column{Name: ColStatusMsg, Kind: recordengine.KindBytes, Codec: chunk.CodecDict},
	recordengine.Column{Name: ColAttrs, Kind: recordengine.KindBytes, Codec: chunk.CodecDict, Bloom: recordengine.BloomAttrs},
	recordengine.Column{Name: ColEvents, Kind: recordengine.KindBytes, Codec: chunk.CodecDict},
	recordengine.Column{Name: ColLinks, Kind: recordengine.KindBytes, Codec: chunk.CodecDict},
)

// int/byte column indices in the declaration order above.
const (
	iDuration = iota
	iKind
	iStatusCode
	iParentID
	iNestedLeft
	iNestedRight
)

const (
	bTraceID = iota
	bSpanID
	bParentSpanID
	bName
	bStatusMsg
	bAttrs
	bEvents
	bLinks
)

// nset holds a span's computed nested-set ids.
type nset struct {
	left, right, parentID int64
}

// Project iterates a [Traces] batch and calls emit once per stream (each Resource+Scope group) with
// a [recordengine.Batch] of that stream's spans in the traces [Schema]'s column order. It returns
// how many spans were emitted.
//
// Before projecting, it computes nested-set ids per trace **within this batch**: spans are grouped
// by trace_id (across resources/scopes — a trace spans services), each trace's parent→child tree is
// built from parent_span_id, and a DFS assigns left/right/parent ids. A span whose parent is absent
// from the batch is treated as a root; cross-batch parents therefore get independent numbering (the
// raw parent_span_id column is always present for the embedder to reconcile). Events and links are
// serialized into their byte columns.
func Project(td Traces, emit func(*recordengine.Batch)) (accepted int) {
	nsets := computeNestedSets(td)

	var b recordengine.Batch

	b.Ints = make([][]int64, 6)
	b.Bytes = make([][][]byte, 8)

	for ri := range td.Resources {
		rs := &td.Resources[ri]
		for si := range rs.Scopes {
			ss := &rs.Scopes[si]
			if len(ss.Spans) == 0 {
				continue
			}

			fillBatch(&b, rs.Resource, ss.Scope, ss.Spans, nsets)
			emit(&b)
			accepted += len(ss.Spans)
		}
	}

	return accepted
}

// spanKey identifies a span within the batch for the nested-set map.
func spanKey(traceID, spanID []byte) string { return string(traceID) + "\x00" + string(spanID) }

// computeNestedSets groups every span in the batch by trace id, builds each trace's tree, and
// assigns nested-set ids by a deterministic preorder DFS.
func computeNestedSets(td Traces) map[string]nset {
	byTrace := map[string][]*Span{}
	for ri := range td.Resources {
		rs := &td.Resources[ri]
		for si := range rs.Scopes {
			ss := &rs.Scopes[si]
			for k := range ss.Spans {
				sp := &ss.Spans[k]
				byTrace[string(sp.TraceID)] = append(byTrace[string(sp.TraceID)], sp)
			}
		}
	}

	out := make(map[string]nset)
	for _, spans := range byTrace {
		assignNestedSet(spans, out)
	}

	return out
}

// assignNestedSet assigns left/right/parent ids to one trace's spans (preorder DFS from the roots).
func assignNestedSet(spans []*Span, out map[string]nset) {
	bySpanID := make(map[string]*Span, len(spans))
	children := make(map[string][]*Span, len(spans))

	for _, sp := range spans {
		bySpanID[string(sp.SpanID)] = sp
	}

	var roots []*Span
	for _, sp := range spans {
		if parent, ok := bySpanID[string(sp.ParentSpanID)]; ok && parent != sp {
			children[string(sp.ParentSpanID)] = append(children[string(sp.ParentSpanID)], sp)
		} else {
			roots = append(roots, sp) // no parent in this batch ⇒ a root
		}
	}

	byStart := func(s []*Span) {
		sort.SliceStable(s, func(i, j int) bool {
			if s[i].Start != s[j].Start {
				return s[i].Start < s[j].Start
			}

			return string(s[i].SpanID) < string(s[j].SpanID)
		})
	}

	byStart(roots)

	counter := int64(1)
	visited := make(map[string]bool, len(spans))

	var dfs func(sp *Span, parentLeft int64)
	dfs = func(sp *Span, parentLeft int64) {
		k := string(sp.SpanID)
		if visited[k] { // defensive against cycles
			return
		}

		visited[k] = true

		left := counter
		counter++

		kids := children[k]
		byStart(kids)
		for _, c := range kids {
			dfs(c, left)
		}

		right := counter
		counter++
		out[spanKey(sp.TraceID, sp.SpanID)] = nset{left: left, right: right, parentID: parentLeft}
	}

	for _, r := range roots {
		dfs(r, 0)
	}
}

// fillBatch resets b and populates it from one stream's spans, in the schema's column order.
func fillBatch(b *recordengine.Batch, res signal.Resource, scope signal.Scope, spans []Span, nsets map[string]nset) {
	series := signal.Series{Resource: res, Scope: scope}
	b.Stream = series.Hash()
	b.Identity = func() signal.Series { return series }

	b.Ts = b.Ts[:0]
	for k := range b.Ints {
		b.Ints[k] = b.Ints[k][:0]
	}

	for k := range b.Bytes {
		b.Bytes[k] = b.Bytes[k][:0]
	}

	for i := range spans {
		sp := &spans[i]
		ns := nsets[spanKey(sp.TraceID, sp.SpanID)]

		b.Ts = append(b.Ts, sp.Start)
		b.Ints[iDuration] = append(b.Ints[iDuration], sp.End-sp.Start)
		b.Ints[iKind] = append(b.Ints[iKind], int64(sp.Kind))
		b.Ints[iStatusCode] = append(b.Ints[iStatusCode], int64(sp.StatusCode))
		b.Ints[iParentID] = append(b.Ints[iParentID], ns.parentID)
		b.Ints[iNestedLeft] = append(b.Ints[iNestedLeft], ns.left)
		b.Ints[iNestedRight] = append(b.Ints[iNestedRight], ns.right)
		b.Bytes[bTraceID] = append(b.Bytes[bTraceID], sp.TraceID)
		b.Bytes[bSpanID] = append(b.Bytes[bSpanID], sp.SpanID)
		b.Bytes[bParentSpanID] = append(b.Bytes[bParentSpanID], sp.ParentSpanID)
		b.Bytes[bName] = append(b.Bytes[bName], sp.Name)
		b.Bytes[bStatusMsg] = append(b.Bytes[bStatusMsg], sp.StatusMessage)
		b.Bytes[bAttrs] = append(b.Bytes[bAttrs], sp.Attributes.AppendHashInput(nil))
		b.Bytes[bEvents] = append(b.Bytes[bEvents], encodeEvents(sp.Events))
		b.Bytes[bLinks] = append(b.Bytes[bLinks], encodeLinks(sp.Links))
	}
}

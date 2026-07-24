package recordengine

import (
	"sort"

	"github.com/oteldb/storage/signal"
)

// KeyScope is a bitset of the scopes an attribute key was observed in. A key can appear in more than
// one — e.g. as a resource attribute on one stream and a per-record attribute on another — and the
// bitset records every scope, so a caller can tell a stream label from a record attribute (or both).
type KeyScope uint8

const (
	// KeyScopeResource marks a resource attribute.
	KeyScopeResource KeyScope = 1 << iota
	// KeyScopeScope marks an instrumentation-scope attribute.
	KeyScopeScope
	// KeyScopeRecord marks a key that must be queried as a per-record column condition: it is
	// stored in a serialized attributes column and is *not* resolvable through the postings index.
	// A resource attribute the tenant's stream-field policy excludes from identity carries
	// KeyScopeResource|KeyScopeRecord — resource by provenance, per-record by query mechanism.
	KeyScopeRecord
	// KeyScopeIndexed marks a key that is part of the stream identity, so a label matcher on it
	// resolves through the postings index. A key stored per record purely because it duplicates the
	// stream key does not also carry KeyScopeRecord, so a caller routes it to the postings index
	// alone. Both bits together mean the key is genuinely both (a resource attribute on one stream,
	// a record attribute on another) and neither pushdown is sound by itself.
	KeyScopeIndexed
)

// KeyInfo is a distinct attribute key and the union of the scopes it was observed in. Key aliases
// engine-owned bytes (a head identity or a decoded part-footer entry); copy it to retain.
type KeyInfo struct {
	Key   []byte
	Scope KeyScope
}

// Keys enumerates the distinct attribute keys present across the engine's streams (head ∪ flushed
// parts) with at least one record in [start, end], each tagged with the scope(s) it appears in. A
// zero start AND end disables the time filter. Stream-identity keys (resource/scope) come from the
// authoritative series index; record-attribute keys come from the head buffers and each in-window
// part's persisted key footer. Window precision is part-granular and best-effort: a key whose
// records all fall outside the window may still be returned (harmless). Safe for concurrent use.
func (e *Engine) Keys(start, end int64) []KeyInfo {
	e.mu.RLock()
	defer e.mu.RUnlock()

	scopes := make(map[string]KeyScope)
	add := func(key []byte, sc KeyScope) {
		if len(key) > 0 {
			scopes[string(key)] |= sc
		}
	}

	// Stream-identity keys: resource/scope attributes of every in-window stream. The series index is
	// authoritative (it survives flush and a stateless reload), so this is sound on all backends.
	e.head.series.ForEach(func(id signal.SeriesID, s signal.Series) {
		if !e.streamInRangeLocked(id, start, end) {
			return
		}

		for i := range s.Resource.Attributes {
			add(s.Resource.Attributes[i].Key, KeyScopeResource|KeyScopeIndexed)
		}

		for i := range s.Scope.Attributes {
			add(s.Scope.Attributes[i].Key, KeyScopeScope|KeyScopeIndexed)
		}

		if len(s.Scope.Name) > 0 {
			add(labelScopeName, KeyScopeScope|KeyScopeIndexed)
		}

		if len(s.Scope.Version) > 0 {
			add(labelScopeVersion, KeyScopeScope|KeyScopeIndexed)
		}
	})

	// Column-stored keys: the head's still-buffered records (plus any detached mid-flush) and each
	// in-window part's footer. These are collected apart from the identity keys because a column's
	// scope is its OTLP *provenance*, not its query mechanism — see mergeColumnScopes.
	colScopes := make(map[string]KeyScope)
	collect := func(k KeyInfo) {
		if len(k.Key) > 0 {
			colScopes[string(k.Key)] |= k.Scope
		}
	}

	collectRecordKeys(e.cfg.Schema, e.head.records, start, end, collect)
	collectRecordKeys(e.cfg.Schema, e.flushing, start, end, collect)

	for _, p := range e.parts {
		if !partInWindow(p, start, end) {
			continue
		}

		for _, k := range p.recordKeys {
			collect(k)
		}
	}

	mergeColumnScopes(scopes, colScopes)

	return keyInfoSlice(scopes)
}

// mergeColumnScopes folds the serialized-attribute columns' keys (colScopes, keyed by the OTLP
// provenance of the column each came from) into the identity keys collected from the series index.
//
// A record-provenance key is condition-only, always. A resource-provenance key is a schema storing
// a stream's resource attributes on every record: it is condition-answerable only where it is *not*
// already in the stream key, so it takes [KeyScopeRecord] only when the series index did not report
// it as [KeyScopeIndexed]. A key that is genuinely both — a resource attribute on one stream and a
// record attribute on another — still ends up with both bits, which tells a caller neither pushdown
// is sound on its own.
func mergeColumnScopes(scopes, colScopes map[string]KeyScope) {
	for key, sc := range colScopes {
		if sc&KeyScopeRecord != 0 {
			scopes[key] |= KeyScopeRecord
		}

		if sc&KeyScopeResource != 0 {
			scopes[key] |= KeyScopeResource

			if scopes[key]&KeyScopeIndexed == 0 {
				scopes[key] |= KeyScopeRecord
			}
		}
	}
}

// partInWindow reports whether part p's record bounds overlap [start, end]. A zero start AND end
// disables the filter (mirrors [Engine.streamInRangeLocked]).
func partInWindow(p *part, start, end int64) bool {
	if start == 0 && end == 0 {
		return true
	}

	return p.maxTime >= start && p.minTime <= end
}

// keyInfoSlice projects the accumulated key→scope map into a deterministic, key-sorted slice.
func keyInfoSlice(scopes map[string]KeyScope) []KeyInfo {
	if len(scopes) == 0 {
		return nil
	}

	out := make([]KeyInfo, 0, len(scopes))
	for k, sc := range scopes {
		out = append(out, KeyInfo{Key: []byte(k), Scope: sc})
	}

	sort.Slice(out, func(i, j int) bool { return string(out[i].Key) < string(out[j].Key) })

	return out
}

// collectRecordKeys calls emit for every attribute key buffered in the schema's serialized-attribute
// columns, for records falling in [start, end] (a zero start AND end disables the filter), tagged
// with the emitting column's scope. No-op when the schema has no such column or records is nil.
// Caller holds the engine lock.
func collectRecordKeys(schema *Schema, records map[signal.SeriesID]*recordCols, start, end int64, emit func(KeyInfo)) {
	cols := schema.attrsByteCols()
	if len(cols) == 0 {
		return
	}

	all := start == 0 && end == 0

	for _, buf := range records {
		if buf.len() == 0 {
			continue
		}
		// When the window covers the whole buffer, the in-window distinct keys equal the buffer's
		// distinct keys — serve them from the (append-invalidated) cache instead of decoding every
		// record's attributes per Keys call.
		if all || (start <= buf.tsMin && buf.tsMax <= end) {
			for _, k := range buf.distinctAttrKeys() {
				emit(k)
			}
			continue
		}

		// Partial overlap: only the in-window records' keys count, so scan them.
		for _, k := range cols {
			sc := schema.keyScope(k)
			blobs := &buf.bytes[k]

			for i := range buf.ts {
				if buf.ts[i] < start || buf.ts[i] > end {
					continue
				}
				forEachAttrKey(blobs.at(i), func(key []byte) { emit(KeyInfo{Key: key, Scope: sc}) })
			}
		}
	}
}

// forEachAttrKey decodes the canonical serialized-attributes blob and calls fn for each attribute
// key (keys alias blob). A malformed blob is skipped — enumeration is best-effort metadata.
func forEachAttrKey(blob []byte, fn func(key []byte)) {
	if len(blob) == 0 {
		return
	}

	a, _, err := signal.DecodeAttributes(blob)
	if err != nil {
		return
	}

	for i := range a {
		fn(a[i].Key)
	}
}

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
	// KeyScopeResource marks a resource attribute (part of the stream identity, postings-indexed).
	KeyScopeResource KeyScope = 1 << iota
	// KeyScopeScope marks an instrumentation-scope attribute (also stream identity).
	KeyScopeScope
	// KeyScopeRecord marks a per-record attribute (the serialized attrs column).
	KeyScopeRecord
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
			add(s.Resource.Attributes[i].Key, KeyScopeResource)
		}

		for i := range s.Scope.Attributes {
			add(s.Scope.Attributes[i].Key, KeyScopeScope)
		}

		if len(s.Scope.Name) > 0 {
			add(labelScopeName, KeyScopeScope)
		}

		if len(s.Scope.Version) > 0 {
			add(labelScopeVersion, KeyScopeScope)
		}
	})

	// Record-attribute keys: the head's still-buffered records (plus any detached mid-flush) and each
	// in-window part's footer.
	emitRecord := func(key []byte) { add(key, KeyScopeRecord) }
	collectRecordKeys(e.cfg.Schema, e.head.records, start, end, emitRecord)
	collectRecordKeys(e.cfg.Schema, e.flushing, start, end, emitRecord)

	for _, p := range e.parts {
		if !partInWindow(p, start, end) {
			continue
		}

		for _, key := range p.recordKeys {
			add(key, KeyScopeRecord)
		}
	}

	return keyInfoSlice(scopes)
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

// collectRecordKeys calls emit for every per-record attribute key buffered in records whose record
// falls in [start, end] (a zero start AND end disables the filter). No-op when the schema has no
// serialized-attributes column or records is nil. Caller holds the engine lock.
func collectRecordKeys(schema *Schema, records map[signal.SeriesID]*recordCols, start, end int64, emit func(key []byte)) {
	k, ok := schema.attrsByteCol()
	if !ok {
		return
	}

	all := start == 0 && end == 0

	for _, buf := range records {
		blobs := buf.bytes[k]
		for i := range buf.ts {
			if !all && (buf.ts[i] < start || buf.ts[i] > end) {
				continue
			}

			forEachAttrKey(blobs[i], emit)
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

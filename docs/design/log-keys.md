# Design: `LogKeys` — log attribute-key enumeration

Status: **proposal**

## Summary

Add a read-side primitive that enumerates the distinct **attribute keys** present in a tenant's log
records over a time window, each tagged with the **scope(s)** it was seen in (resource, scope,
record). It is the counterpart to `Storage.LogSeries` (which enumerates only stream *identities*) and
the enabler for label-name listing and label-name → key resolution above the fetch seam.

```go
// LogKeys returns the distinct attribute keys present in a tenant's log records within [start, end],
// each tagged with the scopes it appears in. Keys are interned, low-cardinality metadata. A zero
// start AND end disables the time filter. Local to this node; cluster fan-out is a follow-up.
func (s *Storage) LogKeys(ctx context.Context, tenant signal.TenantID, start, end int64) ([]KeyInfo, error)

// KeyInfo is a distinct attribute key and the scopes it was observed in.
type KeyInfo struct {
	Key   []byte
	Scope KeyScope
}

// KeyScope is a bitset of the scopes an attribute key appears in. A key can appear in more than one
// (e.g. as a resource attribute on one stream and a record attribute on another).
type KeyScope uint8

const (
	KeyScopeResource KeyScope = 1 << iota // resource attribute (stream identity)
	KeyScopeScope                         // instrumentation-scope attribute (stream identity)
	KeyScopeRecord                        // per-record attribute (attrs column)
)
```

`KeyScopeResource | KeyScopeScope` are *stream* scopes (postings-indexed). `KeyScopeRecord` is the
attrs column. The bitset is the whole point: it lets the caller tell, authoritatively, whether a key
is a stream label, a record attribute, or **both**.

## Motivation

The embedded oteldb log backend resolves a LogQL label to the storage by enumerating
`LogSeries` — which only knows resource/scope keys. That leaves three gaps, all stemming from "we
can't see record-attribute keys":

1. **Record-attribute label listing is incomplete.** `/loki/api/v1/labels` and
   `/label/<name>/values` go through `LogSeries`, so for Loki-style logs (stream labels arrive as
   *record* attributes with an empty resource) `job`/`filename` work in queries but **do not appear**
   in the label browser. This is a functional gap, not just performance.
2. **Dotted record-attribute pushdown is impossible.** LogQL normalizes label names
   (`http.method` → `http_method`); the storage keys attrs raw. Without the raw keys, the querier
   can only push the *clean* subset (names with no normalization, where the raw key is the label
   itself). `http_method` and friends fall back to a full in-memory scan.
3. **Mixed-scope is unsound to push.** A label that is a resource attribute on some streams and a
   record attribute on others cannot be safely pushed as a stream `Matcher` (it would miss the
   record matches) without knowing it spans both scopes.

`LogKeys` closes all three with one primitive, and keeps the storage **language-agnostic**: it
returns raw bytes; the caller (oteldb) owns the normalization rule (`KeyToLabel`).

### Why not normalize / store aliases in the engine

- **Normalize keys in place** breaks OTLP round-trip (`TraceByID`, profile merge, backup/restore all
  reconstruct the original keys; the Tempo conformance suite asserts exact attributes) and is lossy
  on collisions.
- **Store oteldb-computed normalized aliases** puts language artifacts into the index/data, needs
  `colValue`-by-alias or duplicate attrs in the blob (round-trip pollution again), and doubles index
  entries.

Enumeration keeps normalization in the language layer where it belongs.

## How oteldb consumes it

At query time (result cached; the key set changes slowly), oteldb builds
`normalizedLabel → []{rawKey, scope}` from `LogKeys`, then for each equality matcher / pre-parser
label filter on label `L`:

| `L` resolves to… | action |
|---|---|
| exactly one stream key, no record key | postings `Matcher` (prune streams) |
| exactly one record key, no stream key | per-record `Condition` (drop records) |
| both scopes, multiple keys, or none | fall back to in-memory `matchSelector` |

This supersedes today's `LogSeries`-based resolution and is **sound on all backends** (the enumeration
is authoritative, not ingestion-observed), fixing mixed-scope as a bonus. It also lets `LabelNames`
return record-attribute keys.

A stale cache only ever *misses* keys (→ fall back), never invents a wrong mapping, since a key's
scope is immutable — so caching is safe and a generation/TTL is enough.

## Implementation sketch

Record-attribute values are high-cardinality, but **keys are not** — a bounded set per stream schema.
So track a per-engine distinct-key set, not values:

- **Head:** on append, the record's attribute keys are already decoded for the attrs bloom; intern
  each into a per-engine key set with `KeyScopeRecord`. Resource/scope keys are already interned by
  `indexLabels`; tag them `KeyScopeResource`/`KeyScopeScope`.
- **Flush:** persist the key set (interned ids + scope bits) in the part footer, next to the bloom.
- **Read:** merge the head set with the sets of the flushed parts overlapping `[start,end]`, union
  the scope bits per key, and project interned ids back to bytes.

Cost is one small set per part; no per-value tracking. Window precision is best-effort (a key whose
records all fall outside the window may still be returned — harmless: it just yields a `Condition`
that matches nothing).

## Edge cases

- **Collisions** (`http.method` + `http_method` → same normalized label): the caller sees two raw
  keys → ambiguous → fall back. Rare.
- **Mixed scope**: surfaced via the scope bitset → caller falls back. The whole reason the field is a
  bitset.
- **Parsed fields** (`| json | level=…`): not the storage's concern — oteldb only resolves filters
  that appear before any label-producing stage.
- **Value typing for the equality**: out of scope here. Pushed conditions carry an exact `Match`; a
  bloom `Equal` token hint for record attrs would need value-type parity and is a separate decision.
- **Multi-tenant / federated**: same scoping as `LogSeries` (one tenant, or all).
- **Empty store / unknown tenant**: returns nil.

## Non-goals (separate follow-ups)

- **Record-attribute `LabelValues`** — distinct *values* per key are high-cardinality; this needs
  either a bounded `LogValues(key, start, end, limit)` enumeration or a fetch-and-collect in oteldb.
  `LogKeys` deliberately stops at keys.
- **Cluster fan-out** for `LogKeys` (mirror the eventual `LogSeries` fan-out).

## Compatibility

Additive: a new method on `*Storage`. Existing callers are unaffected; oteldb feature-detects and
keeps the `LogSeries`-based clean-label path as a fallback when querying against an older storage
version.

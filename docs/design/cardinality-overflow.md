# Design: soft cardinality budget with `__overflow__` routing (Track 3a)

**Status:** proposal (design only — no code yet). For review before implementation.

This designs the replacement of the hard `MaxSeries` reject (`ARCHITECTURE.md`, "Admission control") with a soft
budget that, once exceeded, **routes new series into a synthetic overflow series** rather than
dropping them — keeping a tenant queryable and its aggregates approximately correct under a
cardinality spike.

## What changed from the original plan

The roadmap entry (`docs/design/improvement-roadmap.md` §3a) assumed a Prometheus-style
*active-series* model with hysteresis. Two findings from the code revise that:

1. **No hysteresis.** The head's series index (`engine/head.go` `h.series`) is **monotonic** within
   an engine's lifetime: `head.detach()` swaps out the sample buffers on flush but never shrinks the
   index (it is the authoritative index of every series ever seen, flushed or not, that queries rely
   on). So cardinality only ever *grows* — there is no "drops back below the line" event for
   hysteresis to react to. Hysteresis is therefore **out of scope**; once the budget is crossed the
   engine stays in overflow until restart (or until retention drops parts *and* we also prune the
   index — a separate, larger change tracked below).

2. **Overflow identity can't be built in the head.** Routing overflow to a per-metric
   `{__name__, __overflow__="true"}` series needs `__name__` — a **metric** concept. The head and
   `recordengine` are deliberately signal-agnostic (`ARCHITECTURE.md`, "Cross-cutting invariants":
   "Storage never learns a language's or signal's concepts"). So the overflow *identity* must be
   supplied by the caller via a
   callback; the head only decides *when* to overflow and appends to the id the caller hands it.

## Proposed design

### 1. Limits: a soft budget

`tenant.Limits` keeps `MaxSeries` (the hard ceiling) and gains an optional soft line:

```go
// MaxSeriesSoft, when 0 < MaxSeriesSoft <= MaxSeries, is the point past which a *new* series is
// routed to an overflow series instead of admitted normally; MaxSeries stays the hard ceiling
// (past it, even overflow stops and the sample is rejected). 0 ⇒ no soft budget (today's behavior:
// a hard reject at MaxSeries).
MaxSeriesSoft int64
```

Semantics, evaluated in the head under the engine lock (race-free, like today):
- `series.Len() < MaxSeriesSoft` (or soft unset): admit normally.
- `MaxSeriesSoft <= series.Len() < MaxSeries`: a **new** series is **overflowed** (its samples go to
  the overflow series); a **known** series is unaffected.
- `series.Len() >= MaxSeries`: hard reject (overflow series creation also stops; but the overflow
  series itself, created earlier, is exempt and keeps accepting).

### 2. Head: an overflow remapper callback

`engine.AppendLimits` gains:

```go
// Overflow, when non-nil, remaps a new series that crosses the soft budget to an overflow series
// (the caller builds its identity — e.g. {__name__, __overflow__} — so the head stays
// signal-agnostic). nil ⇒ a hard reject at MaxSeries (today's behavior).
Overflow func(s signal.Series) signal.Series
MaxSeriesSoft int64
```

`head.appendByID`, on the new-series cardinality decision:
- under soft: existing path.
- soft..hard with `Overflow != nil`: `ov := Overflow(materialize()); oid := ov.Hash()`; register `ov`
  **exempt from the cap** (so it can exist past the soft line) if new; append the sample to `oid`'s
  buffer. Return a new outcome `admittedOverflow` plus the overflow id+series, so the caller logs the
  WAL under the *overflow* identity and counts an overflow.
- at/over hard, or `Overflow == nil`: `rejectCardinality` (today).

The overflow closure fires **only on the degraded path** (a new series past the soft line), never in
steady state, so the zero-alloc hot path is untouched.

### 3. WAL consistency

`appendByID` already returns `(outcome, isNew, series)`. Extend it to also return the **effective id**
(the overflow id when redirected). `engine.AppendBatch` then logs `walB.add(effectiveID, ts, value,
sf, isNew, effectiveSeries)` — so replay reconstructs the overflow series, matching the head. (Same
for the single-sample `Append`.) Without this, replay would resurrect the original over-cap identity
and diverge from the live head.

### 4. Accounting

Add `Overflowed int64` to `AppendResult` and to `AdmissionStats`; the facade folds it into the OTLP
reply (overflowed points count as **accepted** — the data is retained, just relabeled) and the
admission meta-metric (`storage.ingest.overflowed` by signal). `Inspect`'s `TenantStats.Admission`
surfaces it.

### 5. The overflow remapper (facade, metric-aware)

The metric facade supplies `Overflow`: strip every label except `__name__`, add `__overflow__="true"`.
This keeps per-metric `sum`/`count` approximately right (all overflowed series of a metric collapse
into one bucket) while bounding cardinality to ~one extra series per metric name. Record signals
(logs/traces/profiles) keep the **hard reject** for now (no overflow callback): collapsing a
log/trace stream loses the per-stream rows that are the data, so overflow there needs a different
model (a future item).

### 6. Scope / non-goals

- Metrics only (record-signal overflow is a separate design).
- No hysteresis (monotonic index — see above). A follow-on could prune the series index when
  retention drops the last part referencing a series, which would make the soft budget breathe; that
  is a larger change to the index/retention interaction, tracked separately.
- Cluster: the soft budget is enforced on the shard primary (where the head lives), like the other
  head valves (Track 4b) — no central coordination (that is the separate central→edge feedback item).

## Testing

- Property: under a series flood with `MaxSeriesSoft < MaxSeries`, total admitted points
  (normal + overflow) exceeds the hard-reject baseline; distinct stored series ≈ `MaxSeriesSoft` +
  one overflow series per metric name; nothing exceeds `MaxSeries`.
- WAL: ingest past the soft line, replay into a fresh engine, assert the overflow series (not the
  original identities) is reconstructed with the right sample count.
- Accounting: `AppendResult.Overflowed` and `AdmissionStats` match the redirected count; overflowed
  points are counted accepted, not rejected.
- Hot-path: an unsampled, under-budget batch still does zero allocs (the overflow closure never
  fires) — guard in `efficiency_test.go`.

## Files touched (estimate)

`tenant/tenant.go` (soft limit), `engine/admission.go` (AppendLimits + AppendResult fields),
`engine/head.go` (appendByID overflow branch + effective-id return), `engine/engine.go`
(AppendBatch/Append WAL-id threading), `admission.go` + `storage.go` (facade remapper + accounting),
`internal/obs` (overflowed counter), `inspect.go` (surface it). Plus tests + an `ARCHITECTURE.md`
("Admission control") update.

// Package signal holds the OTel data model, signal-neutral types shared across
// metrics, logs, traces, and profiles: the [Signal] enum, [TenantID], and the typed
// attribute identity primitives ([Value], [KeyValue], [Attributes], [SeriesID]).
//
// Per-signal point types live under sub-packages: [signal/metric] (the first
// vertical), and signal/log, signal/trace, signal/profile (later verticals).
package signal

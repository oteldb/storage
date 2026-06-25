// Package signal holds the OTel data model, signal-neutral types shared across
// metrics, logs, traces, and profiles: the [signal.Signal] enum, [signal.TenantID],
// and common label/identity primitives.
//
// Per-signal point types live under sub-packages: [signal/metric] (the first
// vertical), and signal/log, signal/trace, signal/profile (later verticals).
package signal

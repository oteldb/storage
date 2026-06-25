// Package storage is the embeddable entry point for the oteldb storage library.
//
// It is a low-level, distributed, OpenTelemetry-centric storage engine for signals
// (metrics, logs, traces, profiles), exposed as a small [Storage] facade. Everything
// else is an implementation detail behind this package. See the package layout and
// milestone plan in DESIGN.md at the repository root.
//
// The in-memory engine ([InMemory]) is the default in tests: a full ingest+query path
// with WAL and durable flush disabled.
package storage

// Package query holds the query layer and its neutral result types (see [Result]).
//
// Sub-packages:
//   - fetch: the dual-shape Request/Fetcher/Iterator contract — the seam every language
//     front-end compiles to and every backend implements.
//   - promql: the PromQL front-end, an adapter from the fetch contract to the Prometheus
//     promql.Engine.
//
// LogQL/TraceQL/cross-signal front-ends and a sharded execution layer are later milestones.
// See ARCHITECTURE.md for the query layer's current state.
package query

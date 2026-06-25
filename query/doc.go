// Package query implements the L4–L6 query layers (DESIGN.md §3, §14 M4–M7):
//   - fetch: the dual-shape FetchRequest/Fetcher/Iterator contract (the seam)
//   - plan: logical plan IR, Shardable, lowering
//   - exec: streaming pull engine, results cache
//   - promql/logql/traceql/genericql: language front-ends compiling to the fetch contract
//
// Not yet implemented (M4+).
package query

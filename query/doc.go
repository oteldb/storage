// Package query groups the storage library's read seam and its language adapters.
//
// Sub-packages:
//   - fetch: the dual-shape Request/Fetcher/Iterator contract — the language-agnostic seam
//     every embedder query engine drives and every backend implements. This is the library's
//     query surface (exposed via Storage.Fetcher); the storage library does not implement
//     query languages.
//   - promql: an OPTIONAL adapter bridging the fetch contract to the Prometheus
//     storage.Queryable, for embedders that drive the Prometheus PromQL engine. It is the
//     only package importing github.com/prometheus/prometheus.
//
// See query/ARCH.md for the query layer's current state.
package query

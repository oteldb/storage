// Package metric implements the metrics signal: OTLP metric point types, temporality,
// series identity, and the OTLP → internal columnar builder projection.
//
// Identity (DESIGN.md §6): Resource attrs + Scope + metric name + unit + point kind +
// temporality + monotonicity + point attributes, canonicalized to a sorted label set
// and hashed to a SeriesID. Resource/Scope are hoisted and interned once.
package metric

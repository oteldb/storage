// Package metric implements the metrics signal: OTLP metric point types, temporality,
// series identity, and the OTLP → internal projection. A metric series identity extends
// the signal-neutral identity backbone (Resource + Scope + point attributes) with the
// metric-specific identifying fields — name, unit, point kind, temporality and
// monotonicity (description is explicitly not identifying).
package metric

// Package trace holds the traces signal's ingest model. The traces vertical is not yet
// built; [Traces] is a placeholder batch type so the storage facade stays pdata-free. It
// will grow the same []byte-based, resettable, OTLP-shaped shape as signal/metric's Metrics
// when the vertical lands.
package trace

// Traces is the internal traces ingest batch (placeholder until the traces vertical).
type Traces struct {
	// TODO(traces vertical): Resources []ResourceSpans, mirroring metric.Metrics.
}

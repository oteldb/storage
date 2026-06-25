// Package log holds the logs signal's ingest model. The logs vertical is not yet built;
// [Logs] is a placeholder batch type so the storage facade stays pdata-free. It will grow
// the same []byte-based, resettable, OTLP-shaped shape as signal/metric's Metrics when the
// vertical lands.
package log

// Logs is the internal logs ingest batch (placeholder until the logs vertical).
type Logs struct {
	// TODO(logs vertical): Resources []ResourceLogs, mirroring metric.Metrics.
}

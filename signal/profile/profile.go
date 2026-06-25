// Package profile holds the profiles signal's ingest model. The profiles vertical is not
// yet built; [Profiles] is a placeholder batch type so the storage facade stays pdata-free.
// It will grow the same []byte-based, resettable, OTLP-shaped shape as signal/metric's
// Metrics when the vertical lands.
package profile

// Profiles is the internal profiles ingest batch (placeholder until the profiles vertical).
type Profiles struct {
	// TODO(profiles vertical): Resources []ResourceProfiles, mirroring metric.Metrics.
}

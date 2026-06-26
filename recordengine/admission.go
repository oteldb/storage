package recordengine

// AppendLimits are the per-call admission limits the record head enforces while buffering a batch.
// The zero value imposes no limit; they are passed per [Engine.AppendBatch] call so a consumer's
// hot-reloaded tenant policy takes effect on the next write. The engine stays policy-agnostic — it
// sees only these numbers. They mirror the metric engine's limits so the facade maps one
// tenant.Limits to both.
type AppendLimits struct {
	// MaxSeries caps the number of distinct streams buffered in the head. A record that would
	// register a new stream once the head already holds MaxSeries is rejected (the whole batch,
	// since a batch is one stream); known streams are unaffected. 0 ⇒ unlimited.
	MaxSeries int64
	// MaxInFlightBytes caps the head's buffered record bytes. A record arriving while the head is
	// at or over the cap is rejected (memory backpressure) until a flush drains it. 0 ⇒ unlimited.
	MaxInFlightBytes int64
}

// AppendResult reports the disposition of an [Engine.AppendBatch] run by reason, so the caller can
// attribute an OTLP partial-success precisely.
type AppendResult struct {
	Accepted            int
	RejectedOOO         int // older than the out-of-order window
	RejectedCardinality int // would exceed AppendLimits.MaxSeries (a new stream)
	RejectedBytes       int // head at or over AppendLimits.MaxInFlightBytes
}

// Rejected returns the total number of rejected records across all reasons.
func (r AppendResult) Rejected() int {
	return r.RejectedOOO + r.RejectedCardinality + r.RejectedBytes
}

// admitOutcome is the per-record decision inside the head (cardinality is decided per batch in
// [Engine.AppendBatch], so it is not a per-record outcome).
type admitOutcome uint8

const (
	admitted admitOutcome = iota
	rejectOOO
	rejectBytes
)

// recByteSize is the in-flight memory charged for a buffered record: its timestamp, its fixed int
// columns, and the lengths of its byte columns. It measures the dominant, content-proportional cost.
func recByteSize(r rec) int64 {
	n := int64(8 + 8*len(r.ints))
	for _, b := range r.bytes {
		n += int64(len(b))
	}

	return n
}

package engine

// SampleBytes is the in-flight memory charged per buffered sample: an int64 timestamp plus a
// float64 value. It is the unit for the head's byte accounting and for callers sizing a batch
// against a rate budget. It deliberately ignores per-series index overhead (a small, amortized
// constant), so it measures the dominant, sample-proportional cost of the unflushed head.
const SampleBytes = 16

// AppendLimits are the per-call admission limits the head enforces while buffering a batch. The
// zero value imposes no limit. They are passed per [Engine.AppendBatch] call (not stored on the
// engine) so a consumer's hot-reloaded tenant policy takes effect on the next write. The engine
// stays policy-agnostic — it sees only these numbers, never a tenant or a [tenant.Policy].
type AppendLimits struct {
	// MaxSeries caps the number of distinct series buffered in the head. A sample that would
	// register a new series once the head already holds MaxSeries is rejected (cardinality
	// backpressure); samples for already-known series are unaffected. 0 ⇒ unlimited.
	MaxSeries int64
	// MaxInFlightBytes caps the head's buffered sample bytes ([SampleBytes] each). A sample
	// arriving while the head is at or over the cap is rejected (memory backpressure) until a
	// flush drains the head. 0 ⇒ unlimited.
	MaxInFlightBytes int64
}

// AppendResult reports the disposition of an [Engine.AppendBatch] run: how many samples were
// accepted and how many were rejected, by reason, so the caller can attribute an OTLP
// partial-success precisely.
type AppendResult struct {
	Accepted            int
	RejectedOOO         int // older than the out-of-order window
	RejectedCardinality int // would exceed AppendLimits.MaxSeries (a new series)
	RejectedBytes       int // head at or over AppendLimits.MaxInFlightBytes
}

// Rejected returns the total number of rejected samples across all reasons.
func (r AppendResult) Rejected() int {
	return r.RejectedOOO + r.RejectedCardinality + r.RejectedBytes
}

// admitOutcome is the per-sample decision inside the head.
type admitOutcome uint8

const (
	admitted admitOutcome = iota
	rejectOOO
	rejectCardinality
	rejectBytes
)

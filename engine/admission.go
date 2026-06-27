package engine

import "github.com/oteldb/storage/signal"

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
	// MaxSeriesSoft, when 0 < MaxSeriesSoft <= MaxSeries together with a non-nil Overflow, is the
	// soft cardinality budget: a *new* series arriving once the head holds at least MaxSeriesSoft is
	// routed (via Overflow) to an overflow series instead of being registered, until MaxSeries is
	// reached (then it is rejected). 0 ⇒ no soft budget (a hard reject at MaxSeries).
	MaxSeriesSoft int64
	// Overflow, when non-nil, builds the overflow series identity for a new series that crosses the
	// soft budget (the caller supplies it so the head stays signal-agnostic — e.g. metrics map to
	// {__name__, __overflow__}). The overflow series itself is exempt from the cap. nil ⇒ no
	// overflow routing.
	Overflow func(s signal.Series) signal.Series
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
	// Overflowed counts samples for new series past the soft budget that were routed to an overflow
	// series instead of being rejected. They are also counted in Accepted (the data is retained).
	Overflowed int
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
	admittedOverflow // a new series past the soft budget routed to an overflow series
)

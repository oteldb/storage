package engine

import "github.com/oteldb/storage/internal/retention"

// RetentionBound identifies the policy bound behind a requested retention cutoff.
type RetentionBound = retention.Bound

const (
	// RetentionBoundNone means no retention bound is set.
	RetentionBoundNone = retention.BoundNone
	// RetentionBoundAge means the signal age binds.
	RetentionBoundAge = retention.BoundAge
	// RetentionBoundSignalBytes means the signal byte budget binds.
	RetentionBoundSignalBytes = retention.BoundSignalBytes
	// RetentionBoundPooledBytes means the tenant pooled byte budget binds.
	RetentionBoundPooledBytes = retention.BoundPooledBytes
)

func (e *Engine) recordRetentionRequest(cutoff int64, bound RetentionBound) {
	term := e.term()
	if e.cfg.Term != nil && term == 0 {
		return
	}
	e.retentionState.Record(cutoff, bound, term)
}

func (e *Engine) retentionRequest() retention.Request {
	term := e.term()
	if e.cfg.Term != nil && term == 0 {
		return retention.Request{}
	}
	return e.retentionState.Load(term)
}

func retentionStraddler(parts []*part, cutoff int64) bool {
	if cutoff <= 0 {
		return false
	}
	for _, p := range parts {
		if p.minTime < cutoff && p.maxTime >= cutoff {
			return true
		}
	}
	return false
}

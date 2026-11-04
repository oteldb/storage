package storage

import (
	"github.com/oteldb/storage/internal/retention"
	"github.com/oteldb/storage/signal"
	"github.com/oteldb/storage/tenant"
)

// RetentionBound identifies the bound behind a locally requested retention cutoff.
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

type retentionDecision struct {
	cutoff int64
	bound  RetentionBound
}

type sizeRetentionDecision struct {
	cutoffs bySignal
	bounds  [signalCount]RetentionBound
}

func (d sizeRetentionDecision) at(sig signal.Signal) retentionDecision {
	if int(sig) >= len(d.bounds) {
		return retentionDecision{}
	}
	return retentionDecision{d.cutoffs[sig], d.bounds[sig]}
}

func retentionDecisionFor(r tenant.Retention, sig signal.Signal, now int64, size retentionDecision) retentionDecision {
	age := retentionCutoff(r, sig, now)
	if age > 0 && age >= size.cutoff {
		return retentionDecision{age, RetentionBoundAge}
	}
	if size.cutoff <= 0 {
		return retentionDecision{}
	}
	return size
}

func (s *Storage) retentionFor(tid signal.TenantID, sig signal.Signal, size retentionDecision) retentionDecision {
	r := s.tenant.Resolve(s.normalizeTenant(tenantOfShard(tid))).Retention
	return retentionDecisionFor(r, sig, s.now(), size)
}

func (s *Storage) attachRetention(out *SignalStats, tid signal.TenantID, cutoff int64, bound RetentionBound) {
	if !s.claimsShard(tid) {
		return
	}
	out.RetainFromUnixNano, out.RetentionBoundBy = cutoff, bound
}

func retentionLag(cutoff, oldest int64, parts int) float64 {
	if cutoff <= 0 || parts == 0 || oldest >= cutoff {
		return 0
	}
	return float64(cutoff)/1e9 - float64(oldest)/1e9
}

package storage

import (
	"math"

	"github.com/oteldb/storage/engine"
	"github.com/oteldb/storage/signal"
)

// MetricReadRevision identifies a local metric read view and its age-retention deadline.
type MetricReadRevision struct {
	Revision engine.ReadRevision
	// ValidUntil is an exclusive unix-nanosecond deadline; zero means no age expiry.
	ValidUntil int64
}

// MetricReadRevision returns a reusable read token for windows starting at minInputNS.
// Clustered, closed, expired and unrepresentable windows cannot reuse completed results.
func (s *Storage) MetricReadRevision(t signal.TenantID, minInputNS int64) (MetricReadRevision, bool) {
	if s.closed.Load() || s.cluster != nil {
		return MetricReadRevision{}, false
	}
	t = s.normalizeTenant(t)
	var out MetricReadRevision
	policyTenant := s.normalizeTenant(tenantOfShard(t))
	age := int64(s.tenant.Resolve(policyTenant).Retention.AgeFor(signal.Metric))
	if age > 0 {
		if minInputNS > math.MaxInt64-age {
			return out, false
		}
		out.ValidUntil = minInputNS + age
		if out.ValidUntil == 0 || s.now() >= out.ValidUntil {
			return out, false
		}
	}
	eng, exists := s.lookupEngine(t)
	if !exists {
		return out, true
	}
	var ok bool
	out.Revision, ok = eng.ReadRevision()
	return out, ok
}

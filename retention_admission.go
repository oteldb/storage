package storage

import (
	"math"

	"github.com/oteldb/storage/signal"
	"github.com/oteldb/storage/tenant"
)

type writeTimeFloor struct {
	timestamp int64
	enabled   bool
}

func (s *Storage) writeTimeFloor(policy tenant.Retention, sig signal.Signal) writeTimeFloor {
	if !s.opts.RejectOutOfRetention {
		return writeTimeFloor{}
	}
	age := policy.AgeFor(sig)
	if age <= 0 {
		return writeTimeFloor{}
	}
	now := s.now()
	floor := int64(math.MinInt64)
	if now >= math.MinInt64+int64(age) {
		floor = now - int64(age)
	}
	return writeTimeFloor{timestamp: floor, enabled: true}
}

func (a *tenantAdmission) sampleRetained(budget, now int64, ids []signal.SeriesID, ts []int64, floor writeTimeFloor) ([]float64, int64) {
	return a.sampleBatchWithFloor(budget, now, ids, ts, floor)
}

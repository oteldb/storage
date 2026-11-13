package series

import (
	"math"

	"github.com/oteldb/storage/signal"
)

const (
	templateProbeWindow = 4096
	templateRetryDelay  = 4 * templateProbeWindow
)

type templateEvidence struct{ fingerprint, uses uint64 }

// Fingerprints guide allocation only; the owning map still checks exact candidate keys.
func (s *templateShard) observe(hash signal.SeriesID) uint64 {
	slot, fingerprint := (hash.Lo/64)%128, hash.Hi|1
	next := (slot + 1) % 128
	entry := &s.probation[slot]
	if entry.fingerprint != fingerprint {
		if s.probation[next].fingerprint != fingerprint {
			s.probation[next], *entry = *entry, templateEvidence{fingerprint: fingerprint, uses: 1}
			return 1
		}
		entry = &s.probation[next]
	}
	if entry.uses < math.MaxUint64 {
		entry.uses++
	}
	return entry.uses
}

type templatePolicy struct{ attempts, recurrent, skip uint32 }

func (p *templatePolicy) probe() bool {
	if p.skip == 0 {
		return true
	}
	p.skip--
	return false
}

func (p *templatePolicy) observe(recurrent bool) {
	p.attempts++
	if recurrent {
		p.recurrent++
	}
	if p.attempts < templateProbeWindow {
		return
	}
	if p.recurrent*16 < p.attempts {
		p.skip = templateRetryDelay
	}
	p.attempts, p.recurrent = 0, 0
}

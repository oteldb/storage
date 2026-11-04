package tenant

import (
	"time"

	"github.com/go-faster/errors"

	"github.com/oteldb/storage/signal"
)

// Retention bounds a tenant's stored data. Zero bounds retain forever.
type Retention struct {
	// MaxAge is the default age bound for signals without an override.
	MaxAge time.Duration
	// MaxBytes bounds this node's stored bytes pooled across all tenant signals and shards.
	MaxBytes int64
	// PerSignal overrides age and adds a byte bound for individual signals. Zero fields inherit.
	PerSignal map[signal.Signal]SignalRetention
}

// SignalRetention bounds one signal. Zero fields are unset.
type SignalRetention struct {
	// MaxAge replaces the tenant's default age, whether longer or shorter.
	MaxAge time.Duration
	// MaxBytes adds a byte bound beside the pooled limit. The newest part is retained even over budget.
	// Pair it with [Limits.MaxPartSize], which is fixed when the engine is created, for a tighter floor.
	MaxBytes int64
}

// AgeFor returns sig's positive age override, otherwise the default age.
func (r Retention) AgeFor(sig signal.Signal) time.Duration {
	if sig >= signal.Metric && sig <= signal.Exemplar {
		if age := r.PerSignal[sig].MaxAge; age > 0 {
			return age
		}
	}
	return r.MaxAge
}

// Validate rejects negative bounds, unknown signals and signal byte budgets above the pooled bound.
func (r Retention) Validate() error {
	if r.MaxAge < 0 || r.MaxBytes < 0 {
		return errors.New("negative retention bound")
	}
	for sig, policy := range r.PerSignal {
		if sig < signal.Metric || sig > signal.Exemplar {
			return errors.Wrapf(signal.ErrUnknownSignal, "retention signal %d", sig)
		}
		if policy.MaxAge < 0 || policy.MaxBytes < 0 {
			return errors.Wrapf(errors.New("negative retention bound"), "signal %s", sig)
		}
		if r.MaxBytes > 0 && policy.MaxBytes > r.MaxBytes {
			return errors.Errorf("signal %s byte budget exceeds pooled retention", sig)
		}
	}
	return nil
}

package signal

import "github.com/go-faster/errors"

// Signal is the OTel signal kind. It is part of the [fetch.Request] identity and the
// top-level [storage.Query] routing. Values are stable (persisted/wire-stable).
type Signal uint8

const (
	// Metric is the metrics signal (the first vertical, DESIGN.md §14 M0–M7).
	Metric Signal = iota + 1
	// Log is the logs signal (later vertical, DESIGN.md §12).
	Log
	// Trace is the traces signal (later vertical, DESIGN.md §12).
	Trace
	// Profile is the profiles signal (later vertical, DESIGN.md §12).
	Profile
)

// String returns a lower-case signal name. It is stable.
func (s Signal) String() string {
	switch s {
	case Metric:
		return "metric"
	case Log:
		return "log"
	case Trace:
		return "trace"
	case Profile:
		return "profile"
	default:
		return "unknown"
	}
}

// ParseSignal decodes a Signal from its [Signal.String] form. Unknown values yield
// [ErrUnknownSignal].
func ParseSignal(s string) (Signal, error) {
	switch s {
	case "metric":
		return Metric, nil
	case "log":
		return Log, nil
	case "trace":
		return Trace, nil
	case "profile":
		return Profile, nil
	default:
		return 0, errors.Wrapf(ErrUnknownSignal, "unknown signal kind %q", s)
	}
}

// ErrUnknownSignal is returned by [ParseSignal] for an unrecognized name.
var ErrUnknownSignal = errors.New("signal: unknown signal kind")

// TenantID identifies a tenant. It is the leading key/path prefix for all data
// (DESIGN.md §9) and the argument to [tenant.Resolver] policy lookups. The zero value
// is the default tenant; callers may use any non-empty string.
//
// TenantID is compared by value (it is a string) and is safe to use as a map key.
type TenantID string

// String returns the tenant id as a string, or "default" for the zero value.
func (t TenantID) String() string {
	if t == "" {
		return "default"
	}
	return string(t)
}

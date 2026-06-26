package signal

import "github.com/go-faster/errors"

// Aggregation names how a set of samples collapses into one value. It is the shared
// vocabulary for merge-time downsampling (the engine applies it) and per-tenant
// downsampling policy (the tenant package configures it), so both layers agree on the
// rollup function without depending on each other. Values are stable.
type Aggregation uint8

const (
	// AggLast keeps the value of the newest (largest-timestamp) sample in the group. It is
	// the zero value and the safe default: it preserves a gauge's most recent reading and a
	// cumulative counter's latest total (so rate() over downsampled data stays meaningful).
	AggLast Aggregation = iota
	// AggFirst keeps the value of the oldest (smallest-timestamp) sample.
	AggFirst
	// AggMin keeps the smallest value.
	AggMin
	// AggMax keeps the largest value.
	AggMax
	// AggSum adds the values.
	AggSum
	// AggAvg keeps the arithmetic mean of the values.
	AggAvg
	// AggCount keeps the number of samples in the group (as a float64).
	AggCount
)

// Stable lower-case aggregation names (used by [Aggregation.String] and [ParseAggregation]).
const (
	nameLast  = "last"
	nameFirst = "first"
	nameMin   = "min"
	nameMax   = "max"
	nameSum   = "sum"
	nameAvg   = "avg"
	nameCount = "count"
)

// String returns a lower-case aggregation name. It is stable.
func (a Aggregation) String() string {
	switch a {
	case AggLast:
		return nameLast
	case AggFirst:
		return nameFirst
	case AggMin:
		return nameMin
	case AggMax:
		return nameMax
	case AggSum:
		return nameSum
	case AggAvg:
		return nameAvg
	case AggCount:
		return nameCount
	default:
		return "unknown"
	}
}

// ParseAggregation decodes an Aggregation from its [Aggregation.String] form. Unknown values
// yield [ErrUnknownAggregation].
func ParseAggregation(s string) (Aggregation, error) {
	switch s {
	case nameLast:
		return AggLast, nil
	case nameFirst:
		return AggFirst, nil
	case nameMin:
		return AggMin, nil
	case nameMax:
		return AggMax, nil
	case nameSum:
		return AggSum, nil
	case nameAvg:
		return AggAvg, nil
	case nameCount:
		return AggCount, nil
	default:
		return 0, errors.Wrapf(ErrUnknownAggregation, "unknown aggregation %q", s)
	}
}

// ErrUnknownAggregation is returned by [ParseAggregation] for an unrecognized name.
var ErrUnknownAggregation = errors.New("signal: unknown aggregation")

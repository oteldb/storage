package metric

import (
	"bytes"
	"slices"

	"github.com/oteldb/storage/internal/memsize"
	"github.com/oteldb/storage/signal"
)

// AppendAttributes appends canonical metric labels; the appended region must not overlap input attributes.
func (id Identity) AppendAttributes(dst signal.Attributes) signal.Attributes {
	pts := id.Series.Attributes
	reserved := [5]signal.KeyValue{
		{Key: LabelKind, Value: signal.IntValue(int64(id.Kind))},
		{Key: LabelMonotonic, Value: signal.BoolValue(id.Monotonic)},
		{Key: LabelName, Value: signal.StringValue(id.Name)},
		{Key: LabelTemporality, Value: signal.IntValue(int64(id.Temporality))},
		{Key: LabelUnit, Value: signal.StringValue(id.Unit)},
	}
	start := len(dst)
	dst = slices.Grow(dst, len(pts)+len(reserved))
	if !slices.IsSortedFunc(pts, func(a, b signal.KeyValue) int { return bytes.Compare(a.Key, b.Key) }) {
		dst = append(dst, pts...)
		dst = append(dst, reserved[:]...)
		signal.NewAttributes(dst[start:]...)
		return dst
	}
	i, j := 0, 0
	for i < len(pts) && j < len(reserved) {
		// Original point attributes precede reserved labels when malformed duplicate keys occur.
		if bytes.Compare(pts[i].Key, reserved[j].Key) <= 0 {
			dst = append(dst, pts[i])
			i++
		} else {
			dst = append(dst, reserved[j])
			j++
		}
	}
	dst = append(dst, pts[i:]...)
	return append(dst, reserved[j:]...)
}

// SeriesArena owns attribute arrays until Reset; inputs remain borrowed and immutable.
// It is not safe for concurrent use.
type SeriesArena struct{ attrs signal.Attributes }

// Materialize retains point labels for an independently addressable staged identity.
func (a *SeriesArena) Materialize(id Identity) signal.Series {
	start := len(a.attrs)
	a.attrs = id.AppendAttributes(a.attrs)
	s := id.Series
	s.Attributes = a.attrs[start:len(a.attrs):len(a.attrs)]
	return s
}

// Reset releases input references; identities returned by Materialize expire.
func (a *SeriesArena) Reset() {
	clear(a.attrs)
	if memsize.Slice(a.attrs) > 8<<20 {
		a.attrs = nil
	} else {
		a.attrs = a.attrs[:0]
	}
}

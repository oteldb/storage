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
type SeriesArena struct {
	chunks  []signal.Attributes
	current int
}

// Materialize retains point labels for an independently addressable staged identity.
func (a *SeriesArena) Materialize(id Identity) signal.Series {
	need := len(id.Series.Attributes) + 5
	if a.current < len(a.chunks) && cap(a.chunks[a.current])-len(a.chunks[a.current]) < need {
		a.current++
	}
	if a.current == len(a.chunks) {
		size := 64
		if a.current > 0 {
			size = min(4096, cap(a.chunks[a.current-1])*2)
		}
		a.chunks = append(a.chunks, make(signal.Attributes, 0, max(size, need)))
	} else if cap(a.chunks[a.current]) < need {
		a.chunks[a.current] = make(signal.Attributes, 0, need)
	}
	chunk := a.chunks[a.current]
	start := len(chunk)
	chunk = id.AppendAttributes(chunk)
	a.chunks[a.current] = chunk
	s := id.Series
	s.Attributes = chunk[start:len(chunk):len(chunk)]
	return s
}

// Reset releases input references; identities returned by Materialize expire.
func (a *SeriesArena) Reset() {
	retained := a.chunks[:0]
	left := int64(8 << 20)
	for _, chunk := range a.chunks {
		clear(chunk)
		if size := memsize.Slice(chunk); size <= left {
			retained = append(retained, chunk[:0])
			left -= size
		}
	}
	clear(a.chunks[len(retained):])
	a.chunks = retained
	a.current = 0
	if len(a.chunks) == 0 {
		a.chunks = nil
	}
}

package fetch

// Int64Bound is an inclusive or exclusive signed integer bound.
type Int64Bound struct {
	Value     int64
	Inclusive bool
}

// Int64Range hints that every accepted value of a physical int64 column lies in this range.
// Nil bounds are unbounded. It does not describe attributes or reinterpreted float values.
type Int64Range struct {
	Lower *Int64Bound
	Upper *Int64Bound
}

// Overlaps reports whether the inclusive column bounds can intersect the range.
// Invalid column statistics are unknown and retain the candidate.
func (r Int64Range) Overlaps(minimum, maximum int64) bool {
	if minimum > maximum {
		return true
	}
	if r.Lower != nil && r.Upper != nil &&
		(r.Lower.Value > r.Upper.Value || (r.Lower.Value == r.Upper.Value && (!r.Lower.Inclusive || !r.Upper.Inclusive))) {
		return false
	}
	if b := r.Lower; b != nil && (maximum < b.Value || (maximum == b.Value && !b.Inclusive)) {
		return false
	}
	if b := r.Upper; b != nil && (minimum > b.Value || (minimum == b.Value && !b.Inclusive)) {
		return false
	}
	return true
}

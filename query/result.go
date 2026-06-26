package query

// Result and its value types are the neutral query-result shape shared by the language
// front-ends (query/promql, …) and returned through the storage facade, so the public API
// never leaks a language engine's own types.

// ResultType is the kind of value a query produced (the PromQL value types).
type ResultType uint8

const (
	// ResultScalar is a single number (an instant query over a scalar expression).
	ResultScalar ResultType = iota
	// ResultVector is an instant vector: one [Point] per series at a single time.
	ResultVector
	// ResultMatrix is a range vector: a range of points per series.
	ResultMatrix
	// ResultString is a string value (rare; from string literals).
	ResultString
)

// String returns the lower-case result-type name.
func (t ResultType) String() string {
	switch t {
	case ResultScalar:
		return "scalar"
	case ResultVector:
		return "vector"
	case ResultMatrix:
		return "matrix"
	case ResultString:
		return "string"
	default:
		return "unknown"
	}
}

// Label is one name/value pair of a result series' label set.
type Label struct {
	Name  string
	Value string
}

// Point is one (timestamp, value) sample. T is unix nanoseconds (the storage time unit).
type Point struct {
	T int64
	V float64
}

// Series is one result series: its label set and its points. For a [ResultVector] each
// series carries exactly one point; for a [ResultMatrix] it carries the range.
type Series struct {
	Metric []Label
	Points []Point
}

// Result is a query result. The populated field is selected by [Result.Type]: Series for
// vector/matrix, Scalar for scalar, String for string.
type Result struct {
	Type   ResultType
	Series []Series
	Scalar Point
	String string
}

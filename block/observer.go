package block

// BytesObserver sees the values a writer encodes into a dictionary bytes column ([Column.Observer]):
// each declined granule's distinct values, then the column dictionary. Every value is reported at
// least once and the counts over all calls sum to the column's rows, so a consumer must be
// set-semantic.
//
// Calls are synchronous, on the goroutine driving the writer, and stop at Abort. Slices alias writer
// memory and are valid only during the call.
type BytesObserver interface {
	// SelfGranule reports a granule that self-encoded rather than join the column dictionary: its
	// distinct values and how many of its rows hold each.
	SelfGranule(values [][]byte, counts []uint64)
	// Dictionary reports the column dictionary and each entry's row count, once per column written
	// with an object, before that object is committed. A column that collapses to a constant gets no
	// call, though a streaming writer may already have reported its granules.
	Dictionary(entries [][]byte, counts []uint64)
}

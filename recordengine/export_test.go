package recordengine

// SetMergeSplitDict forces the merge's byte-column carry on or off for the calling test and returns
// the restore. Off is the flat path every column took before the split (union dictionary + ids)
// carry existed, and is the oracle the split path is compared against.
func SetMergeSplitDict(v bool) func() {
	old := mergeSplitDict
	mergeSplitDict = v

	return func() { mergeSplitDict = old }
}

// ObserveMergeSplit installs fn to receive each merge's per-byte-column decision (true where the
// column took the split path), and returns the removal. It is what keeps a byte-identity test from
// passing by silently exercising the fallback everywhere.
func ObserveMergeSplit(fn func(split []bool)) func() {
	old := mergeSplitObserver
	mergeSplitObserver = fn

	return func() { mergeSplitObserver = old }
}

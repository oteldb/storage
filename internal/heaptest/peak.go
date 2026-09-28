package heaptest

// Peak runs run and returns the most the heap held live at any point run marked by calling sample,
// and once more after run returns, above what it held before. Each sample collects garbage first, so
// it reads what is reachable at that point; a caller samples where what it measures is still
// referenced.
func Peak(run func(sample func())) uint64 {
	base := Live()
	peak := base

	sample := func() { peak = max(peak, Live()) }

	run(sample)
	sample()

	return peak - base
}

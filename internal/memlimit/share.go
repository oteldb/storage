package memlimit

import "math"

const (
	// mergeFraction is the reciprocal share of the process budget that all concurrent merges
	// together may hold. The rest belongs to the head, the caches and the decode budget, so merging
	// — background work — takes a minority slice.
	mergeFraction = 8

	// defaultMergeBytes is the allowance where no budget can be detected at all: no GOMEMLIMIT, no
	// cgroup, no host total.
	defaultMergeBytes = 512 << 20

	// minMergeBytes is the smallest allowance a merge is worth running in. It is what sets merge
	// concurrency on a small-memory node: the budget supports as many merges as can each hold this
	// much, so a 512 MiB pod runs one merge with a usable budget rather than sixteen that each hold
	// too little to make progress against the part count.
	//
	// The number is the read side of a streaming merge — k sources × columns × the read-ahead
	// window, ~48 MiB for 8 parts of 6 columns at 1 MiB — rounded up. Below it a merge reads so
	// little per pass that it merges fewer parts than the next flush adds.
	minMergeBytes = 64 << 20

	// queryFraction is the reciprocal share of the process budget one query may read. It is
	// deliberately a minority slice: the point of the bound is that a single query cannot evict the
	// head, the caches and every concurrent reader, so it must sit well below the whole heap.
	queryFraction = 4

	// defaultQueryBytes is the per-query allowance where no budget can be detected at all.
	defaultQueryBytes = 256 << 20
)

// MergeBudget is how many bytes every concurrent merge may hold together: the caller's configured
// allowance, or a share of the detected process budget ([Bytes]) when it is 0. A negative value
// opts out, returning [math.MaxInt64] so the caller's other bounds decide alone.
func MergeBudget(configured int64) int64 {
	switch {
	case configured < 0:
		return math.MaxInt64
	case configured > 0:
		return configured
	}

	if limit := Bytes(); limit > 0 {
		return limit / mergeFraction
	}

	return defaultMergeBytes
}

// MergeConcurrency is how many merges the budget supports: enough that each gets at least
// [minMergeBytes], never more than cpuLimit, never fewer than one.
//
// Deriving it from memory rather than from cores is the point. A merge's *allowance* is the budget
// divided by this number, so taking the divisor from the core count prices a memory quantity in
// CPUs: a 16-core pod with a 4 GiB limit would give each merge 32 MiB and produce a great many
// merges too small to keep up with ingest. cpuLimit still caps it from above — merging is CPU-bound
// work, and there is nothing to gain from more merges than there are cores to run them.
func MergeConcurrency(configured int64, cpuLimit int) int {
	budget := MergeBudget(configured)
	ceiling := max(cpuLimit, 1)

	if budget == math.MaxInt64 {
		return ceiling
	}

	return max(min(int(budget/minMergeBytes), ceiling), 1)
}

// MergeShare is how many bytes one merge may hold: [MergeBudget] divided across the merges that may
// run at once and by amplification, the peak resident per byte of the bound (a merge that holds its
// output twice over passes 2).
func MergeShare(configured int64, concurrency, amplification int) int64 {
	budget := MergeBudget(configured)
	if budget == math.MaxInt64 {
		return budget
	}

	return budget / int64(max(concurrency, 1)) / int64(max(amplification, 1))
}

// QueryShare is how many bytes one query may read before it is refused, derived from the process
// budget the same way [MergeShare] derives the merge allowance.
//
// configured is the caller's cap: 0 takes a share of the detected process budget ([Bytes]), and a
// negative value opts out. Opting out returns 0 rather than [math.MaxInt64], because the consumer
// spells "unbounded" as "install no limiter at all" — an effectively-infinite ceiling would still
// pay to count every byte it will never reject.
func QueryShare(configured int64) int64 {
	if configured < 0 {
		return 0
	}

	if configured > 0 {
		return configured
	}

	if limit := Bytes(); limit > 0 {
		return limit / queryFraction
	}

	return defaultQueryBytes
}

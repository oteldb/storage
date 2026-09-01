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

	// queryFraction is the reciprocal share of the process budget one query may read. It is
	// deliberately a minority slice: the point of the bound is that a single query cannot evict the
	// head, the caches and every concurrent reader, so it must sit well below the whole heap.
	queryFraction = 4

	// defaultQueryBytes is the per-query allowance where no budget can be detected at all.
	defaultQueryBytes = 256 << 20
)

// MergeShare is how many bytes one merge may hold: the total merge allowance divided across the
// merges that may run at once and by amplification, the peak resident per byte of the bound (a
// merge that holds its output twice over passes 2).
//
// configured is the caller's total allowance: 0 takes a share of the detected process budget
// ([Bytes]), and a negative value opts out, returning [math.MaxInt64] so the caller's other bounds
// decide alone.
func MergeShare(configured int64, concurrency, amplification int) int64 {
	if configured < 0 {
		return math.MaxInt64
	}

	if configured == 0 {
		configured = defaultMergeBytes
		if limit := Bytes(); limit > 0 {
			configured = limit / mergeFraction
		}
	}

	return configured / int64(max(concurrency, 1)) / int64(max(amplification, 1))
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

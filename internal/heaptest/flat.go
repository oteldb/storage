package heaptest

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// Run is one [Resident] measurement and the source bytes it read.
type Run struct {
	Resident, Source uint64
}

// Flat bounds how a resident set may grow when its sources grow.
type Flat struct {
	// Floor is what the code holds by design. The baseline is process-wide, so heap another test
	// frees during the small run can push it toward zero; growth is never taken against less.
	Floor uint64
	// MaxGrowth bounds large.Resident / max(small.Resident, Floor).
	MaxGrowth float64
	// SourceShare bounds large.Resident below large.Source / SourceShare, independent of the small
	// run's baseline: holding the sources whole costs about their size.
	SourceShare uint64
}

// AssertFlat asserts that large, with several times small's sources, stays within f.
func AssertFlat(tb testing.TB, small, large Run, f Flat) {
	tb.Helper()

	const mib = 1 << 20

	tb.Logf("small: source %.1f MiB, resident %.1f MiB", float64(small.Source)/mib, float64(small.Resident)/mib)
	tb.Logf("large: source %.1f MiB, resident %.1f MiB", float64(large.Source)/mib, float64(large.Resident)/mib)

	growth := float64(large.Resident) / float64(max(small.Resident, f.Floor))

	assert.Less(tb, growth, f.MaxGrowth,
		"sources grew %.1fx and the live heap grew %.1fx (%.1f → %.1f MiB): it holds its sources, "+
			"not a window of them",
		float64(large.Source)/float64(small.Source), growth,
		float64(small.Resident)/mib, float64(large.Resident)/mib)

	assert.Less(tb, large.Resident, large.Source/f.SourceShare,
		"held %.1f MiB against %.1f MiB of sources: it holds them, not a window of them",
		float64(large.Resident)/mib, float64(large.Source)/mib)
}

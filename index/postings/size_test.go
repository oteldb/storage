package postings

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/signal"
)

func TestMemPostingsSizeBytes(t *testing.T) {
	t.Parallel()

	p := NewMemPostings()
	assert.Zero(t, p.SizeBytes(), "an empty index owns no lists")

	p.Add(signal.SeriesID{Lo: 1}, 1, 10)
	one := p.SizeBytes()
	require.Positive(t, one)

	p.Add(signal.SeriesID{Lo: 2}, 1, 10)
	sameBucket := p.SizeBytes() - one

	p.Add(signal.SeriesID{Lo: 3}, 2, 20)
	newName := p.SizeBytes() - one - sameBucket

	assert.Greater(t, newName, sameBucket, "a new label name costs a map of its own")
}

func TestMemPostingsSizeBytesStableAcrossSort(t *testing.T) {
	t.Parallel()

	p := NewMemPostings()
	for i := range 100 {
		// Every series added twice: the lazy sort deduplicates in place, shrinking the lists' length
		// but not their capacity — which is what stays resident, and what SizeBytes must report.
		id := signal.SeriesID{Lo: uint64(i)}
		p.Add(id, 1, 10)
		p.Add(id, 1, 10)
	}

	before := p.SizeBytes()
	p.EnsureSorted()

	assert.Equal(t, before, p.SizeBytes(), "an in-place dedup frees nothing")
}

func TestMemPostingsSizeBytesCountsUnlabeledSeries(t *testing.T) {
	t.Parallel()

	p := NewMemPostings()
	p.AddSeries(signal.SeriesID{Lo: 1})

	assert.Positive(t, p.SizeBytes(), "a series with no labels still occupies the all-set")
}

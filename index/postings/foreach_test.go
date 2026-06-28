package postings

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/signal"
)

func TestForEachName(t *testing.T) {
	t.Parallel()

	p := buildIndex()

	type card struct {
		distinctValues int
		totalSeries    int
	}

	got := make(map[uint32]card)
	p.ForEachName(func(nameID uint32, distinctValues, totalSeries int) {
		got[nameID] = card{distinctValues, totalSeries}
	})

	require.Len(t, got, 2, "two indexed names (job, env)")
	assert.Equal(t, card{distinctValues: 2, totalSeries: 3}, got[nJob], "job: api×2, web×1")
	assert.Equal(t, card{distinctValues: 2, totalSeries: 3}, got[nEnv], "env: prod×2, dev×1")
}

// TestForEachNameDeduplicates verifies a series added more than once for the same value is counted
// once (ForEachName deduplicates like any read).
func TestForEachNameDeduplicates(t *testing.T) {
	t.Parallel()

	p := NewMemPostings()
	p.Add(sid(1), nJob, vAPI)
	p.Add(sid(1), nJob, vAPI) // duplicate triple
	p.Add(sid(2), nJob, vAPI)

	p.ForEachName(func(nameID uint32, distinctValues, totalSeries int) {
		assert.Equal(t, nJob, nameID)
		assert.Equal(t, 1, distinctValues)
		assert.Equal(t, 2, totalSeries, "the duplicate is collapsed")
	})
}

// TestForEachNameProperty cross-checks ForEachName against a naive reference over a generated index:
// each series carries one value per name, so the distinct-series count equals the sum of the
// deduplicated value buckets.
func TestForEachNameProperty(t *testing.T) {
	t.Parallel()

	const (
		names  = 5
		values = 4
		series = 200
	)

	p := NewMemPostings()

	// Reference: per name → set of distinct values, and set of distinct series.
	refValues := make(map[uint32]map[uint32]struct{})
	refSeries := make(map[uint32]map[signal.SeriesID]struct{})

	for s := range series {
		id := sid(uint64(s + 1))
		for n := range names {
			nameID := uint32(n + 1)
			valueID := uint32((s*7+n)%values + 100) // deterministic spread

			p.Add(id, nameID, valueID)
			// Add a duplicate sometimes to exercise dedup.
			if s%3 == 0 {
				p.Add(id, nameID, valueID)
			}

			if refValues[nameID] == nil {
				refValues[nameID] = make(map[uint32]struct{})
				refSeries[nameID] = make(map[signal.SeriesID]struct{})
			}

			refValues[nameID][valueID] = struct{}{}
			refSeries[nameID][id] = struct{}{}
		}
	}

	seen := make(map[uint32]struct{})
	p.ForEachName(func(nameID uint32, distinctValues, totalSeries int) {
		seen[nameID] = struct{}{}
		assert.Equal(t, len(refValues[nameID]), distinctValues, "distinct values for name %d", nameID)
		assert.Equal(t, len(refSeries[nameID]), totalSeries, "distinct series for name %d", nameID)
	})

	assert.Len(t, seen, len(refValues), "every name visited exactly once")
}

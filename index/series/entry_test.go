package series

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/signal"
)

func TestIndexAddEntry(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name  string
		input signal.Series
	}{
		{name: "empty"}, {name: "typed", input: encodingSeries()}, {name: "labels", input: mk("api", "cpu", "0")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			for _, templated := range []bool{false, true} {
				var pool *AttributeTemplates
				if templated {
					pool = NewAttributeTemplates(1<<20, []byte("cpu"))
					warmExporterCandidate(pool, tc.input)
				}
				ix := NewWithAttributeTemplates(nil, pool)
				original := tc.input.Clone()
				input := original.Clone()
				entry := Entry{ID: input.Hash(), Series: input}
				require.Equal(t, entry.ID, ix.AddEntry(entry))
				require.Equal(t, entry.ID, ix.AddEntry(entry))
				require.Equal(t, 1, ix.Len())
				want := original.AppendHashInput(nil)
				input.Resource.SchemaURL = append(input.Resource.SchemaURL[:0], "changed"...)
				for i := range input.Attributes {
					input.Attributes[i].Key = append(input.Attributes[i].Key[:0], "changed"...)
				}
				require.Equal(t, want, ix.View().AppendCanonical(nil, 0))
				require.Equal(t, entry.ID, ix.View().ID(0))
				copied := New()
				copied.AddView(ix.View(), 0)
				require.Equal(t, want, copied.View().AppendCanonical(nil, 0))
			}
		})
	}
}

//nolint:paralleltest // AllocsPerRun changes GOMAXPROCS and measures global allocation counters.
func TestSeriesOnlyEntriesReuse(t *testing.T) {
	fixture := newSeriesOnlyFixture(t, 8)
	batch := fixture.prepareSeriesBatch(0)
	entries := make([]Entry, 0, len(batch))
	buf := make([]byte, 0, 4096)
	entries, buf = prepareSeriesOnlyEntries(entries, batch, buf)
	for i, entry := range entries {
		require.Equal(t, batch[i].Hash(), entry.ID)
	}
	allocations := testing.AllocsPerRun(10, func() { entries, buf = prepareSeriesOnlyEntries(entries[:0], batch, buf) })
	require.Zero(t, allocations)
}

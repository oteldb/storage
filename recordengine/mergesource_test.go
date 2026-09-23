package recordengine_test

import (
	"context"
	"slices"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/recordengine"
)

// runMergeRead builds the case's store from scratch and merges it with every source read forward,
// or with every source decoded whole, returning the backend's objects and each merge's per-source
// read decision.
func runMergeRead(t *testing.T, c mergeCase, whole bool) (map[string][]byte, [][]bool) {
	t.Helper()

	defer recordengine.SetMergeReadWhole(whole)()

	var reads [][]bool

	defer recordengine.ObserveMergeReads(func(streamed []bool) {
		reads = append(reads, slices.Clone(streamed))
	})()

	be := backend.Memory()
	e := recordengine.New(recordengine.Config{
		Schema: c.schema, Backend: be, Prefix: "t/dict", MaxPartBytes: c.maxPart,
	})

	c.fill(t, e)

	require.NoError(t, e.Merge(context.Background(), c.retain))

	return dumpBackend(t, be), reads
}

// TestMergeStreamedMatchesWhole: reading every source forward, a granule at a time, writes exactly
// the backend a whole decode of every source writes — the read path the merge had before it could
// stream.
//
//nolint:paralleltest // flips package-level seams
func TestMergeStreamedMatchesWhole(t *testing.T) {
	for _, c := range mergeCases() {
		t.Run(c.name, func(t *testing.T) {
			whole, wholeReads := runMergeRead(t, c, true)
			got, reads := runMergeRead(t, c, false)

			require.Equal(t, whole, got, "merge output differs between the streamed and whole reads")

			if c.wantSplit == nil {
				assert.Empty(t, reads)

				return
			}

			require.NotEmpty(t, reads, "no merge ran, so the case proves nothing")

			for i := range reads {
				assert.NotContains(t, reads[i], false, "a healthy source was decoded whole")
				assert.NotContains(t, wholeReads[i], true, "a forced whole decode streamed")
			}
		})
	}
}

// FuzzMergeStreamedMatchesWhole is [TestMergeStreamedMatchesWhole] over generated stream/row shapes
// and column values.
func FuzzMergeStreamedMatchesWhole(f *testing.F) {
	f.Add(byte(2), byte(3), byte(2), []byte("ab"))
	f.Add(byte(1), byte(1), byte(1), []byte(""))
	f.Add(byte(5), byte(7), byte(4), []byte("the quick brown fox"))
	f.Add(byte(3), byte(2), byte(9), []byte{0x00, 0xff, 0x01})

	f.Fuzz(func(t *testing.T, streams, rows, parts byte, values []byte) {
		c := mergeCase{
			schema: testSchema,
			fill:   fuzzFill(int(streams)%6+1, int(rows)%9+1, int(parts)%4+2, values),
		}

		whole, _ := runMergeRead(t, c, true)
		got, reads := runMergeRead(t, c, false)

		require.Equal(t, whole, got)
		require.NotEmpty(t, reads)
	})
}

// TestMergeUnionFallbackMatchesFlat: a column whose union outgrows its bound mid-merge falls back to
// the flat carry, and the output is still the one the flat path writes from the start.
//
//nolint:paralleltest // flips package-level seams
func TestMergeUnionFallbackMatchesFlat(t *testing.T) {
	defer recordengine.SetMergeUnionEntriesPerSource(3)()

	for _, c := range mergeCases() {
		t.Run(c.name, func(t *testing.T) {
			flat, _ := runMergeCase(t, c, false)
			got, _ := runMergeCase(t, c, true)

			require.Equal(t, flat, got)
		})
	}
}

package bucketindex_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend/bucketindex"
)

func TestRemovalRoundTrip(t *testing.T) {
	t.Parallel()

	in := &bucketindex.Index{
		Entries:    []bucketindex.Entry{{Prefix: "p/2", MinTime: 1, MaxTime: 2}},
		Generation: gen(3, 9),
	}
	in.Tombstone(bucketindex.Removal{Prefix: "p/1", Generation: gen(3, 8)})
	in.Tombstone(bucketindex.Removal{Prefix: "p/0", Generation: gen(2, 4)})

	out, err := bucketindex.Decode(in.AppendBinary(nil))
	require.NoError(t, err)
	assert.Equal(t, in.Removed, out.Removed)
	assert.Equal(t, map[string]struct{}{"p/0": {}, "p/1": {}}, out.Removals())
}

// An index that predates the format states no removals, so absence in it cannot be read as one.
func TestRecordsRemovals(t *testing.T) {
	t.Parallel()

	legacy, err := bucketindex.Decode([]byte{'B', 'I', 2, 1, 1, 'a', 2, 4, 3})
	require.NoError(t, err)
	assert.False(t, legacy.RecordsRemovals())

	assert.True(t, (&bucketindex.Index{Generation: gen(1, 1)}).RecordsRemovals())
}

func TestTombstoneReplacesAndSorts(t *testing.T) {
	t.Parallel()

	var ix bucketindex.Index
	ix.Tombstone(bucketindex.Removal{Prefix: "b", Generation: gen(1, 1)})
	ix.Tombstone(bucketindex.Removal{Prefix: "a", Generation: gen(1, 2)})
	ix.Tombstone(bucketindex.Removal{Prefix: "b", Generation: gen(1, 3)})

	require.Len(t, ix.Removed, 2)
	assert.Equal(t, "a", ix.Removed[0].Prefix, "kept sorted for a deterministic encoding")
	assert.Equal(t, gen(1, 3), ix.Removed[1].Generation, "re-removal replaces rather than duplicates")
}

func TestTrimRemovals(t *testing.T) {
	t.Parallel()

	removals := []bucketindex.Removal{
		{Prefix: "p/1", Generation: gen(1, 1)},
		{Prefix: "p/2", Generation: gen(1, 2)},
		{Prefix: "p/3", Generation: gen(1, 3)},
	}

	// A part that came back is live, and live is the later statement.
	got := bucketindex.TrimRemovals(append([]bucketindex.Removal(nil), removals...),
		map[string]struct{}{"p/2": {}}, 10)
	assert.Equal(t, []string{"p/1", "p/3"}, prefixes(got))

	// Over the bound, the newest survive — and come back in prefix order.
	got = bucketindex.TrimRemovals(append([]bucketindex.Removal(nil), removals...), nil, 2)
	assert.Equal(t, []string{"p/2", "p/3"}, prefixes(got))
}

func prefixes(rs []bucketindex.Removal) []string {
	out := make([]string, len(rs))
	for i, r := range rs {
		out[i] = r.Prefix
	}

	return out
}

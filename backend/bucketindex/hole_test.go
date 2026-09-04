package bucketindex_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend/bucketindex"
)

func lostWant() bucketindex.Want {
	return bucketindex.Want{
		Prefix:  "p/0000000002",
		Blocks:  bucketindex.Interval{Min: 2, Max: 2},
		Level:   1,
		MinTime: 100,
		MaxTime: 200,
	}
}

// TestRecordHoleIsOneMutation verifies the three halves of acknowledging a loss land together: the
// hole enters Entries, the want leaves Wanted, and the data-loss count rises.
func TestRecordHoleIsOneMutation(t *testing.T) {
	t.Parallel()

	w := lostWant()
	ix := &bucketindex.Index{}
	ix.RecordWant(w)

	hole := ix.RecordHole(w)

	assert.True(t, hole.Hole)
	assert.False(t, hole.Data())
	assert.Equal(t, w.Entry().Prefix, hole.Prefix)
	assert.Equal(t, w.MinTime, hole.MinTime, "a hole is prunable by time like any entry")
	assert.Equal(t, w.MaxTime, hole.MaxTime)
	assert.Equal(t, w.Blocks, hole.Blocks)
	assert.Equal(t, w.Level, hole.Level)

	assert.Empty(t, ix.Wanted, "the hole discharges the obligation")
	assert.EqualValues(t, 1, ix.LostParts)
	assert.Equal(t, []bucketindex.Entry{hole}, ix.Holes())
}

// TestHoleIsNotAnEmptyPart is the point of the flag: a zero-row part and a hole are the same shape
// on every other axis, and only the flag tells a reader that a query over the range is short.
func TestHoleIsNotAnEmptyPart(t *testing.T) {
	t.Parallel()

	empty := bucketindex.Entry{Prefix: "p/0000000001", MinTime: 1, MaxTime: 2}
	hole := bucketindex.Entry{Prefix: "p/0000000002", MinTime: 1, MaxTime: 2, Hole: true}

	ix := &bucketindex.Index{Entries: []bucketindex.Entry{empty, hole}}

	assert.True(t, empty.Data())
	assert.False(t, hole.Data())
	assert.Equal(t, []bucketindex.Entry{hole}, ix.Holes())
	assert.Len(t, ix.Overlapping(0, 10), 2, "a hole is still pruned by time like any entry")
}

// TestHoleDoesNotSatisfyAWant separates the two questions a hole answers differently: it ends the
// obligation locally, and it never stands in for the data a peer is asked for.
func TestHoleDoesNotSatisfyAWant(t *testing.T) {
	t.Parallel()

	w := lostWant()
	ix := &bucketindex.Index{}
	ix.RecordWant(w)
	ix.RecordHole(w)

	_, ok := ix.Satisfying(w)
	assert.False(t, ok, "an acknowledged loss holds no data, so it satisfies nothing")

	got, ok := ix.Discharging(w)
	require.True(t, ok, "but the obligation is over")
	assert.True(t, got.Hole)
}

// TestTrimWantsDischargedByHole verifies the want is dropped by the commit that publishes the hole,
// which is what lets reads resume.
func TestTrimWantsDischargedByHole(t *testing.T) {
	t.Parallel()

	w := lostWant()
	hole := w.Entry()
	hole.Hole = true

	kept, dropped := bucketindex.TrimWants([]bucketindex.Want{w}, []bucketindex.Entry{hole}, 10)
	assert.Empty(t, kept)
	assert.Empty(t, dropped)
}

func TestRevokes(t *testing.T) {
	t.Parallel()

	hole := lostWant().Entry()
	hole.Hole = true

	tests := []struct {
		name string
		live bucketindex.Entry
		want bool
	}{
		{
			name: "exact prefix",
			live: bucketindex.Entry{Prefix: hole.Prefix, Blocks: hole.Blocks, Level: hole.Level},
			want: true,
		},
		{
			name: "containing successor",
			live: bucketindex.Entry{
				Prefix: "p/0000000009", Blocks: bucketindex.Interval{Min: 1, Max: 4}, Level: 2,
			},
			want: true,
		},
		{
			name: "overlapping at the same level",
			live: bucketindex.Entry{
				Prefix: "p/0000000009", Blocks: bucketindex.Interval{Min: 1, Max: 4}, Level: 1,
			},
			want: false,
		},
		{
			name: "unrelated part",
			live: bucketindex.Entry{
				Prefix: "p/0000000009", Blocks: bucketindex.Interval{Min: 7, Max: 8}, Level: 3,
			},
			want: false,
		},
		{
			name: "another hole over the same blocks",
			live: bucketindex.Entry{
				Prefix: "p/0000000009", Blocks: bucketindex.Interval{Min: 1, Max: 4}, Level: 2, Hole: true,
			},
			want: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.want, bucketindex.Revokes(tt.live, hole))
			assert.Equal(t, tt.want, len(bucketindex.TrimHoles(
				[]bucketindex.Entry{hole}, []bucketindex.Entry{tt.live})) == 0)
		})
	}
}

// TestRevokesRejectsNonHole guards the argument order: only a hole can be revoked.
func TestRevokesRejectsNonHole(t *testing.T) {
	t.Parallel()

	e := bucketindex.Entry{Prefix: "p/0000000002"}
	assert.False(t, bucketindex.Revokes(e, e))
}

// TestNextBlockCountsHoles verifies a hole keeps its blocks reserved: reusing them would let two
// different parts claim one identity, and the hole is revocable precisely so the original may
// come back.
func TestNextBlockCountsHoles(t *testing.T) {
	t.Parallel()

	hole := bucketindex.Entry{Blocks: bucketindex.Interval{Min: 4, Max: 6}, Hole: true, Prefix: "h"}
	ix := &bucketindex.Index{Entries: []bucketindex.Entry{hole}}

	assert.EqualValues(t, 7, ix.NextBlock())
}

// TestHoleEncodingRoundTrip pins that the flag and the loss count survive the wire.
func TestHoleEncodingRoundTrip(t *testing.T) {
	t.Parallel()

	w := lostWant()
	in := &bucketindex.Index{Entries: []bucketindex.Entry{{Prefix: "p/0000000001", MinTime: 1, MaxTime: 2}}}
	in.RecordHole(w)

	out, err := bucketindex.Decode(in.AppendBinary(nil))
	require.NoError(t, err)
	assert.Equal(t, in, out)
	assert.Len(t, out.Holes(), 1)
	assert.EqualValues(t, 1, out.LostParts)
}

// TestDecodeRejectsUnknownEntryFlags verifies a reserved bit is refused rather than read as a
// data-bearing part: guessing wrong there turns an acknowledged loss back into a silent one.
func TestDecodeRejectsUnknownEntryFlags(t *testing.T) {
	t.Parallel()

	_, err := bucketindex.Decode([]byte{
		'B', 'I', 5,
		1, 1, 'a', 2, 4, 0, 0, 0, 2, // entry with flag bit 1 set
		3, 4, 5, 0, 0, 0, 0,
	})
	require.ErrorIs(t, err, bucketindex.ErrCorrupt)
}

// TestWantOfCarriesIdentity verifies the round trip repair depends on: a lost entry becomes the
// want, and the want becomes the hole standing in for it.
func TestWantOfCarriesIdentity(t *testing.T) {
	t.Parallel()

	ent := bucketindex.Entry{
		Prefix: "p/0000000002", MinTime: 100, MaxTime: 200,
		Blocks: bucketindex.Interval{Min: 2, Max: 2}, Level: 1,
	}
	g := bucketindex.Generation{Term: 3, Counter: 4}

	w := bucketindex.WantOf(ent, g)
	assert.Equal(t, g, w.Generation)
	assert.Equal(t, ent, w.Entry())
}

package bucketindex_test

import (
	"fmt"
	"math"
	"math/rand/v2"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend/bucketindex"
)

func fullV5Index() *bucketindex.Index {
	return &bucketindex.Index{
		Entries: []bucketindex.Entry{
			{Prefix: "a", MinTime: 1, MaxTime: 2, Blocks: bucketindex.Interval{Min: 1, Max: 4}, Level: 2},
		},
		FlushedEpoch: 3,
		Generation:   bucketindex.Generation{Term: 4, Counter: 5},
		Removed:      []bucketindex.Removal{{Prefix: "r", Generation: bucketindex.Generation{Term: 6, Counter: 7}}},
		Epochs: []bucketindex.WriterEpoch{
			{Writer: "w", Epoch: 8, Generation: bucketindex.Generation{Term: 9, Counter: 10}},
		},
		Wanted: []bucketindex.Want{
			{
				Prefix:     "x",
				Blocks:     bucketindex.Interval{Min: 5, Max: 5},
				Generation: bucketindex.Generation{Term: 11, Counter: 12},
			},
		},
	}
}

// TestGoldenV5 pins the v5 byte layout: an accidental reordering or a dropped field breaks here
// before it breaks a deployment.
func TestGoldenV5(t *testing.T) {
	t.Parallel()

	want := []byte{
		'B', 'I', 5,
		1, 1, 'a', 2, 4, 1, 4, 2, // one entry: prefix, zigzag times, blocks [1,4], level 2
		3,    // flushed epoch
		4, 5, // generation
		1, 1, 'r', 6, 7, // one removal
		1, 1, 'w', 8, 9, 10, // one writer epoch
		1, 1, 'x', 5, 5, 11, 12, // one want: prefix, blocks [5,5], generation
	}
	assert.Equal(t, want, fullV5Index().AppendBinary(nil))
}

func TestV5RoundTrip(t *testing.T) {
	t.Parallel()

	in := fullV5Index()
	out, err := bucketindex.Decode(in.AppendBinary(nil))
	require.NoError(t, err)
	assert.Equal(t, in, out)
}

// TestDecodeV4Compat verifies a v4 index — no block identity, no wanted list — still decodes, with
// both left unset so the entry takes part in no supersession.
func TestDecodeV4Compat(t *testing.T) {
	t.Parallel()

	got, err := bucketindex.Decode([]byte{
		'B', 'I', 4,
		1, 1, 'a', 2, 4,
		3,
		4, 5,
		1, 1, 'r', 6, 7,
		1, 1, 'w', 8, 9, 10,
	})
	require.NoError(t, err)

	require.Len(t, got.Entries, 1)
	assert.Equal(t, bucketindex.Interval{}, got.Entries[0].Blocks)
	assert.Zero(t, got.Entries[0].Level)
	assert.False(t, got.Entries[0].Blocks.Valid())
	assert.Nil(t, got.Wanted)
	assert.Len(t, got.Removed, 1)
	assert.Len(t, got.Epochs, 1)
}

// TestDecodeAllVersions verifies every format this reader claims to support still parses.
func TestDecodeAllVersions(t *testing.T) {
	t.Parallel()

	cases := map[uint8][]byte{
		1: {'B', 'I', 1, 1, 1, 'a', 2, 4},
		2: {'B', 'I', 2, 1, 1, 'a', 2, 4, 3},
		3: {'B', 'I', 3, 1, 1, 'a', 2, 4, 3, 4, 5, 0},
		4: {'B', 'I', 4, 1, 1, 'a', 2, 4, 3, 4, 5, 0, 0},
		5: {'B', 'I', 5, 1, 1, 'a', 2, 4, 0, 0, 0, 3, 4, 5, 0, 0, 0},
	}
	for ver, data := range cases {
		t.Run(fmt.Sprintf("v%d", ver), func(t *testing.T) {
			t.Parallel()

			got, err := bucketindex.Decode(data)
			require.NoError(t, err)
			require.Len(t, got.Entries, 1)
			assert.Equal(t, "a", got.Entries[0].Prefix)
			assert.EqualValues(t, 1, got.Entries[0].MinTime)
			assert.EqualValues(t, 2, got.Entries[0].MaxTime)
		})
	}

	_, err := bucketindex.Decode([]byte{'B', 'I', 6, 0})
	require.ErrorIs(t, err, bucketindex.ErrCorrupt, "a version this reader does not know is rejected")
}

func TestDecodeRejectsCorruptV5(t *testing.T) {
	t.Parallel()

	cases := map[string][]byte{
		"missing block min":  {'B', 'I', 5, 1, 1, 'a', 2, 4},
		"missing block max":  {'B', 'I', 5, 1, 1, 'a', 2, 4, 1},
		"missing level":      {'B', 'I', 5, 1, 1, 'a', 2, 4, 1, 4},
		"level overflows":    {'B', 'I', 5, 1, 1, 'a', 2, 4, 1, 4, 0x80, 0x80, 0x80, 0x80, 0x10, 0, 0, 0, 0, 0, 0},
		"missing want count": {'B', 'I', 5, 0, 0, 0, 0, 0, 0},
		"want count huge":    {'B', 'I', 5, 0, 0, 0, 0, 0, 0, 200},
		"want prefix len":    {'B', 'I', 5, 0, 0, 0, 0, 0, 0, 1, 200, 1, 1, 1, 1},
		"want blocks min":    {'B', 'I', 5, 0, 0, 0, 0, 0, 0, 1, 1, 'x'},
		"want blocks max":    {'B', 'I', 5, 0, 0, 0, 0, 0, 0, 1, 1, 'x', 5},
		"want term":          {'B', 'I', 5, 0, 0, 0, 0, 0, 0, 1, 1, 'x', 5, 5},
		"want counter":       {'B', 'I', 5, 0, 0, 0, 0, 0, 0, 1, 1, 'x', 5, 5, 1},
	}
	for name, data := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			_, err := bucketindex.Decode(data)
			require.ErrorIs(t, err, bucketindex.ErrCorrupt)
		})
	}
}

// TestEncodeDecodeIdentity is the property test: over randomly generated indexes, including empty
// and boundary intervals, decode∘encode is the identity.
func TestEncodeDecodeIdentity(t *testing.T) {
	t.Parallel()

	rnd := rand.New(rand.NewPCG(1, 2))
	for i := range 500 {
		in := randomIndex(rnd)

		out, err := bucketindex.Decode(in.AppendBinary(nil))
		require.NoErrorf(t, err, "case %d", i)
		require.Equalf(t, in, out, "case %d", i)
	}
}

func randomIndex(rnd *rand.Rand) *bucketindex.Index {
	ix := &bucketindex.Index{
		FlushedEpoch: rnd.Uint64(),
		Generation:   bucketindex.Generation{Term: rnd.Uint64(), Counter: rnd.Uint64()},
	}

	for i := range rnd.IntN(6) {
		ix.Add(bucketindex.Entry{
			Prefix:  fmt.Sprintf("part-%02d", i),
			MinTime: rnd.Int64() - math.MaxInt32,
			MaxTime: rnd.Int64(),
			Blocks:  randomInterval(rnd),
			Level:   uint32(rnd.IntN(4)),
		})
	}

	for i := range rnd.IntN(4) {
		ix.Tombstone(bucketindex.Removal{
			Prefix:     fmt.Sprintf("dead-%02d", i),
			Generation: bucketindex.Generation{Term: rnd.Uint64(), Counter: rnd.Uint64()},
		})
	}

	for i := range rnd.IntN(4) {
		ix.SetWriterEpoch(fmt.Sprintf("node-%02d", i), rnd.Uint64(),
			bucketindex.Generation{Term: rnd.Uint64(), Counter: rnd.Uint64()})
	}

	for i := range rnd.IntN(4) {
		ix.RecordWant(bucketindex.Want{
			Prefix:     fmt.Sprintf("lost-%02d", i),
			Blocks:     randomInterval(rnd),
			Generation: bucketindex.Generation{Term: rnd.Uint64(), Counter: rnd.Uint64()},
		})
	}

	return ix
}

// randomInterval covers the boundaries that matter: unset, block 1, a single block, a wide range,
// and the top of the number space.
func randomInterval(rnd *rand.Rand) bucketindex.Interval {
	switch rnd.IntN(5) {
	case 0:
		return bucketindex.Interval{}
	case 1:
		return bucketindex.Interval{Min: 1, Max: 1}
	case 2:
		n := rnd.Uint64N(1000) + 1

		return bucketindex.Interval{Min: n, Max: n}
	case 3:
		lo := rnd.Uint64N(1000) + 1

		return bucketindex.Interval{Min: lo, Max: lo + rnd.Uint64N(1000)}
	default:
		return bucketindex.Interval{Min: math.MaxUint64 - 1, Max: math.MaxUint64}
	}
}

func FuzzRoundTrip(f *testing.F) {
	f.Add(uint64(1), uint64(1), uint32(0), uint64(0), uint64(0))
	f.Add(uint64(0), uint64(0), uint32(0), uint64(1), uint64(1))
	f.Add(uint64(3), uint64(9), uint32(7), uint64(math.MaxUint64), uint64(math.MaxUint64))

	f.Fuzz(func(t *testing.T, blockMin, blockMax uint64, level uint32, wantMin, wantMax uint64) {
		in := &bucketindex.Index{
			Entries: []bucketindex.Entry{
				{Prefix: "p", MinTime: -1, MaxTime: 1, Blocks: bucketindex.Interval{Min: blockMin, Max: blockMax}, Level: level},
			},
			Wanted: []bucketindex.Want{
				{Prefix: "w", Blocks: bucketindex.Interval{Min: wantMin, Max: wantMax}},
			},
		}

		out, err := bucketindex.Decode(in.AppendBinary(nil))
		require.NoError(t, err)
		require.Equal(t, in, out)
	})
}

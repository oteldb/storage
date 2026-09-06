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

func fullIndex() *bucketindex.Index {
	return &bucketindex.Index{
		Entries: []bucketindex.Entry{
			{
				Prefix: "a", MinTime: 1, MaxTime: 2, Level: 2,
				Blocks: bucketindex.Interval{Min: 1, Max: 4, Gaps: []bucketindex.Gap{{Min: 2, Max: 3}}},
				Claim: bucketindex.Claim{
					Blocks: bucketindex.Interval{Min: 6, Max: 7},
					Group:  bucketindex.Interval{Min: 1, Max: 4},
				},
			},
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
				Level:      1,
				MinTime:    13,
				MaxTime:    14,
				Generation: bucketindex.Generation{Term: 11, Counter: 12},
			},
		},
		LostParts:       15,
		AllocatedBlocks: 16,
	}
}

// TestGoldenV6 pins the v6 byte layout: an accidental reordering or a dropped field breaks here
// before it breaks a deployment.
func TestGoldenV6(t *testing.T) {
	t.Parallel()

	want := []byte{
		'B', 'I', 6,
		// one entry: prefix, zigzag times, blocks {1} ∪ {4} as bounds 1..4 with one gap 2..3,
		// level 2, flags, a claim on blocks [6,7] held by group [1,4].
		1, 1, 'a', 2, 4, 1, 4, 1, 2, 3, 2, 0, 1, 6, 7, 0, 1, 4, 0,
		3,    // flushed epoch
		4, 5, // generation
		1, 1, 'r', 6, 7, // one removal
		1, 1, 'w', 8, 9, 10, // one writer epoch
		1, 1, 'x', 5, 5, 0, 1, 26, 28, 11, 12, 0, // one want: prefix, blocks, level, times, gen, no claim
		15, // lost parts
		16, // allocated blocks
	}
	assert.Equal(t, want, fullIndex().AppendBinary(nil))
}

func TestV6RoundTrip(t *testing.T) {
	t.Parallel()

	in := fullIndex()
	out, err := bucketindex.Decode(in.AppendBinary(nil))
	require.NoError(t, err)
	assert.Equal(t, in, out)
}

// TestDecodeV5Golden pins the migration: the v5 bytes this reader must keep parsing, and what a v5
// hull becomes. The hull is read as the contiguous set of its bounds — the meaning it had when it
// was written — the claim is unset, and the absent high-water mark leaves [Index.NextBlock] running
// off the live set exactly as v5 did.
func TestDecodeV5Golden(t *testing.T) {
	t.Parallel()

	got, err := bucketindex.Decode([]byte{
		'B', 'I', 5,
		1, 1, 'a', 2, 4, 1, 4, 2, 0,
		3,
		4, 5,
		1, 1, 'r', 6, 7,
		1, 1, 'w', 8, 9, 10,
		1, 1, 'x', 5, 5, 1, 26, 28, 11, 12,
		15,
	})
	require.NoError(t, err)

	assert.Equal(t, bucketindex.Interval{Min: 1, Max: 4}, got.Entries[0].Blocks)
	assert.Equal(t, bucketindex.Claim{}, got.Entries[0].Claim)
	assert.Zero(t, got.AllocatedBlocks)
	assert.EqualValues(t, 15, got.LostParts)
	assert.EqualValues(t, 6, got.NextBlock(), "numbering continues above what a v5 index still names")
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
		5: {'B', 'I', 5, 1, 1, 'a', 2, 4, 0, 0, 0, 0, 3, 4, 5, 0, 0, 0, 0},
		6: {'B', 'I', 6, 1, 1, 'a', 2, 4, 0, 0, 0, 0, 3, 4, 5, 0, 0, 0, 0, 0},
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

	_, err := bucketindex.Decode([]byte{'B', 'I', 7, 0})
	require.ErrorIs(t, err, bucketindex.ErrCorrupt, "a version this reader does not know is rejected")
}

// TestDecodeRejectsCorruptV6 covers the fields v6 added: a gap list that does not describe a
// canonical set is rejected rather than normalized, or encode∘decode would stop being the identity.
func TestDecodeRejectsCorruptV6(t *testing.T) {
	t.Parallel()

	cases := map[string][]byte{
		"missing gap count":  {'B', 'I', 6, 1, 1, 'a', 2, 4, 1, 4},
		"gap count huge":     {'B', 'I', 6, 1, 1, 'a', 2, 4, 1, 4, 200},
		"gap outside bounds": {'B', 'I', 6, 1, 1, 'a', 2, 4, 1, 4, 1, 5, 6, 0, 0, 0, 3, 4, 5, 0, 0, 0, 0, 0},
		"gap inverted":       {'B', 'I', 6, 1, 1, 'a', 2, 4, 1, 4, 1, 3, 2, 0, 0, 0, 3, 4, 5, 0, 0, 0, 0, 0},
		"gaps unordered":     {'B', 'I', 6, 1, 1, 'a', 2, 9, 1, 4, 2, 5, 6, 2, 3, 0, 0, 0, 3, 4, 5, 0, 0, 0, 0, 0},
		"missing claim":      {'B', 'I', 6, 1, 1, 'a', 2, 4, 1, 4, 0, 0, 0},
		"claim not a flag":   {'B', 'I', 6, 1, 1, 'a', 2, 4, 1, 4, 0, 0, 0, 2},
		"claim group unset":  {'B', 'I', 6, 1, 1, 'a', 2, 4, 1, 4, 0, 0, 0, 1, 6, 7, 0, 0},
		"missing allocated":  {'B', 'I', 6, 0, 0, 0, 0, 0, 0, 0},
	}
	for name, data := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			_, err := bucketindex.Decode(data)
			require.ErrorIs(t, err, bucketindex.ErrCorrupt)
		})
	}
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
		FlushedEpoch:    rnd.Uint64(),
		Generation:      bucketindex.Generation{Term: rnd.Uint64(), Counter: rnd.Uint64()},
		AllocatedBlocks: rnd.Uint64(),
	}

	for i := range rnd.IntN(6) {
		ix.Add(bucketindex.Entry{
			Prefix:  fmt.Sprintf("part-%02d", i),
			MinTime: rnd.Int64() - math.MaxInt32,
			MaxTime: rnd.Int64(),
			Blocks:  randomInterval(rnd),
			Claim:   randomClaim(rnd),
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
			Claim:      randomClaim(rnd),
			Generation: bucketindex.Generation{Term: rnd.Uint64(), Counter: rnd.Uint64()},
		})
	}

	return ix
}

func randomClaim(rnd *rand.Rand) bucketindex.Claim {
	if rnd.IntN(3) != 0 {
		return bucketindex.Claim{}
	}

	c := bucketindex.Claim{Blocks: randomInterval(rnd), Group: randomInterval(rnd)}
	if !c.Valid() {
		return bucketindex.Claim{}
	}

	return c
}

// randomInterval covers the boundaries that matter: unset, block 1, a single block, a wide range,
// a gapped set, and the top of the number space.
func randomInterval(rnd *rand.Rand) bucketindex.Interval {
	switch rnd.IntN(6) {
	case 5:
		nums := make([]uint64, 0, 8)
		for range rnd.IntN(7) + 1 {
			nums = append(nums, rnd.Uint64N(40)+1)
		}

		return bucketindex.Blocks(nums...)
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

// FuzzRoundTrip drives encode∘decode over arbitrary block sets: the entry's own set is built from
// the fuzzer's bytes so gapped, inverted and out-of-range shapes all reach the encoder.
func FuzzRoundTrip(f *testing.F) {
	f.Add([]byte{1}, uint32(0), uint64(0), uint64(0), uint64(0))
	f.Add([]byte{}, uint32(0), uint64(1), uint64(1), uint64(0))
	f.Add([]byte{3, 4, 9, 200}, uint32(7), uint64(math.MaxUint64), uint64(math.MaxUint64), uint64(9))

	f.Fuzz(func(t *testing.T, blocks []byte, level uint32, wantMin, wantMax, allocated uint64) {
		nums := make([]uint64, 0, len(blocks))
		for _, b := range blocks {
			nums = append(nums, uint64(b))
		}

		// An interval that names no real set of blocks is written as unset (see
		// TestInvalidIntervalEncodesAsUnset), so the identity is stated over canonical input.
		wanted := bucketindex.Interval{Min: wantMin, Max: wantMax}
		if !wanted.Valid() {
			wanted = bucketindex.Interval{}
		}

		in := &bucketindex.Index{
			Entries: []bucketindex.Entry{
				{Prefix: "p", MinTime: -1, MaxTime: 1, Blocks: bucketindex.Blocks(nums...), Level: level},
			},
			Wanted:          []bucketindex.Want{{Prefix: "w", Blocks: wanted}},
			AllocatedBlocks: allocated,
		}

		out, err := bucketindex.Decode(in.AppendBinary(nil))
		require.NoError(t, err)
		require.Equal(t, in, out)
	})
}

// TestInvalidIntervalEncodesAsUnset pins how a block set that names nothing real — a zero bound, an
// inversion, a denormalized gap list — is persisted. It is written as unset rather than verbatim,
// which is the safe direction: an unset interval takes part in no containment, so it can neither
// claim a part's rows nor be claimed, while a bogus range persisted as-is could do both.
func TestInvalidIntervalEncodesAsUnset(t *testing.T) {
	t.Parallel()

	cases := map[string]bucketindex.Interval{
		"names block zero": {Min: 0, Max: 1},
		"inverted":         {Min: 9, Max: 2},
		"gap outside":      {Min: 1, Max: 4, Gaps: []bucketindex.Gap{{Min: 7, Max: 8}}},
		"gaps unordered":   {Min: 1, Max: 9, Gaps: []bucketindex.Gap{{Min: 6, Max: 7}, {Min: 3, Max: 4}}},
	}
	for name, iv := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			in := &bucketindex.Index{Entries: []bucketindex.Entry{{Prefix: "p", Blocks: iv}}}

			out, err := bucketindex.Decode(in.AppendBinary(nil))
			require.NoError(t, err)
			assert.Equal(t, bucketindex.Interval{}, out.Entries[0].Blocks)
		})
	}
}

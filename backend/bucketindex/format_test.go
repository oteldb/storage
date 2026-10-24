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
				Prefix: "a", MinTime: 1, MaxTime: 2, Level: 2, Term: 3,
				Blocks: bucketindex.TermBlocks(1, 1, 4).Union(bucketindex.TermBlocks(2, 1)),
				Claim: bucketindex.Claim{
					Blocks: bucketindex.Range(1, 6, 7),
					Group:  bucketindex.Range(2, 1, 4),
				},
				Rollup: &bucketindex.Rollup{Tiers: []bucketindex.RollupTier{{Before: 5, Interval: 3, Agg: 2}}},
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
				Blocks:     bucketindex.Range(2, 5, 5),
				Level:      1,
				Term:       2,
				MinTime:    13,
				MaxTime:    14,
				Generation: bucketindex.Generation{Term: 11, Counter: 12},
			},
		},
		LostParts:       15,
		AllocatedBlocks: bucketindex.Block{Term: 2, N: 16},
		Catalog: []bucketindex.Claim{
			{Blocks: bucketindex.Range(1, 6, 7), Group: bucketindex.Range(2, 1, 4)},
		},
	}
}

// TestGoldenV8 pins the v8 byte layout: an accidental reordering or a dropped field breaks here
// before it breaks a deployment.
func TestGoldenV8(t *testing.T) {
	t.Parallel()

	want := []byte{
		'B', 'I', 8,
		// one entry: prefix, zigzag times,
		1, 1, 'a', 2, 4,
		// blocks {1:1, 1:4, 2:1}: lowest number 1, its term 1, top term +1, top number 1, and two
		// gaps, 1:2..1:3 and 1:5..2:0 — the rest of term 1's number space,
		1, 1, 1, 1, 2, 1, 2, 1, 3, 1, 5, 2, 0,
		// level 2, flags (a layout follows), a claim on term 1's [6,7] held by term 2's group [1,4],
		// the writer's term, and a layout of one tier: zigzag before 5, zigzag interval 3, agg 2,
		2, 2, 1, 6, 1, 0, 7, 0, 1, 2, 0, 4, 0, 3, 1, 10, 6, 2,
		3,    // flushed epoch
		4, 5, // generation
		1, 1, 'r', 6, 7, // one removal
		1, 1, 'w', 8, 9, 10, // one writer epoch
		// one want: prefix, blocks {2:5}, level, times, gen, no claim, the writer's term
		1, 1, 'x', 5, 2, 0, 5, 0, 1, 26, 28, 11, 12, 0, 2,
		15,    // lost parts
		2, 16, // allocated blocks: term, number
		1, 6, 1, 0, 7, 0, 1, 2, 0, 4, 0, // the catalog: one claim, its blocks then its group
	}
	assert.Equal(t, want, fullIndex().AppendBinary(nil))
}

func TestV8RoundTrip(t *testing.T) {
	t.Parallel()

	in := fullIndex()
	out, err := bucketindex.Decode(in.AppendBinary(nil))
	require.NoError(t, err)
	assert.Equal(t, in, out)
}

// TestDecodeV7Golden pins the migration from v7: no entry's layout is known and the catalog is
// empty, until the first v8 commit fills both in.
func TestDecodeV7Golden(t *testing.T) {
	t.Parallel()

	got, err := bucketindex.Decode([]byte{
		'B', 'I', 7,
		1, 1, 'a', 2, 4,
		1, 1, 1, 1, 2, 1, 2, 1, 3, 1, 5, 2, 0,
		2, 0, 1, 6, 1, 0, 7, 0, 1, 2, 0, 4, 0, 3,
		3,
		4, 5,
		1, 1, 'r', 6, 7,
		1, 1, 'w', 8, 9, 10,
		1, 1, 'x', 5, 2, 0, 5, 0, 1, 26, 28, 11, 12, 0, 2,
		15,
		2, 16,
	})
	require.NoError(t, err)

	want := fullIndex()
	want.Entries[0].Rollup = nil
	want.Catalog = nil
	assert.Equal(t, want, got)

	got.RecordLineage()
	assert.Equal(t, fullIndex().Catalog, got.Catalog, "the first v8 commit records the claims the entries carry")
}

// TestDecodeV6Golden pins the migration from v6: every block is term 0, and so are the writers'
// terms and the high-water mark.
func TestDecodeV6Golden(t *testing.T) {
	t.Parallel()

	got, err := bucketindex.Decode([]byte{
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
	})
	require.NoError(t, err)

	want := fullIndex()
	want.Entries[0].Rollup, want.Catalog = nil, nil
	want.Entries[0].Term, want.Wanted[0].Term = 0, 0
	want.Entries[0].Blocks = bucketindex.Blocks(1, 4)
	want.Entries[0].Claim = bucketindex.Claim{Blocks: bucketindex.Range(0, 6, 7), Group: bucketindex.Range(0, 1, 4)}
	want.Wanted[0].Blocks = bucketindex.Blocks(5)
	want.AllocatedBlocks = bucketindex.Block{N: 16}
	assert.Equal(t, want, got)
	assert.Equal(t, bucketindex.Block{N: 17}, got.NextBlock(0), "a writer with no cluster continues term 0")
	assert.Equal(t, bucketindex.Block{Term: 9, N: 1}, got.NextBlock(9), "a tenure starts its own sequence")
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

	assert.Equal(t, bucketindex.Range(0, 1, 4), got.Entries[0].Blocks)
	assert.Equal(t, bucketindex.Claim{}, got.Entries[0].Claim)
	assert.Zero(t, got.AllocatedBlocks)
	assert.EqualValues(t, 15, got.LostParts)
	assert.Equal(t, bucketindex.Block{N: 6}, got.NextBlock(0), "numbering continues above what a v5 index still names")

	inverted, err := bucketindex.Decode([]byte{'B', 'I', 5, 1, 1, 'a', 2, 4, 9, 2, 0, 0, 0, 0, 0, 0, 0, 0, 0})
	require.NoError(t, err)
	assert.Equal(t, bucketindex.Interval{}, inverted.Entries[0].Blocks, "a hull naming nothing reads as unset")
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
		7: {'B', 'I', 7, 1, 1, 'a', 2, 4, 0, 0, 0, 0, 0, 3, 4, 5, 0, 0, 0, 0, 0, 0},
		8: {'B', 'I', 8, 1, 1, 'a', 2, 4, 0, 0, 0, 0, 0, 3, 4, 5, 0, 0, 0, 0, 0, 0, 0},
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

	_, err := bucketindex.Decode([]byte{'B', 'I', 9, 0})
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

// TestDecodeRejectsCorruptV8 covers the fields v8 added: the entry's layout behind its flag bit, and
// the lineage catalog, which has to be in its one canonical order.
func TestDecodeRejectsCorruptV8(t *testing.T) {
	t.Parallel()

	head := []byte{'B', 'I', 8, 1, 1, 'a', 2, 4, 0, 0}
	tail := []byte{3, 4, 5, 0, 0, 0, 0, 0, 0, 0}
	entry := func(flags byte, rest ...byte) []byte {
		out := append(append([]byte(nil), head...), flags, 0, 0)

		return append(append(out, rest...), tail...)
	}
	claim := func(lo, group byte) []byte { return []byte{lo, 0, 0, lo, 0, group, 0, 0, group, 0} }

	cases := map[string][]byte{
		"unknown flag":        entry(4),
		"missing layout":      {'B', 'I', 8, 1, 1, 'a', 2, 4, 0, 0, 2, 0, 0},
		"layout count huge":   entry(2, 200),
		"missing tier":        {'B', 'I', 8, 1, 1, 'a', 2, 4, 0, 0, 2, 0, 0, 1, 2},
		"agg overflows":       entry(2, 1, 2, 2, 0x80, 0x02),
		"missing catalog":     {'B', 'I', 8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
		"catalog count huge":  {'B', 'I', 8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 200},
		"catalog claim unset": append([]byte{'B', 'I', 8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1}, 0, 0),
		"catalog unsorted": append(append([]byte{'B', 'I', 8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2}, claim(1, 9)...),
			claim(1, 5)...),
		"catalog duplicate": append(append([]byte{'B', 'I', 8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2}, claim(1, 5)...),
			claim(1, 5)...),
	}
	for name, data := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			_, err := bucketindex.Decode(data)
			require.ErrorIs(t, err, bucketindex.ErrCorrupt)
		})
	}

	ok := append([]byte{'B', 'I', 8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2}, claim(1, 5)...)
	ok = append(ok, claim(1, 9)...)
	ix, err := bucketindex.Decode(ok)
	require.NoError(t, err, "the sorted form of the same catalog decodes")
	assert.Len(t, ix.Catalog, 2)

	layout, err := bucketindex.Decode(entry(2, 0))
	require.NoError(t, err)
	assert.Equal(t, &bucketindex.Rollup{}, layout.Entries[0].Rollup, "a known raw layout is not unknown")
}

// TestDecodeRejectsCorruptV7 covers the fields v7 added: the block terms, the writers' terms and
// the high-water mark's term.
func TestDecodeRejectsCorruptV7(t *testing.T) {
	t.Parallel()

	cases := map[string][]byte{
		"missing min term":    {'B', 'I', 7, 1, 1, 'a', 2, 4, 1},
		"missing term span":   {'B', 'I', 7, 1, 1, 'a', 2, 4, 1, 3},
		"term span overflows": {'B', 'I', 7, 1, 1, 'a', 2, 4, 1, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x01, 1, 1, 0},
		"run across terms":    {'B', 'I', 7, 1, 1, 'a', 2, 4, 5, 1, 1, 3, 0, 0, 0, 0, 0, 3, 4, 5, 0, 0, 0, 0, 0, 0},
		"missing gap block":   {'B', 'I', 7, 1, 1, 'a', 2, 4, 1, 1, 0, 4, 1, 1},
		"missing entry term":  {'B', 'I', 7, 1, 1, 'a', 2, 4, 0, 0, 0, 0},
		"missing want term":   {'B', 'I', 7, 0, 0, 0, 0, 0, 0, 1, 1, 'x', 0, 0, 0, 0, 0, 0, 0},
		"missing mark term":   {'B', 'I', 7, 0, 0, 0, 0, 0, 0, 0, 0},
		"missing mark number": {'B', 'I', 7, 0, 0, 0, 0, 0, 0, 0, 0, 5},
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
		AllocatedBlocks: bucketindex.Block{Term: rnd.Uint64(), N: rnd.Uint64()},
	}

	for i := range rnd.IntN(6) {
		ix.Add(bucketindex.Entry{
			Prefix:  fmt.Sprintf("part-%02d", i),
			MinTime: rnd.Int64() - math.MaxInt32,
			MaxTime: rnd.Int64(),
			Blocks:  randomInterval(rnd),
			Claim:   randomClaim(rnd),
			Level:   uint32(rnd.IntN(4)),
			Term:    rnd.Uint64N(5),
			Rollup:  randomRollup(rnd),
		})
	}

	for range rnd.IntN(4) {
		if c := randomClaim(rnd); c.Valid() {
			ix.Catalog = bucketindex.MergeLineage(ix.Catalog, []bucketindex.Claim{c})
		}
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
			Term:       rnd.Uint64(),
			Generation: bucketindex.Generation{Term: rnd.Uint64(), Counter: rnd.Uint64()},
		})
	}

	return ix
}

// randomRollup covers the three states a layout has: unknown, raw, and tiered.
func randomRollup(rnd *rand.Rand) *bucketindex.Rollup {
	switch rnd.IntN(3) {
	case 0:
		return nil
	case 1:
		return &bucketindex.Rollup{}
	default:
		r := &bucketindex.Rollup{}
		for range rnd.IntN(3) + 1 {
			r.Tiers = append(r.Tiers, bucketindex.RollupTier{
				Before: rnd.Int64() - math.MaxInt32, Interval: rnd.Int64N(1 << 40), Agg: uint8(rnd.UintN(256)),
			})
		}

		return r
	}
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
// a gapped set, one spanning several terms, and the top of the number and term spaces.
func randomInterval(rnd *rand.Rand) bucketindex.Interval {
	switch rnd.IntN(8) {
	case 5:
		nums := make([]uint64, 0, 8)
		for range rnd.IntN(7) + 1 {
			nums = append(nums, rnd.Uint64N(40)+1)
		}

		return bucketindex.TermBlocks(rnd.Uint64N(3), nums...)
	case 6:
		var iv bucketindex.Interval
		for range rnd.IntN(4) + 1 {
			iv = iv.Union(bucketindex.TermBlocks(rnd.Uint64(), rnd.Uint64N(40)+1))
		}

		return iv
	case 7:
		return bucketindex.Range(math.MaxUint64, math.MaxUint64-1, math.MaxUint64)
	case 0:
		return bucketindex.Interval{}
	case 1:
		return bucketindex.Range(0, 1, 1)
	case 2:
		n := rnd.Uint64N(1000) + 1

		return bucketindex.Range(rnd.Uint64(), n, n)
	case 3:
		lo := rnd.Uint64N(1000) + 1

		return bucketindex.Range(0, lo, lo+rnd.Uint64N(1000))
	default:
		return bucketindex.Range(0, math.MaxUint64-1, math.MaxUint64)
	}
}

// FuzzRoundTrip drives encode∘decode over arbitrary block sets: the entry's own set is built from
// the fuzzer's bytes so gapped, inverted, out-of-range and multi-term shapes all reach the encoder.
// Each byte is a block: its low 5 bits the number, its high 3 bits added to term.
func FuzzRoundTrip(f *testing.F) {
	f.Add([]byte{1}, uint32(0), uint64(0), uint64(0), uint64(0), uint64(0))
	f.Add([]byte{}, uint32(0), uint64(1), uint64(1), uint64(0), uint64(0))
	f.Add([]byte{3, 4, 9, 200}, uint32(7), uint64(math.MaxUint64), uint64(math.MaxUint64), uint64(9), uint64(0))
	f.Add([]byte{0x21, 0x22, 0x41, 0xff}, uint32(1), uint64(5), uint64(5), uint64(3), uint64(math.MaxUint64-8))

	f.Fuzz(func(t *testing.T, blocks []byte, level uint32, wantMin, wantMax, allocated, term uint64) {
		var own bucketindex.Interval
		for _, b := range blocks {
			own = own.Union(bucketindex.TermBlocks(term+uint64(b>>5), uint64(b&0x1f)))
		}

		// An interval that names no real set of blocks is written as unset (see
		// TestInvalidIntervalEncodesAsUnset), so the identity is stated over canonical input.
		wanted := bucketindex.Range(term, wantMin, wantMax)
		if !wanted.Valid() {
			wanted = bucketindex.Interval{}
		}

		// The same bytes as a layout, and as the claims of a catalog: ordered and deduplicated as
		// Decode requires, the canonical form a writer produces.
		var rollup *bucketindex.Rollup
		if level%3 != 0 {
			rollup = &bucketindex.Rollup{}
			for i := 0; i+1 < len(blocks) && level%3 == 2; i += 2 {
				rollup.Tiers = append(rollup.Tiers, bucketindex.RollupTier{
					Before: int64(blocks[i]) - 128, Interval: int64(blocks[i+1]), Agg: blocks[i] ^ blocks[i+1],
				})
			}
		}

		var catalog []bucketindex.Claim
		for _, b := range blocks {
			catalog = bucketindex.MergeLineage(catalog, []bucketindex.Claim{{
				Blocks: bucketindex.TermBlocks(term, uint64(b)+1),
				Group:  bucketindex.TermBlocks(term+1, uint64(b)+1),
			}})
		}

		in := &bucketindex.Index{
			Entries: []bucketindex.Entry{
				{Prefix: "p", MinTime: -1, MaxTime: 1, Blocks: own, Level: level, Term: term, Rollup: rollup},
			},
			Wanted:          []bucketindex.Want{{Prefix: "w", Blocks: wanted, Term: allocated}},
			AllocatedBlocks: bucketindex.Block{Term: term, N: allocated},
			Catalog:         catalog,
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
		"names block zero": bucketindex.Range(0, 0, 1),
		"inverted":         bucketindex.Range(0, 9, 2),
		"run across terms": {Min: bucketindex.Block{Term: 1, N: 1}, Max: bucketindex.Block{Term: 2, N: 1}},
		"gap outside": {
			Min: bucketindex.Block{N: 1}, Max: bucketindex.Block{N: 4},
			Gaps: []bucketindex.Gap{{Min: bucketindex.Block{N: 7}, Max: bucketindex.Block{N: 8}}},
		},
		"gaps unordered": {
			Min: bucketindex.Block{N: 1}, Max: bucketindex.Block{N: 9},
			Gaps: []bucketindex.Gap{
				{Min: bucketindex.Block{N: 6}, Max: bucketindex.Block{N: 7}},
				{Min: bucketindex.Block{N: 3}, Max: bucketindex.Block{N: 4}},
			},
		},
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

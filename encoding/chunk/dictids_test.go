package chunk

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// materializeDict expands the split (entries, ids) form to the row-per-cell form the value-hashing
// encoders take.
func materializeDict(entries [][]byte, ids []int32) [][]byte {
	vals := make([][]byte, len(ids))
	for i, id := range ids {
		vals[i] = entries[id]
	}

	return vals
}

// dedupEntries returns entries with value-duplicates removed, plus a mapping from the original
// index to the surviving one — the precondition [EncodeBytesDict] places on its caller.
func dedupEntries(entries [][]byte) (out [][]byte, remap []int32) {
	seen := make(map[string]int32, len(entries))
	remap = make([]int32, len(entries))

	for i, e := range entries {
		id, ok := seen[string(e)]
		if !ok {
			id = int32(len(out))
			seen[string(e)] = id
			out = append(out, e)
		}

		remap[i] = id
	}

	return out, remap
}

type dictIDCase struct {
	name    string
	entries [][]byte
	ids     []int32
	lo, hi  int
}

// distinctEntries builds n distinct values.
func distinctEntries(n int) [][]byte {
	entries := make([][]byte, n)
	for i := range n {
		entries[i] = []byte("v-" + itoa(i))
	}

	return entries
}

// identityIDs builds ids 0..n-1.
func identityIDs(n int) []int32 {
	ids := make([]int32, n)
	for i := range n {
		ids[i] = int32(i)
	}

	return ids
}

// cyclicIDs builds n row ids cycling over card entries, so the dictionary fills in the natural
// 0,1,…,card-1 order.
func cyclicIDs(n, card int) []int32 {
	ids := make([]int32, n)
	for i := range n {
		ids[i] = int32(i % card)
	}

	return ids
}

func dictIDCases() []dictIDCase {
	return []dictIDCase{
		{name: "empty", entries: nil, ids: nil, lo: 0, hi: 0},
		{name: "single row", entries: [][]byte{[]byte("a")}, ids: []int32{0}, lo: 0, hi: 1},
		{
			name:    "single entry many rows",
			entries: [][]byte{[]byte("service-a")},
			ids:     make([]int32, 1000),
			lo:      10,
			hi:      900,
		},
		{
			name:    "unused entries",
			entries: distinctEntries(64),
			ids:     []int32{63, 7, 63, 0, 7},
			lo:      1,
			hi:      4,
		},
		{
			name:    "reverse first occurrence",
			entries: distinctEntries(8),
			ids:     []int32{7, 6, 5, 4, 3, 2, 1, 0, 7, 0},
			lo:      3,
			hi:      9,
		},
		{
			name:    "256 distinct",
			entries: distinctEntries(256),
			ids:     cyclicIDs(1024, 256),
			lo:      0,
			hi:      1024,
		},
		{
			name:    "257 distinct",
			entries: distinctEntries(257),
			ids:     cyclicIDs(1028, 257),
			lo:      0,
			hi:      1028,
		},
		{
			name:    "2-byte ids strict subrange",
			entries: distinctEntries(300),
			ids:     cyclicIDs(3000, 300),
			lo:      7,
			hi:      2999,
		},
		{
			name:    "empty cells",
			entries: [][]byte{{}, []byte("a"), []byte("bb")},
			ids:     []int32{0, 1, 0, 2, 0, 0, 1},
			lo:      2,
			hi:      6,
		},
		{
			name:    "lo equals hi",
			entries: distinctEntries(4),
			ids:     []int32{0, 1, 2, 3},
			lo:      2,
			hi:      2,
		},
		{
			name:    "range to end",
			entries: distinctEntries(4),
			ids:     []int32{3, 3, 1, 0, 2},
			lo:      1,
			hi:      5,
		},
	}
}

// TestEncodeBytesDictMatchesEncodeBytes pins the split-form encode to the value-hashing one: both
// write the on-disk bytes stream, so their output must be byte-identical for every shape.
func TestEncodeBytesDictMatchesEncodeBytes(t *testing.T) {
	t.Parallel()

	for _, tc := range dictIDCases() {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			cells := materializeDict(tc.entries, tc.ids)
			require.Equal(t, EncodeBytes(nil, cells), EncodeBytesDict(nil, tc.entries, tc.ids))

			sub := materializeDict(tc.entries, tc.ids[tc.lo:tc.hi])
			require.Equal(t,
				EncodeBytes(nil, sub),
				EncodeBytesDictRange(nil, tc.entries, tc.ids, tc.lo, tc.hi),
			)
		})
	}
}

// TestEncodeBytesDictRoundTrip checks the emitted stream decodes back to the materialized rows.
func TestEncodeBytesDictRoundTrip(t *testing.T) {
	t.Parallel()

	for _, tc := range dictIDCases() {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			enc := EncodeBytesDict(nil, tc.entries, tc.ids)

			got, consumed, err := DecodeBytes(nil, enc)
			require.NoError(t, err)
			assert.Equal(t, len(enc), consumed)

			want := materializeDict(tc.entries, tc.ids)
			require.Len(t, got, len(want))

			for i := range want {
				// A decoded empty cell is a zero-length view, which may be nil: compare by value.
				assert.Equal(t, string(want[i]), string(got[i]), "row %d", i)
			}
		})
	}
}

// TestEncodeBytesDictAppends checks the encoders append to a non-empty dst rather than overwriting.
func TestEncodeBytesDictAppends(t *testing.T) {
	t.Parallel()

	entries := distinctEntries(4)
	ids := []int32{3, 0, 1, 1, 2}
	prefix := []byte("prefix")

	got := EncodeBytesDict(append([]byte(nil), prefix...), entries, ids)
	assert.Equal(t, prefix, got[:len(prefix)])
	assert.Equal(t, EncodeBytesDict(nil, entries, ids), got[len(prefix):])
}

// TestEncodeBytesDictFlatFallback pins the >65536-distinct crossover: the split-form encode must
// abandon the dictionary on the same row as [EncodeBytes] and emit the identical flat stream.
func TestEncodeBytesDictFlatFallback(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name     string
		distinct int
	}{
		{name: "at limit stays dict", distinct: maxDictEntries},
		{name: "over limit goes flat", distinct: maxDictEntries + 1},
		{name: "well over limit", distinct: maxDictEntries + 512},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			entries := distinctEntries(tc.distinct)
			ids := identityIDs(tc.distinct)

			cells := materializeDict(entries, ids)
			require.Equal(t, EncodeBytes(nil, cells), EncodeBytesDict(nil, entries, ids))

			// The dictionary is abandoned mid-scan, so the rows after the crossover must still be
			// written: check the truncated range that stops just short of it stays dictionary-encoded.
			lo, hi := 1, maxDictEntries
			require.Equal(t,
				EncodeBytes(nil, cells[lo:hi]),
				EncodeBytesDictRange(nil, entries, ids, lo, hi),
			)
		})
	}
}

// TestEncodeBytesDictScratchReuse exercises the pooled remap across calls with different entry-table
// sizes, where a stale generation stamp would leak ids from a prior call.
func TestEncodeBytesDictScratchReuse(t *testing.T) {
	t.Parallel()

	for _, card := range []int{300, 4, 1000, 1, 257} {
		entries := distinctEntries(card)
		ids := cyclicIDs(card*3, card)

		require.Equal(t,
			EncodeBytes(nil, materializeDict(entries, ids)),
			EncodeBytesDict(nil, entries, ids),
			"cardinality %d", card,
		)
	}
}

// TestDictRemapScratchGenerationWrap covers the one path the encode tests cannot reach: the
// generation counter wrapping. A stamp left in the backing array past the armed prefix outlives a
// clear of the prefix alone, and matches whichever generation later reaches its value — a hit on an
// entry this call never assigned, which emits a wrong dictionary id and corrupts the column.
func TestDictRemapScratchGenerationWrap(t *testing.T) {
	t.Parallel()

	s := &dictRemapScratch{out: make([]int32, 4), stamp: make([]uint32, 4)}

	// Entry 3 is stamped at generation 2 by a wide call, then a narrow one leaves it outside the
	// armed prefix — so a clear of the prefix alone does not reach it.
	s.arm(4)
	s.arm(4)
	s.set(3, 9)
	s.arm(2)

	s.gen = ^uint32(0)
	s.arm(2) // wraps: generation restarts at 1, and every stale stamp must go with it

	s.arm(4) // generation 2 again, now addressing the stale stamp
	_, seen := s.get(3)
	assert.False(t, seen, "a stamp from before the wrap must not match the restarted generation")

	// The restarted generation must still work as a generation.
	s.set(1, 7)
	out, seen := s.get(1)
	assert.True(t, seen)
	assert.Equal(t, int32(7), out)
}

func BenchmarkDictEncodeFromIDs(b *testing.B) {
	entries := distinctEntries(300)
	ids := cyclicIDs(8192, 300)
	cells := materializeDict(entries, ids)
	buf := make([]byte, 0, len(EncodeBytesDict(nil, entries, ids)))

	b.SetBytes(totalStringBytes(cells)) // logical uncompressed input bytes encoded/sec
	b.ReportAllocs()
	b.ResetTimer()

	for range b.N {
		buf = EncodeBytesDict(buf[:0], entries, ids)
	}
}

func BenchmarkDictEncodeFromValues(b *testing.B) {
	entries := distinctEntries(300)
	ids := cyclicIDs(8192, 300)
	cells := materializeDict(entries, ids)
	buf := make([]byte, 0, len(EncodeBytes(nil, cells)))

	b.SetBytes(totalStringBytes(cells)) // logical uncompressed input bytes encoded/sec
	b.ReportAllocs()
	b.ResetTimer()

	for range b.N {
		buf = EncodeBytes(buf[:0], cells)
	}
}

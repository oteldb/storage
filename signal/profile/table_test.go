package profile

import (
	"bytes"
	"encoding/binary"
	"hash/crc32"
	"math/rand/v2"
	"os"
	"path/filepath"
	"testing"

	"github.com/go-faster/sdk/gold"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/encoding/compress"
	"github.com/oteldb/storage/internal/heaptest"
	"github.com/oteldb/storage/signal"
)

// fixtureTable is the content of testdata/symtable-v2-zstd.bin: stack-shaped entries over a few
// location ids, so the body compresses.
func fixtureTable() map[signal.SeriesID][]byte {
	m := make(map[signal.SeriesID][]byte, 64)

	for i := range uint64(64) {
		entry := binary.AppendUvarint(nil, 8)
		for j := range uint64(8) {
			entry = signal.SeriesID{Hi: (i + j) % 16, Lo: 0xfeed}.AppendBinary(entry)
		}

		m[signal.SeriesID{Hi: i, Lo: i * 7}] = entry
	}

	return m
}

func goldenEntry() map[signal.SeriesID][]byte {
	return map[signal.SeriesID][]byte{{Hi: 0x0102030405060708, Lo: 0x090a0b0c0d0e0f10}: []byte("frame")}
}

// randomTable draws a table whose entries share a small alphabet, so some compress and some do not.
func randomTable(r *rand.Rand) map[signal.SeriesID][]byte {
	m := make(map[signal.SeriesID][]byte)

	for range r.IntN(64) {
		n := r.IntN(200)
		entry := make([]byte, 0, n)

		for range n {
			entry = append(entry, byte('a'+r.IntN(4)))
		}

		m[signal.SeriesID{Hi: r.Uint64(), Lo: r.Uint64()}] = entry
	}

	return m
}

func tableHeader(version uint32, alg compress.Algorithm) []byte {
	out := binary.BigEndian.AppendUint32(nil, symMagic)
	out = binary.BigEndian.AppendUint32(out, version)

	return append(out, byte(alg))
}

func withCRC(b []byte) []byte {
	return binary.BigEndian.AppendUint32(b, crc32.Checksum(b, castagnoli))
}

// frameTable frames a version 2 table around block with a valid CRC, whatever block holds.
func frameTable(alg compress.Algorithm, rawLen uint64, block []byte) []byte {
	out := binary.AppendUvarint(tableHeader(symVersion, alg), rawLen)

	return withCRC(append(out, block...))
}

func readFixture(tb testing.TB, name string) []byte {
	tb.Helper()

	data, err := os.ReadFile(filepath.Join("testdata", name))
	require.NoError(tb, err)

	return data
}

func requireSameTable(tb testing.TB, want, got map[signal.SeriesID][]byte) {
	tb.Helper()

	require.Len(tb, got, len(want))

	for id, entry := range want {
		g, ok := got[id]
		require.True(tb, ok, "missing %v", id)
		require.True(tb, bytes.Equal(entry, g), "entry %v", id)
	}
}

// TestDecodeTableV1 decodes a version 1 table as parts written before compression hold it.
func TestDecodeTableV1(t *testing.T) {
	t.Parallel()

	data := readFixture(t, "symtable-v1.bin")
	require.Equal(t, symVersionRaw, binary.BigEndian.Uint32(data[4:]))

	got := map[signal.SeriesID][]byte{}
	require.NoError(t, decodeTable(got, data))
	requireSameTable(t, goldenEntry(), got)
}

// TestDecodeTableV2Fixture decodes a zstd-compressed version 2 table written by an earlier build,
// so an encoder change cannot move the fixture along with it.
func TestDecodeTableV2Fixture(t *testing.T) {
	t.Parallel()

	data := readFixture(t, "symtable-v2-zstd.bin")
	require.Equal(t, symVersion, binary.BigEndian.Uint32(data[4:]))
	require.Equal(t, byte(compress.AlgorithmZSTD), data[8])

	got := map[signal.SeriesID][]byte{}
	require.NoError(t, decodeTable(got, data))
	requireSameTable(t, fixtureTable(), got)
	assert.Less(t, len(data), len(encodeTable(got, memoryCompressor)), "the fixture is compressed")
}

// TestTableGolden pins the version 2 framing on a table too small to compress.
func TestTableGolden(t *testing.T) {
	t.Parallel()

	enc := encodeTable(goldenEntry(), storageCompressor)
	gold.Bytes(t, enc, "symtable-v2")
	assert.Equal(t, compress.FlagRaw, enc[10], "zstd falls back to a raw block")
}

// TestTableRoundTrip checks decode∘encode is the identity and encoding is deterministic, for every
// algorithm a table may carry.
func TestTableRoundTrip(t *testing.T) {
	t.Parallel()

	for _, alg := range []compress.Algorithm{compress.AlgorithmNone, compress.AlgorithmZSTD, compress.AlgorithmLZ4} {
		c := tableCompressor(alg)

		t.Run(alg.String(), func(t *testing.T) {
			t.Parallel()

			for seed := range uint64(200) {
				m := randomTable(rand.New(rand.NewPCG(seed, 0)))
				if seed == 0 {
					m[signal.SeriesID{Hi: 1}] = nil
				}

				enc := encodeTable(m, c)
				require.Equal(t, byte(alg), enc[8])

				got := map[signal.SeriesID][]byte{}
				require.NoError(t, decodeTable(got, enc), "seed %d", seed)
				requireSameTable(t, m, got)
				require.Equal(t, enc, encodeTable(got, c), "seed %d", seed)
			}
		})
	}
}

func storedSidecars(tb testing.TB, s *SymbolStore) map[string][]byte {
	tb.Helper()

	stored, err := s.Stored(s.Encode())
	require.NoError(tb, err)

	return stored
}

// TestSidecarsCompressedOnlyStored checks only [SymbolStore.Stored] compresses: the delta, Encode
// and Union — everything a resolver build touches — stay raw.
func TestSidecarsCompressedOnlyStored(t *testing.T) {
	t.Parallel()

	corpus := pprofCorpus(t)
	delta := encodeDelta(corpus)

	_, n := binary.Uvarint(delta)
	require.Positive(t, n)
	assert.Equal(t, byte(compress.AlgorithmNone), delta[n+8], "delta")

	s := NewSymbolStore()
	require.NoError(t, s.Absorb(delta))

	stored := storedSidecars(t, s)
	union, err := NewSymbolStore().Union([]map[string][]byte{s.Encode(), stored})
	require.NoError(t, err)

	for i, name := range tableNames {
		assert.Equal(t, byte(compress.AlgorithmNone), s.Encode()[name][8], "encode %s", name)
		assert.Equal(t, byte(compress.AlgorithmNone), union[name][8], "union %s", name)
		require.Equal(t, byte(compress.AlgorithmZSTD), stored[name][8], "stored %s", name)

		for _, data := range [][]byte{stored[name], union[name]} {
			got := map[signal.SeriesID][]byte{}
			require.NoError(t, decodeTable(got, data))
			requireSameTable(t, corpus.t[i], got)
		}
	}

	_, err = s.Stored(map[string][]byte{"stacks": {1, 2, 3}})
	require.ErrorIs(t, err, ErrCorruptSymbols)
}

func decodeRejects() map[string][]byte {
	zeros := storageCompressor.Compress(nil, make([]byte, 256<<10))
	body := tableBody(fixtureTable())
	bodyLen := uint64(len(body))
	block := storageCompressor.Compress(nil, body)

	badCRC := encodeTable(fixtureTable(), storageCompressor)
	badCRC[len(badCRC)-1] ^= 1

	badMagic := encodeTable(fixtureTable(), storageCompressor)
	badMagic[0] ^= 1

	return map[string][]byte{
		"bomb":                  frameTable(compress.AlgorithmZSTD, 64, zeros),
		"body shorter":          frameTable(compress.AlgorithmZSTD, bodyLen+1, block),
		"body longer":           frameTable(compress.AlgorithmZSTD, bodyLen-1, block),
		"claims a terabyte":     frameTable(compress.AlgorithmZSTD, 1<<40, zeros),
		"claims past MaxInt":    frameTable(compress.AlgorithmZSTD, 1<<63, zeros),
		"unknown algorithm":     frameTable(9, bodyLen, block),
		"compressed under none": frameTable(compress.AlgorithmNone, bodyLen, block),
		"raw past its length":   frameTable(compress.AlgorithmNone, 1, append([]byte{compress.FlagRaw}, body...)),
		"truncated body length": withCRC(append(tableHeader(symVersion, compress.AlgorithmZSTD), 0x80)),
		"no algorithm":          withCRC(tableHeader(symVersion, 0)[:8]),
		"bad version":           withCRC(append(binary.AppendUvarint(tableHeader(3, compress.AlgorithmZSTD), bodyLen), block...)),
		"bad crc":               badCRC,
		"bad magic":             withCRC(badMagic[:len(badMagic)-4]),
	}
}

// TestDecodeTableRejects checks every malformed version 2 table fails as corrupt.
func TestDecodeTableRejects(t *testing.T) {
	t.Parallel()

	for name, data := range decodeRejects() {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			require.ErrorIs(t, decodeTable(map[signal.SeriesID][]byte{}, data), ErrCorruptSymbols)
		})
	}

	require.ErrorIs(t, decodeTable(map[signal.SeriesID][]byte{}, decodeRejects()["bomb"]), compress.ErrLimit)
}

// TestDecodeTableBombAllocation checks a table allocates what it decodes to, not what its header
// claims, and nothing of a frame that inflates past the claim.
//
//nolint:paralleltest // reads process-wide allocation counters
func TestDecodeTableBombAllocation(t *testing.T) {
	rejects := decodeRejects()

	for _, tc := range []struct {
		name  string
		bound uint64
	}{
		{"bomb", compress.DecodeWorkspace},
		{"claims a terabyte", 2*256<<10 + compress.DecodeWorkspace},
	} {
		data := rejects[tc.name]

		var err error

		got := heaptest.Allocated(func() { err = decodeTable(map[signal.SeriesID][]byte{}, data) })
		require.ErrorIs(t, err, ErrCorruptSymbols, tc.name)
		assert.Less(t, got, tc.bound, tc.name)
	}
}

// FuzzDecodeTable: arbitrary bytes must error or decode to a table that round-trips.
func FuzzDecodeTable(f *testing.F) {
	f.Add(readFixture(f, "symtable-v1.bin"))
	f.Add(readFixture(f, "symtable-v2-zstd.bin"))
	f.Add(encodeTable(goldenEntry(), storageCompressor))
	f.Add([]byte{0x4f, 0x54, 0x53, 0x50})

	for _, data := range decodeRejects() {
		f.Add(data)
	}

	for seed := range uint64(4) {
		m := randomTable(rand.New(rand.NewPCG(seed, 1)))
		for _, alg := range []compress.Algorithm{compress.AlgorithmNone, compress.AlgorithmZSTD, compress.AlgorithmLZ4} {
			f.Add(encodeTable(m, tableCompressor(alg)))
		}
	}

	f.Fuzz(func(t *testing.T, data []byte) {
		_ = decodeTable(map[signal.SeriesID][]byte{}, data)

		// A mutation almost never keeps the CRC, so re-sign it to reach the parser behind.
		m := map[signal.SeriesID][]byte{}
		if len(data) < 4 || decodeTable(m, withCRC(data[:len(data)-4:len(data)-4])) != nil {
			return
		}

		got := map[signal.SeriesID][]byte{}
		if err := decodeTable(got, encodeTable(m, memoryCompressor)); err != nil {
			t.Fatalf("re-encoded table: %v", err)
		}

		if len(got) != len(m) {
			t.Fatalf("round trip: %d entries, want %d", len(got), len(m))
		}

		for id, entry := range m {
			if !bytes.Equal(got[id], entry) {
				t.Fatalf("round trip: entry %v differs", id)
			}
		}
	})
}

package profile

import (
	"bytes"
	"math/rand/v2"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/encoding/compress"
	"github.com/oteldb/storage/signal"
)

func encodeTables(s symTables, c *compress.Compressor) map[string][]byte {
	out := make(map[string][]byte, len(tableNames))
	for i, name := range tableNames {
		out[name] = encodeTable(s.t[i], c)
	}

	return out
}

func TestDecodeTables(t *testing.T) {
	t.Parallel()

	corpus := pprofCorpus(t)

	for _, alg := range []compress.Algorithm{compress.AlgorithmNone, compress.AlgorithmZSTD, compress.AlgorithmLZ4} {
		t.Run(alg.String(), func(t *testing.T) {
			t.Parallel()

			got, err := DecodeTables(encodeTables(corpus, tableCompressor(alg)))
			require.NoError(t, err)

			var entries int64

			for i := range tableNames {
				requireSameTable(t, corpus.t[i], got.t.t[i])
				entries += int64(len(corpus.t[i]))
			}

			assert.Greater(t, got.Size(), entries*entryOverhead)
		})
	}

	t.Run("V1", func(t *testing.T) {
		t.Parallel()

		got, err := DecodeTables(map[string][]byte{"stacks": readFixture(t, "symtable-v1.bin")})
		require.NoError(t, err)
		requireSameTable(t, goldenEntry(), got.t.t[tableStacks])
		assert.Empty(t, got.t.t[tableStrings], "absent tables are empty")
	})
}

// TestDecodeTablesOwnsItsBytes checks decoded entries survive the caller reusing its input.
func TestDecodeTablesOwnsItsBytes(t *testing.T) {
	t.Parallel()

	for name, data := range map[string][]byte{
		"v1":   readFixture(t, "symtable-v1.bin"),
		"none": encodeTable(goldenEntry(), memoryCompressor),
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			got, err := DecodeTables(map[string][]byte{"stacks": data})
			require.NoError(t, err)

			for i := range data {
				data[i] = 0
			}

			requireSameTable(t, goldenEntry(), got.t.t[tableStacks])
		})
	}
}

func TestDecodeTablesRejects(t *testing.T) {
	t.Parallel()

	for name, data := range decodeRejects() {
		_, err := DecodeTables(map[string][]byte{"locations": data})
		require.ErrorIs(t, err, ErrCorruptSymbols, name)
	}

	countPastBody := withCRC(append(tableHeader(symVersionRaw, 0)[:8], 0x05))
	_, err := DecodeTables(map[string][]byte{"stacks": countPastBody})
	require.ErrorIs(t, err, ErrCorruptSymbols)
}

// TestDecodeTablesScratchBounded checks the decompression scratch never keeps a buffer beyond its
// cap, whether the oversized body decoded or failed, and never keeps more than its slots.
func TestDecodeTablesScratchBounded(t *testing.T) {
	t.Parallel()

	big := map[signal.SeriesID][]byte{{Hi: 1}: make([]byte, 2*maxScratchBytes)}
	body := tableBody(big)
	block := storageCompressor.Compress(nil, body)

	got, err := DecodeTables(map[string][]byte{"stacks": encodeTable(big, storageCompressor)})
	require.NoError(t, err)
	require.Len(t, got.t.t[tableStacks][signal.SeriesID{Hi: 1}], 2*maxScratchBytes)

	_, err = DecodeTables(map[string][]byte{
		"stacks": frameTable(compress.AlgorithmZSTD, uint64(len(body))+1, block),
	})
	require.ErrorIs(t, err, ErrCorruptSymbols)

	_, err = DecodeTables(map[string][]byte{"stacks": encodeTable(goldenEntry(), storageCompressor)})
	require.NoError(t, err)

	putScratch(make([]byte, 0, maxScratchBytes+1))

	var kept [][]byte

	for len(kept) <= scratchSlots {
		select {
		case b := <-bodyScratch:
			kept = append(kept, b)

			continue
		default:
		}

		break
	}

	for _, b := range kept {
		assert.LessOrEqual(t, cap(b), maxScratchBytes, "an oversized scratch is dropped")
		putScratch(b)
	}

	assert.LessOrEqual(t, len(kept), scratchSlots)
}

// TestSymbolStoreTablesSnapshot checks the snapshot does not see what the accumulator absorbs later.
func TestSymbolStoreTablesSnapshot(t *testing.T) {
	t.Parallel()

	d := &Dictionary{}
	b := newBuilder(d)
	first := b.stackID(buildStack(d, "first", "a.go"))

	s := NewSymbolStore()
	require.NoError(t, s.Absorb(encodeDelta(b.tables)))

	snap := s.Tables()

	d2 := &Dictionary{}
	b2 := newBuilder(d2)
	second := b2.stackID(buildStack(d2, "second", "b.go"))
	require.NoError(t, s.Absorb(encodeDelta(b2.tables)))

	r := NewResolverFrom(snap)
	assert.NotEmpty(t, r.Resolve(first.AppendBinary(nil)))
	assert.Empty(t, r.Resolve(second.AppendBinary(nil)), "absorbed after the snapshot")
	assert.NotEmpty(t, NewResolverFrom(s.Tables()).Resolve(second.AppendBinary(nil)))
}

// splitLayers scatters every entry of s over n layers, some entries into several.
func splitLayers(r *rand.Rand, s symTables, n int) []*Tables {
	layers := make([]*Tables, n)
	for i := range layers {
		layers[i] = &Tables{t: newSymTables()}
	}

	for ti, table := range s.t {
		for id, entry := range table {
			for range 1 + r.IntN(2) {
				layers[r.IntN(n)].t.t[ti][id] = entry
			}
		}
	}

	return layers
}

// TestResolverLayersMatchUnion checks a layered resolver answers exactly as one over the union, for
// every stack, however the entries are spread over the layers.
func TestResolverLayersMatchUnion(t *testing.T) {
	t.Parallel()

	corpus := pprofCorpus(t)

	union, err := NewResolver(encodeTables(corpus, memoryCompressor))
	require.NoError(t, err)

	ids := make([][]byte, 0, 3+len(corpus.t[tableStacks]))
	ids = append(ids, nil, []byte("short"), make([]byte, 16))

	for id := range corpus.t[tableStacks] {
		ids = append(ids, id.AppendBinary(nil))
	}

	for seed := range uint64(4) {
		r := rand.New(rand.NewPCG(seed, 2))
		layers := splitLayers(r, corpus, 1+r.IntN(6))
		layered := NewResolverFrom(append(layers, nil)...)

		for _, id := range ids {
			require.Equal(t, union.Resolve(id), layered.Resolve(id), "seed %d stack %x", seed, id)
		}

		got, err := DecodeTables(EncodeTables(layers))
		require.NoError(t, err)

		for i := range tableNames {
			requireSameTable(t, corpus.t[i], got.t.t[i])
		}
	}
}

func TestEncodeTablesEdges(t *testing.T) {
	t.Parallel()

	empty, err := DecodeTables(EncodeTables(nil))
	require.NoError(t, err)

	for i := range tableNames {
		assert.Empty(t, empty.t.t[i])
	}

	one := &Tables{t: newSymTables()}
	one.t.t[tableStacks] = goldenEntry()

	got, err := DecodeTables(EncodeTables([]*Tables{one}))
	require.NoError(t, err)
	requireSameTable(t, goldenEntry(), got.t.t[tableStacks])
	assert.Empty(t, NewResolverFrom().Resolve(make([]byte, 16)))
}

// FuzzDecodeTables checks the aliasing decoder accepts exactly what [decodeTable] accepts, with the
// same entries.
func FuzzDecodeTables(f *testing.F) {
	f.Add(readFixture(f, "symtable-v1.bin"))
	f.Add(readFixture(f, "symtable-v2-zstd.bin"))
	f.Add(encodeTable(fixtureTable(), memoryCompressor))

	for _, data := range decodeRejects() {
		f.Add(data)
	}

	for seed := range uint64(4) {
		m := randomTable(rand.New(rand.NewPCG(seed, 3)))
		for _, alg := range []compress.Algorithm{compress.AlgorithmNone, compress.AlgorithmZSTD, compress.AlgorithmLZ4} {
			f.Add(encodeTable(m, tableCompressor(alg)))
		}
	}

	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) >= 4 {
			data = withCRC(data[: len(data)-4 : len(data)-4])
		}

		want := map[signal.SeriesID][]byte{}
		wantErr := decodeTable(want, data)

		got, err := DecodeTables(map[string][]byte{"stacks": bytes.Clone(data)})
		if (err == nil) != (wantErr == nil) {
			t.Fatalf("DecodeTables error %v, decodeTable error %v", err, wantErr)
		}

		if err != nil {
			return
		}

		if len(got.t.t[tableStacks]) != len(want) {
			t.Fatalf("%d entries, want %d", len(got.t.t[tableStacks]), len(want))
		}

		for id, entry := range want {
			if !bytes.Equal(got.t.t[tableStacks][id], entry) {
				t.Fatalf("entry %v differs", id)
			}
		}
	})
}

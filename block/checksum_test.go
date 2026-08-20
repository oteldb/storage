package block

import (
	"context"
	"encoding/binary"
	"math/rand/v2"
	"strconv"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/encoding/chunk"
)

// checkedInts is a jittered int64 column large enough to fill several compression frames, so a
// corruption test can aim at a frame other than the first.
func checkedInts(n int) []int64 {
	vals := make([]int64, n)
	t := int64(1_700_000_000_000_000_000)

	for i := range vals {
		t += 15_000_000_000 + int64(i%97)*131_071
		vals[i] = t
	}

	return vals
}

// framedColumn builds a checked, block-framed int64 column with frames small enough that the column
// holds several, and returns its descriptor, object, values, and the object offset its frames start
// at (everything before that is the directory).
func framedColumn(tb testing.TB, n int) (ColumnDesc, []byte, []int64, int) {
	tb.Helper()

	vals := checkedInts(n)
	c := Column{Name: "ts", Kind: KindInt64, Codec: chunk.CodecDoD, Int64: vals, Block: true}

	desc, obj, err := buildColumn(c, zstdComp(), 16, 512)
	require.NoError(tb, err)
	require.True(tb, desc.Checked)
	require.True(tb, desc.Framed)

	dir, err := parseBlockDir(obj, desc)
	require.NoError(tb, err)
	require.Greater(tb, len(dir.frameOff), 3, "the column must hold several frames")

	return desc, obj, vals, len(obj) - len(dir.data)
}

// TestFramedFrameCorruptionIsDetected flips one byte of every compression frame in turn — the first
// and every later one — and requires each read path to report it rather than to decode the damaged
// bytes.
func TestFramedFrameCorruptionIsDetected(t *testing.T) {
	t.Parallel()

	desc, obj, vals, dataOff := framedColumn(t, 4096)

	dir, err := parseBlockDir(obj, desc)
	require.NoError(t, err)

	for f := range len(dir.frameOff) - 1 {
		t.Run("frame"+strconv.Itoa(f), func(t *testing.T) {
			t.Parallel()

			bad := append([]byte(nil), obj...)
			bad[dataOff+int(dir.frameOff[f])] ^= 0x01

			_, err := newColumnReader(desc, bad, zstdComp(), len(vals)).Int64(nil)
			require.ErrorIs(t, err, ErrCorrupt)
			assert.Contains(t, err.Error(), `column "ts"`)
			assert.Contains(t, err.Error(), "frame "+strconv.Itoa(f))

			blocks := make([]int, 0, dir.granules)
			for g := range dir.granules {
				blocks = append(blocks, g)
			}

			_, err = newColumnReader(desc, bad, zstdComp(), len(vals)).DecodeBlocksInt64(nil, blocks)
			require.ErrorIs(t, err, ErrCorrupt)
		})
	}
}

// TestFramedRangeReadDetectsCorruption checks the seek path specifically: a row range landing in a
// damaged frame must fail rather than return whatever the frame decompressed to.
func TestFramedRangeReadDetectsCorruption(t *testing.T) {
	t.Parallel()

	desc, obj, vals, dataOff := framedColumn(t, 4096)

	dir, err := parseBlockDir(obj, desc)
	require.NoError(t, err)

	last := len(dir.frameOff) - 2

	bad := append([]byte(nil), obj...)
	bad[dataOff+int(dir.frameOff[last])] ^= 0x80

	firstGranule := 0

	for g := range dir.granules {
		if int(dir.gFrame[g]) == last {
			firstGranule = g

			break
		}
	}

	lo := firstGranule * dir.blockRows

	_, err = newColumnReader(desc, bad, zstdComp(), len(vals)).RangeInt64(nil, lo, lo+dir.blockRows)
	require.ErrorIs(t, err, ErrCorrupt)

	// A range entirely inside the sound frames still reads, so the checksum does not turn one bad
	// frame into a dead column.
	got, err := newColumnReader(desc, bad, zstdComp(), len(vals)).RangeInt64(nil, 0, dir.blockRows)
	require.NoError(t, err)
	assert.Equal(t, vals[:dir.blockRows], got)
}

// TestFramedDirectoryCorruptionIsDetected flips every byte of the directory in turn. The directory
// carries no data, so nothing downstream would necessarily notice a wrong granule length or a wrong
// blockRows — its own checksum is what does.
func TestFramedDirectoryCorruptionIsDetected(t *testing.T) {
	t.Parallel()

	desc, obj, vals, dataOff := framedColumn(t, 1024)

	for i := range dataOff {
		bad := append([]byte(nil), obj...)
		bad[i] ^= 0x01

		_, err := newColumnReader(desc, bad, zstdComp(), len(vals)).Int64(nil)
		require.ErrorIsf(t, err, ErrCorrupt, "directory byte %d decoded without complaint", i)
	}
}

// TestUnblockedStreamCorruptionIsDetected covers the other encode path: a column written as one
// compressed stream has no directory, so its checksum trails the object.
func TestUnblockedStreamCorruptionIsDetected(t *testing.T) {
	t.Parallel()

	vals := checkedInts(2048)

	desc, obj, err := buildColumn(
		Column{Name: "ts", Kind: KindInt64, Codec: chunk.CodecDoD, Int64: vals}, zstdComp(),
		defaultGranuleSize, defaultCompressBlockBytes)
	require.NoError(t, err)
	require.True(t, desc.Checked)
	require.False(t, desc.Blocked)

	got, err := newColumnReader(desc, obj, zstdComp(), len(vals)).Int64(nil)
	require.NoError(t, err)
	require.Equal(t, vals, got)

	for _, tc := range []struct {
		name   string
		mutate func([]byte) []byte
	}{
		{"body", func(o []byte) []byte { o[len(o)/2] ^= 0xff; return o }},
		{"checksum", func(o []byte) []byte { o[len(o)-1] ^= 0xff; return o }},
		{"truncated to nothing", func([]byte) []byte { return nil }},
		{"checksum dropped", func(o []byte) []byte { return o[:len(o)-objectCRCBytes] }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			bad := tc.mutate(append([]byte(nil), obj...))

			_, err := newColumnReader(desc, bad, zstdComp(), len(vals)).Int64(nil)
			require.ErrorIs(t, err, ErrCorrupt)
			assert.Contains(t, err.Error(), `column "ts"`)
		})
	}
}

// TestSharedDictCorruptionIsDetected covers the third thing a column object can hold: the
// column-wide bytes dictionary, which sits ahead of the frames and so is covered by neither their
// checksums nor the directory's.
func TestSharedDictCorruptionIsDetected(t *testing.T) {
	t.Parallel()

	const n = 4096

	vals := make([][]byte, n)
	for i := range vals {
		vals[i] = []byte("service-" + strconv.Itoa(i%16))
	}

	desc, obj, err := buildColumn(
		Column{Name: "svc", Kind: KindBytes, Codec: chunk.CodecDict, Bytes: vals, Block: true},
		zstdComp(), 64, 512)
	require.NoError(t, err)
	require.True(t, desc.SharedDict)

	// The dictionary blob sits after the two header uvarints.
	_, n1 := binary.Uvarint(obj)
	packedLen, n2 := binary.Uvarint(obj[n1:])
	dictAt := n1 + n2

	bad := append([]byte(nil), obj...)
	bad[dictAt+int(packedLen)/2] ^= 0x01

	_, err = newColumnReader(desc, bad, zstdComp(), n).Bytes()
	require.ErrorIs(t, err, ErrCorrupt)
	assert.Contains(t, err.Error(), "shared dict")
}

// TestFooterColumnCorruptionIsDetected covers the streaming writer's layout end to end: its
// directory trails the frames, and the column is read back by ranged reads rather than whole.
func TestFooterColumnCorruptionIsDetected(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	b := newStreamingMemory()
	rows := gen(4, 64, func(r *rand.Rand, _, _ int) float64 { return r.Float64() }, 7)

	rows.writeStreamTo(t, ctx, b, "p", false, WithSortKey("ts"), WithGranuleSize(8), WithCompressBlockBytes(64))

	key := columnKey("p", 2) // the value column

	object, err := b.Read(ctx, key)
	require.NoError(t, err)

	r, err := OpenPart(ctx, b, "p")
	require.NoError(t, err)

	desc, ok := r.ColumnDescByName("value")
	require.True(t, ok)
	require.True(t, desc.Footer)
	require.True(t, desc.Checked)

	dir, err := parseBlockDir(object, desc)
	require.NoError(t, err)
	require.Greater(t, len(dir.frameOff), 2, "the column must hold several frames")

	// The frames lead under this layout, so a frame offset is an object offset.
	last := len(dir.frameOff) - 2

	bad := append([]byte(nil), object...)
	bad[dir.frameOff[last]] ^= 0x01

	require.NoError(t, b.Write(ctx, key, bad))

	r, err = OpenPart(ctx, b, "p")
	require.NoError(t, err)

	dec, err := r.ColumnBlocks(ctx, "value")
	require.NoError(t, err)

	var lastErr error

	for blk := range dec.NumBlocks() {
		if _, err := dec.DecodeFloat64(blk); err != nil {
			lastErr = err
		}
	}

	require.ErrorIs(t, lastErr, ErrCorrupt)
}

// stripFrameCRCs rewrites a leading-directory framed column object into the bytes a writer that
// predates the checksums would have produced: the same fields with the per-frame and directory
// checksums removed.
func stripFrameCRCs(tb testing.TB, obj []byte) []byte {
	tb.Helper()

	rest := obj

	next := func() uint64 {
		v, n := binary.Uvarint(rest)
		require.Positive(tb, n)
		rest = rest[n:]

		return v
	}

	granules, blockRows, frames := next(), next(), next()

	out := binary.AppendUvarint(nil, granules)
	out = binary.AppendUvarint(out, blockRows)
	out = binary.AppendUvarint(out, frames)

	for range frames {
		count, clen := next(), next()
		out = binary.AppendUvarint(out, count)
		out = binary.AppendUvarint(out, clen)

		require.GreaterOrEqual(tb, len(rest), dirCRCBytes)
		rest = rest[dirCRCBytes:]
	}

	for range granules {
		out = binary.AppendUvarint(out, next())
	}

	require.GreaterOrEqual(tb, len(rest), dirCRCBytes)

	return append(out, rest[dirCRCBytes:]...)
}

// TestVersion1PartStillReads pins the compatibility half: a part written before the checksums
// existed carries none, and its manifest says so, so it reads exactly as it always did.
func TestVersion1PartStillReads(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	framedDesc, framedObj, framedVals, _ := framedColumn(t, 1024)

	plainVals := checkedInts(1024)
	plainDesc, plainObj, err := buildColumn(
		Column{Name: "plain", Kind: KindInt64, Codec: chunk.CodecDoD, Int64: plainVals}, zstdComp(),
		defaultGranuleSize, defaultCompressBlockBytes)
	require.NoError(t, err)

	// Downgrade both descriptors and both objects to the version-1 shapes.
	framedDesc.Checked, plainDesc.Checked = false, false
	framedDesc.Bytes, plainDesc.Bytes = 0, 0

	v1 := builtPart{
		objects: [][]byte{stripFrameCRCs(t, framedObj), plainObj[:len(plainObj)-objectCRCBytes]},
		marks:   BuildMarks(framedVals, defaultGranuleSize).Encode(nil),
	}

	m := Manifest{
		Version: manifestVersionMin, RowCount: len(framedVals), GranuleSize: defaultGranuleSize,
		MinTime: framedDesc.MinInt64, MaxTime: framedDesc.MaxInt64,
		Columns: []ColumnDesc{framedDesc, plainDesc},
	}
	v1.manifest = m.Encode(nil)

	b := newStreamingMemory()
	require.NoError(t, v1.write(ctx, b, "old"))

	r, err := OpenPart(ctx, b, "old")
	require.NoError(t, err)
	require.Equal(t, manifestVersionMin, r.Manifest().Version)
	require.False(t, r.Manifest().Columns[0].Checked, "a version-1 column is read unverified")

	for name, want := range map[string][]int64{"ts": framedVals, "plain": plainVals} {
		col, err := r.Column(ctx, name)
		require.NoError(t, err)

		got, err := col.Int64(nil)
		require.NoError(t, err)
		assert.Equal(t, want, got, "column %q", name)
	}
}

// TestManifestRejectsFutureVersion pins the other end of the accepted range: a version the reader
// does not know is refused, with a message naming what it can read.
func TestManifestRejectsFutureVersion(t *testing.T) {
	t.Parallel()

	m := sampleManifest()
	m.Version = manifestVersion + 1

	_, err := DecodeManifest(m.Encode(nil))
	require.ErrorIs(t, err, ErrCorrupt)
	assert.Contains(t, err.Error(), "unsupported version")
}

// corruptionCorpus is the pair of objects the corruption fuzzer mutates: one framed, one written as
// a single stream. Built once — the fuzzer runs the decode, not the encode.
var corruptionCorpus = sync.OnceValue(func() (out struct {
	framedDesc ColumnDesc
	framedObj  []byte
	plainDesc  ColumnDesc
	plainObj   []byte
	vals       []int64
},
) {
	vals := checkedInts(1024)
	out.vals = vals

	var err error

	out.framedDesc, out.framedObj, err = buildColumn(
		Column{Name: "ts", Kind: KindInt64, Codec: chunk.CodecDoD, Int64: vals, Block: true},
		zstdComp(), 16, 512)
	if err != nil {
		panic(err)
	}

	out.plainDesc, out.plainObj, err = buildColumn(
		Column{Name: "ts", Kind: KindInt64, Codec: chunk.CodecDoD, Int64: vals},
		zstdComp(), defaultGranuleSize, defaultCompressBlockBytes)
	if err != nil {
		panic(err)
	}

	return out
})

// FuzzColumnCorruptionIsDetected is the format-level property the checksums buy: every byte of a
// checked column object is covered, so flipping any single one of them must make the read fail. A
// decode that succeeds on damaged bytes is the silent-wrong-answer case the checksums exist to
// prevent, and is a failure here even when the values happen to come back right.
func FuzzColumnCorruptionIsDetected(f *testing.F) {
	for _, off := range []uint32{0, 1, 7, 64, 300, 1000} {
		f.Add(off, byte(1), true)
		f.Add(off, byte(0x80), false)
	}

	f.Fuzz(func(t *testing.T, off uint32, mask byte, framed bool) {
		if mask == 0 {
			return
		}

		c := corruptionCorpus()

		desc, obj := c.plainDesc, c.plainObj
		if framed {
			desc, obj = c.framedDesc, c.framedObj
		}

		bad := append([]byte(nil), obj...)
		bad[int(off)%len(bad)] ^= mask

		if _, err := newColumnReader(desc, bad, zstdComp(), len(c.vals)).Int64(nil); err == nil {
			t.Fatalf("a flipped byte at %d decoded without error", int(off)%len(bad))
		}
	})
}

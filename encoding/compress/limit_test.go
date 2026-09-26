package compress

import (
	"bytes"
	"encoding/binary"
	"math"
	"runtime"
	"testing"

	"github.com/klauspost/compress/zstd"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/internal/heaptest"
)

func TestDecompressLimitRoundTrip(t *testing.T) {
	t.Parallel()

	cases := [][]byte{
		nil,
		[]byte("hello world"),
		makeRepetitive(200, "ab"),
		makeRepetitive(4096, "ab"),
		makeRandom(4096),
		make([]byte, 3<<20),
	}

	for _, alg := range []Algorithm{AlgorithmNone, AlgorithmZSTD, AlgorithmLZ4} {
		c := NewCompressor(alg, LevelDefault)

		for _, data := range cases {
			src := c.Compress(nil, data)

			got, err := c.DecompressLimit(nil, src, len(data))
			require.NoError(t, err, "%s/%d", alg, len(data))
			assert.True(t, bytes.Equal(data, got), "%s/%d", alg, len(data))

			if len(data) > 0 {
				_, err = c.DecompressLimit(nil, src, len(data)-1)
				require.ErrorIs(t, err, ErrLimit, "%s/%d one byte short", alg, len(data))
			}
		}
	}
}

func TestDecompressLimitReusesDst(t *testing.T) {
	t.Parallel()

	data := makeRepetitive(4096, "abc")

	for _, alg := range []Algorithm{AlgorithmNone, AlgorithmZSTD, AlgorithmLZ4} {
		c := NewCompressor(alg, LevelDefault)
		src := c.Compress(nil, data)

		buf := make([]byte, 0, len(data)+zstdSlack)
		got, err := c.DecompressLimit(buf, src, len(data))
		require.NoError(t, err)
		assert.Equal(t, data, got)
		assert.Same(t, unsafeFirst(buf[:1]), unsafeFirst(got), "%s reuses a dst with room", alg)

		small := make([]byte, 0, 8)
		got, err = c.DecompressLimit(small, src, len(data))
		require.NoError(t, err)
		assert.Equal(t, data, got)
		assert.NotSame(t, unsafeFirst(small[:1]), unsafeFirst(got), "%s never grows a short dst", alg)
	}
}

func unsafeFirst(b []byte) *byte { return &b[:1][0] }

// TestDecompressLimitDiscardsBuf: a full buffer is scratch, not a prefix, so a small block decoded
// into it allocates the block alone.
//
//nolint:paralleltest // reads process-wide heap counters
func TestDecompressLimitDiscardsBuf(t *testing.T) {
	full := make([]byte, 64<<20)

	for _, alg := range []Algorithm{AlgorithmNone, AlgorithmZSTD, AlgorithmLZ4} {
		c := NewCompressor(alg, LevelDefault)
		data := makeRepetitive(4096, "abc")
		src := c.Compress(nil, data)

		var (
			got []byte
			err error
		)

		allocated := heaptest.Allocated(func() { got, err = c.DecompressLimit(full, src, len(data)) })
		require.NoError(t, err)
		assert.Equal(t, data, got, alg.String())
		assert.LessOrEqual(t, allocated, uint64(len(data)+DecodeWorkspace), alg.String())
	}
}

// TestDecompressLimitHugeLengths: lengths near MaxInt under a MaxInt limit are rejected, not
// allocated.
func TestDecompressLimitHugeLengths(t *testing.T) {
	t.Parallel()

	fcs := binary.LittleEndian.AppendUint64([]byte{FlagCompressed, 0x28, 0xb5, 0x2f, 0xfd, 3<<6 | 1<<5}, math.MaxInt64)

	for _, tc := range []struct {
		name string
		alg  Algorithm
		src  []byte
	}{
		{"lz4 MaxInt64", AlgorithmLZ4, append(binary.AppendUvarint([]byte{FlagCompressed}, math.MaxInt64), lz4Literal("abc")...)},
		{"lz4 MaxInt64-1", AlgorithmLZ4, append(binary.AppendUvarint([]byte{FlagCompressed}, math.MaxInt64-1), lz4Literal("abc")...)},
		{"zstd FCS MaxInt64", AlgorithmZSTD, append(fcs, append(blockHeader(true, 0, 1), 'a')...)},
		{"zstd FCS MaxInt64 RLE", AlgorithmZSTD, append(fcs, append(blockHeader(true, 1, zstdMaxBlock), 'a')...)},
	} {
		_, err := NewCompressor(tc.alg, LevelDefault).DecompressLimit(nil, tc.src, math.MaxInt)
		require.ErrorIs(t, err, ErrMalformed, tc.name)
	}

	data := makeRepetitive(1<<20, "raw")
	for _, alg := range []Algorithm{AlgorithmNone, AlgorithmZSTD, AlgorithmLZ4} {
		c := NewCompressor(alg, LevelDefault)
		got, err := c.DecompressLimit(nil, c.Compress(nil, data), math.MaxInt)
		require.NoError(t, err, alg.String())
		assert.Equal(t, data, got, alg.String())
	}
}

func TestOutputSlack(t *testing.T) {
	t.Parallel()

	assert.Equal(t, zstdSlack, NewCompressor(AlgorithmZSTD, LevelDefault).OutputSlack())
	assert.Zero(t, OutputSlack(AlgorithmLZ4))
	assert.Zero(t, OutputSlack(AlgorithmNone))
}

func TestDecompressLimitNoFCSFrame(t *testing.T) {
	t.Parallel()

	enc, err := zstd.NewWriter(nil)
	require.NoError(t, err)

	c := NewCompressor(AlgorithmZSTD, LevelDefault)
	data := makeRepetitive(200, "xyz")
	src := enc.EncodeAll(data, []byte{FlagCompressed})

	var h zstd.Header
	require.NoError(t, h.Decode(src[1:]))
	require.False(t, h.HasFCS, "klauspost writes no content size below 256 bytes")

	got, err := c.DecompressLimit(nil, src, len(data))
	require.NoError(t, err)
	assert.Equal(t, data, got)

	_, err = c.DecompressLimit(nil, src, len(data)-1)
	require.ErrorIs(t, err, ErrLimit)
}

func TestDecompressLimitMalformed(t *testing.T) {
	t.Parallel()

	z := NewCompressor(AlgorithmZSTD, LevelDefault)
	l := NewCompressor(AlgorithmLZ4, LevelDefault)
	n := NewCompressor(AlgorithmNone, LevelDefault)
	frame := z.Compress(nil, makeRepetitive(4096, "ab"))[1:]

	for _, tc := range []struct {
		name string
		c    *Compressor
		src  []byte
	}{
		{"bad flag", z, []byte{7, 1, 2}},
		{"compressed under none", n, []byte{FlagCompressed, 1}},
		{"lz4 no length", l, []byte{FlagCompressed}},
		{"lz4 zero length", l, []byte{FlagCompressed, 0, 0x10, 0x61}},
		{"lz4 garbage", l, []byte{FlagCompressed, 8, 0xab, 1}},
		{"lz4 short decode", l, append([]byte{FlagCompressed, 8}, lz4Literal("abc")...)},
		{"zstd bad magic", z, []byte{FlagCompressed, 1, 2, 3, 4, 5}},
		{"zstd truncated header", z, []byte{FlagCompressed, 0x28, 0xb5}},
		{"zstd truncated", z, append([]byte{FlagCompressed}, frame[:len(frame)-5]...)},
		{"zstd checksum truncated", z, append([]byte{FlagCompressed}, frame[:len(frame)-2]...)},
		{"zstd trailing byte", z, append(append([]byte{FlagCompressed}, frame...), 0)},
		{"zstd reserved block", z, zstdFrame(false, 0, 10, blockHeader(true, 3, 4), []byte{1, 2, 3, 4})},
		{"zstd oversized block", z, zstdFrame(true, 1<<20, 0, blockHeader(true, 0, zstdMaxBlock+1))},
		{"zstd dictionary frame", z, append([]byte{FlagCompressed, 0x28, 0xb5, 0x2f, 0xfd, 0x21, 7, 0}, blockHeader(true, 0, 0)...)},
		{"zstd bad checksum", z, badChecksum(frame)},
	} {
		_, err := tc.c.DecompressLimit(nil, tc.src, 1<<20)
		require.Error(t, err, tc.name)
		assert.ErrorIs(t, err, ErrMalformed, tc.name)
	}
}

func badChecksum(frame []byte) []byte {
	out := append([]byte{FlagCompressed}, frame...)
	out[len(out)-1] ^= 0xff

	return out
}

// lz4Literal is an LZ4 block of one literal-only sequence.
func lz4Literal(s string) []byte {
	return append([]byte{byte(len(s)) << 4}, s...)
}

// blockHeader is a zstd block header: last flag, type (0 raw, 1 RLE, 2 compressed, 3 reserved) and
// size (the regenerated size for RLE).
func blockHeader(last bool, typ, size int) []byte {
	bh := uint32(size)<<3 | uint32(typ)<<1
	if last {
		bh |= 1
	}

	return []byte{byte(bh), byte(bh >> 8), byte(bh >> 16)}
}

// zstdFrame builds a FlagCompressed zstd frame header followed by parts: a content size of fcs when
// withFCS (a 4-byte field), otherwise a window of 1<<windowLog and no content size.
func zstdFrame(withFCS bool, fcs uint32, windowLog int, parts ...[]byte) []byte {
	out := []byte{FlagCompressed, 0x28, 0xb5, 0x2f, 0xfd}

	if withFCS {
		out = append(out, 2<<6|1<<5)
		out = binary.LittleEndian.AppendUint32(out, fcs)
	} else {
		out = append(out, 0, byte((windowLog-10)<<3))
	}

	for _, p := range parts {
		out = append(out, p...)
	}

	return out
}

// rleBlocks is n RLE blocks each regenerating a full block of one byte.
func rleBlocks(n int) []byte {
	out := make([]byte, 0, 4*n)

	for i := range n {
		out = append(out, blockHeader(i == n-1, 1, zstdMaxBlock)...)
		out = append(out, 'a')
	}

	return out
}

// streamedFrame is a zstd frame written by the streaming encoder with a flush after every chunk: no
// content size, one block per flush.
func streamedFrame(t *testing.T, chunks, size int) []byte {
	t.Helper()

	var buf bytes.Buffer

	enc, err := zstd.NewWriter(&buf)
	require.NoError(t, err)

	for i := range chunks {
		_, err := enc.Write(makeRepetitive(size, string(rune('a'+i%26))))
		require.NoError(t, err)
		require.NoError(t, enc.Flush())
	}

	require.NoError(t, enc.Close())

	return append([]byte{FlagCompressed}, buf.Bytes()...)
}

// decompressionBombs are inputs whose decode would outgrow limit; each must fail within limit plus
// [DecodeWorkspace] of allocation.
func decompressionBombs(t *testing.T, limit int) []struct {
	name string
	alg  Algorithm
	src  []byte
} {
	t.Helper()

	z := NewCompressor(AlgorithmZSTD, LevelDefault)
	big := z.Compress(nil, make([]byte, 64<<20))
	frame := big[1:]

	lz4Big := binary.AppendUvarint([]byte{FlagCompressed}, 64<<20)
	lz4Big = append(lz4Big, lz4Literal("abc")...)

	return []struct {
		name string
		alg  Algorithm
		src  []byte
	}{
		{"zstd valid frame past the limit", AlgorithmZSTD, big},
		{"zstd 256 MiB window without FCS", AlgorithmZSTD, zstdFrame(false, 0, 28, rleBlocks(1))},
		{"zstd 256 MiB window without FCS, many blocks", AlgorithmZSTD, zstdFrame(false, 0, 28, rleBlocks(2048))},
		{"zstd FCS at the limit, RLE blocks past it", AlgorithmZSTD, zstdFrame(true, uint32(limit), 0, rleBlocks(2048))},
		{"zstd multi-frame", AlgorithmZSTD, append(append([]byte(nil), big...), frame...)},
		{"zstd skippable frame", AlgorithmZSTD, append([]byte{FlagCompressed, 0x50, 0x2a, 0x4d, 0x18, 4, 0, 0, 0, 1, 2, 3, 4}, frame...)},
		{"zstd no FCS past 255", AlgorithmZSTD, zstdFrame(false, 0, 10, blockHeader(true, 1, 1000), []byte{'a'})},
		{"zstd no FCS, flushed blocks", AlgorithmZSTD, streamedFrame(t, 64, 64<<10)},
		{"lz4 length past the limit", AlgorithmLZ4, lz4Big},
		{"raw past the limit", AlgorithmNone, append([]byte{FlagRaw}, make([]byte, limit+1)...)},
	}
}

// TestDecompressLimitBombs asserts total allocation, not the returned length: a decoder that
// allocates first and checks after would pass a length check and still take the memory.
//
//nolint:paralleltest // reads process-wide heap counters
func TestDecompressLimitBombs(t *testing.T) {
	const limit = 1 << 20

	for _, tc := range decompressionBombs(t, limit) {
		c := NewCompressor(tc.alg, LevelDefault)

		var err error

		// Two collections empty the decoder pool, so the charge includes building a decoder.
		runtime.GC()
		runtime.GC()

		allocated := heaptest.Allocated(func() { _, err = c.DecompressLimit(nil, tc.src, limit) })

		require.Error(t, err, tc.name)
		assert.LessOrEqual(t, allocated, uint64(limit+DecodeWorkspace), tc.name)
		t.Logf("%s: allocated %d KiB: %v", tc.name, allocated>>10, err)
	}
}

// TestDecodeWorkspace measures what a zstd decode allocates past its output with a cold decoder
// pool, which [DecodeWorkspace] must cover.
//
//nolint:paralleltest // reads process-wide heap counters
func TestDecodeWorkspace(t *testing.T) {
	c := NewCompressor(AlgorithmZSTD, LevelDefault)

	var worst uint64

	for _, data := range [][]byte{
		makeRandom(zstdMaxBlock),
		makeRepetitive(1<<20, "the quick brown fox "),
		makeRandom(4 << 20),
		makeRepetitive(200, "ab"),
	} {
		src := c.Compress(nil, data)

		runtime.GC()
		runtime.GC()

		var err error

		allocated := heaptest.Allocated(func() { _, err = c.DecompressLimit(nil, src, len(data)) })
		require.NoError(t, err)

		worst = max(worst, allocated-uint64(len(data)))
	}

	t.Logf("zstd decode workspace: %d KiB", worst>>10)
	assert.LessOrEqual(t, worst, uint64(DecodeWorkspace))
}

var plainZstd, _ = zstd.NewReader(nil, zstd.WithDecoderConcurrency(1))

func FuzzDecompressLimit(f *testing.F) {
	for _, alg := range []Algorithm{AlgorithmNone, AlgorithmZSTD, AlgorithmLZ4} {
		c := NewCompressor(alg, LevelDefault)
		for _, data := range [][]byte{nil, []byte("hello"), makeRepetitive(300, "ab"), makeRandom(64)} {
			f.Add(byte(alg), c.Compress(nil, data), uint16(len(data)))
		}
	}

	f.Add(byte(AlgorithmZSTD), zstdFrame(true, 1000, 0, rleBlocks(2)), uint16(1000))
	f.Add(byte(AlgorithmZSTD), zstdFrame(false, 0, 28, blockHeader(true, 1, 100), []byte{'a'}), uint16(200))

	f.Fuzz(func(t *testing.T, alg byte, src []byte, limit uint16) {
		c := NewCompressor(Algorithm(alg%3), LevelDefault)

		got, err := c.DecompressLimit(nil, src, int(limit))
		if err != nil {
			return
		}

		if len(got) > int(limit) {
			t.Fatalf("decoded %d bytes past limit %d", len(got), limit)
		}

		want, err := c.Decompress(nil, src)
		if c.alg == AlgorithmZSTD && len(src) > 0 && src[0] == FlagCompressed {
			// Decompress swallows zstd errors, and the cgo build rejects windows klauspost takes.
			want, err = plainZstd.DecodeAll(src[1:], nil)
		}

		if err != nil {
			t.Fatalf("DecompressLimit accepted what an unlimited decode rejects: %v", err)
		}

		if !bytes.Equal(got, want) {
			t.Fatalf("DecompressLimit and Decompress disagree")
		}
	})
}

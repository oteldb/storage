package compress

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCompressRoundTrip(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		data []byte
	}{
		{"empty", nil},
		{"small", []byte("hello world")},
		{"repetitive", makeRepetitive(4096, "ab")},
		{"random", makeRandom(4096)},
		{"zeros", make([]byte, 4096)},
	}
	for _, alg := range []Algorithm{AlgorithmNone, AlgorithmZSTD} {
		for _, tc := range cases {
			t.Run(tc.name+"/"+alg.String(), func(t *testing.T) {
				t.Parallel()
				c := NewCompressor(alg, LevelDefault)
				compressed := c.Compress(nil, tc.data)
				got, err := c.Decompress(nil, compressed)
				require.NoError(t, err)
				assert.Equal(t, tc.data, got)
			})
		}
	}
}

func TestCompressShrinks(t *testing.T) {
	t.Parallel()
	data := makeRepetitive(8192, "hello world! ")
	c := NewCompressor(AlgorithmZSTD, LevelDefault)
	compressed := c.Compress(nil, data)
	assert.Less(t, len(compressed), len(data), "ZSTD should shrink repetitive data")
	// The compressed data should start with FlagCompressed.
	assert.Equal(t, FlagCompressed, compressed[0])
}

func TestCompressZSTDRoundTripLarge(t *testing.T) {
	t.Parallel()
	// Large repetitive data → ZSTD actually compresses (not raw fallback).
	data := makeRepetitive(65536, "the quick brown fox jumps over the lazy dog")
	c := NewCompressor(AlgorithmZSTD, LevelDefault)
	compressed := c.Compress(nil, data)
	require.Equal(t, FlagCompressed, compressed[0])
	require.Less(t, len(compressed), len(data), "ZSTD should shrink")

	got, err := c.Decompress(nil, compressed)
	require.NoError(t, err)
	assert.Equal(t, data, got)
}

func TestCompressZSTDFastLevel(t *testing.T) {
	t.Parallel()
	c := NewCompressor(AlgorithmZSTD, LevelFast)
	data := makeRepetitive(4096, "ab")
	compressed := c.Compress(nil, data)
	got, err := c.Decompress(nil, compressed)
	require.NoError(t, err)
	assert.Equal(t, data, got)
}

func TestCompressZSTDBestLevel(t *testing.T) {
	t.Parallel()
	c := NewCompressor(AlgorithmZSTD, LevelBest)
	data := makeRepetitive(4096, "ab")
	compressed := c.Compress(nil, data)
	got, err := c.Decompress(nil, compressed)
	require.NoError(t, err)
	assert.Equal(t, data, got)
}

func TestCompressReuseBuffer(t *testing.T) {
	t.Parallel()
	c := NewCompressor(AlgorithmZSTD, LevelDefault)
	data := makeRepetitive(4096, "test")
	// Compress into a pre-allocated dst.
	dst := make([]byte, 0, 8192)
	compressed := c.Compress(dst, data)
	got, err := c.Decompress(nil, compressed)
	require.NoError(t, err)
	assert.Equal(t, data, got)
}

func TestAlgorithmString(t *testing.T) {
	t.Parallel()
	cases := []struct {
		alg  Algorithm
		want string
	}{
		{AlgorithmNone, "none"},
		{AlgorithmZSTD, "zstd"},
		{AlgorithmLZ4, "lz4"},
		{Algorithm(99), "unknown"},
	}
	for _, tc := range cases {
		assert.Equalf(t, tc.want, tc.alg.String(), "Algorithm(%d).String()", tc.alg)
	}
}

func TestCompressorAlgorithm(t *testing.T) {
	t.Parallel()
	c := NewCompressor(AlgorithmZSTD, LevelDefault)
	assert.Equal(t, AlgorithmZSTD, c.Algorithm())
}

func TestDecompressBadFlag(t *testing.T) {
	t.Parallel()
	c := NewCompressor(AlgorithmZSTD, LevelDefault)
	_, err := c.Decompress(nil, []byte{0xFF, 0x00})
	require.Error(t, err, "expected error for bad flag")
}

func TestDecompressEmpty(t *testing.T) {
	t.Parallel()
	c := NewCompressor(AlgorithmZSTD, LevelDefault)
	got, err := c.Decompress(nil, nil)
	require.NoError(t, err)
	assert.Empty(t, got)
}

func TestCompressRawFallback(t *testing.T) {
	t.Parallel()
	// Very small input: ZSTD overhead > payload → raw fallback.
	c := NewCompressor(AlgorithmZSTD, LevelDefault)
	data := []byte{0x01}
	compressed := c.Compress(nil, data)
	// Should start with FlagRaw.
	assert.Equal(t, FlagRaw, compressed[0], "tiny input should fall back to raw")

	got, err := c.Decompress(nil, compressed)
	require.NoError(t, err)
	assert.Equal(t, data, got)
}

func TestCompressNoneAlgorithm(t *testing.T) {
	t.Parallel()
	c := NewCompressor(AlgorithmNone, LevelDefault)
	data := []byte("hello world")
	compressed := c.Compress(nil, data)
	// AlgorithmNone always uses FlagRaw.
	assert.Equal(t, FlagRaw, compressed[0])

	got, err := c.Decompress(nil, compressed)
	require.NoError(t, err)
	assert.Equal(t, data, got)
}

func TestCompressLZ4(t *testing.T) {
	t.Parallel()
	c := NewCompressor(AlgorithmLZ4, LevelDefault)
	data := makeRepetitive(1024, "hello")
	compressed := c.Compress(nil, data)
	got, err := c.Decompress(nil, compressed)
	require.NoError(t, err)
	assert.Equal(t, data, got)
}

func TestLevelConstants(t *testing.T) {
	t.Parallel()
	assert.NotNil(t, NewCompressor(AlgorithmZSTD, LevelFast))
	assert.NotNil(t, NewCompressor(AlgorithmZSTD, LevelBest))
}

func TestErrBadFlag(t *testing.T) {
	t.Parallel()
	e := badFlagError{flag: 0xFF}
	assert.Equal(t, "compress: unknown block flag", e.Error())
}

func TestCompressZSTDRandomData(t *testing.T) {
	t.Parallel()
	// Random data: ZSTD can't compress → raw fallback path.
	data := makeRandom(256)
	c := NewCompressor(AlgorithmZSTD, LevelDefault)
	compressed := c.Compress(nil, data)
	// Should be either raw (if ZSTD didn't help) or compressed — either way round-trips.
	got, err := c.Decompress(nil, compressed)
	require.NoError(t, err)
	assert.Equal(t, data, got)
}

func makeRepetitive(n int, pattern string) []byte {
	out := make([]byte, n)
	for i := range n {
		out[i] = pattern[i%len(pattern)]
	}
	return out
}

func makeRandom(n int) []byte {
	out := make([]byte, n)
	// Deterministic pseudo-random for reproducibility.
	seed := uint64(12345)
	for i := range n {
		seed = seed*6364136223846793005 + 1442695040888963407
		out[i] = byte(seed >> 33)
	}
	return out
}

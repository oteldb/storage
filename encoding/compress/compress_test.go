package compress

import (
	"bytes"
	"testing"
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
				if err != nil {
					t.Fatalf("Decompress: %v", err)
				}
				if len(got) != len(tc.data) {
					t.Fatalf("len = %d, want %d", len(got), len(tc.data))
				}
				for i := range tc.data {
					if got[i] != tc.data[i] {
						t.Fatalf("byte %d = %d, want %d", i, got[i], tc.data[i])
					}
				}
			})
		}
	}
}

func TestCompressShrinks(t *testing.T) {
	t.Parallel()
	data := makeRepetitive(8192, "hello world! ")
	c := NewCompressor(AlgorithmZSTD, LevelDefault)
	compressed := c.Compress(nil, data)
	if len(compressed) >= len(data) {
		t.Fatalf("ZSTD didn't shrink: %d → %d", len(data), len(compressed))
	}
	// The compressed data should start with FlagCompressed.
	if compressed[0] != FlagCompressed {
		t.Errorf("expected FlagCompressed, got %d", compressed[0])
	}
}

func TestCompressZSTDRoundTripLarge(t *testing.T) {
	t.Parallel()
	// Large repetitive data → ZSTD actually compresses (not raw fallback).
	data := makeRepetitive(65536, "the quick brown fox jumps over the lazy dog")
	c := NewCompressor(AlgorithmZSTD, LevelDefault)
	compressed := c.Compress(nil, data)
	if compressed[0] != FlagCompressed {
		t.Fatalf("expected FlagCompressed, got %d (len=%d)", compressed[0], len(compressed))
	}
	if len(compressed) >= len(data) {
		t.Fatalf("ZSTD didn't shrink: %d → %d", len(data), len(compressed))
	}
	got, err := c.Decompress(nil, compressed)
	if err != nil {
		t.Fatalf("Decompress: %v", err)
	}
	if len(got) != len(data) {
		t.Fatalf("len = %d, want %d", len(got), len(data))
	}
	for i := range data {
		if got[i] != data[i] {
			t.Fatalf("byte %d mismatch", i)
		}
	}
}

func TestCompressZSTDFastLevel(t *testing.T) {
	t.Parallel()
	c := NewCompressor(AlgorithmZSTD, LevelFast)
	data := makeRepetitive(4096, "ab")
	compressed := c.Compress(nil, data)
	got, err := c.Decompress(nil, compressed)
	if err != nil {
		t.Fatalf("Decompress: %v", err)
	}
	if len(got) != len(data) {
		t.Fatalf("len = %d, want %d", len(got), len(data))
	}
}

func TestCompressZSTDBestLevel(t *testing.T) {
	t.Parallel()
	c := NewCompressor(AlgorithmZSTD, LevelBest)
	data := makeRepetitive(4096, "ab")
	compressed := c.Compress(nil, data)
	got, err := c.Decompress(nil, compressed)
	if err != nil {
		t.Fatalf("Decompress: %v", err)
	}
	if len(got) != len(data) {
		t.Fatalf("len = %d, want %d", len(got), len(data))
	}
}

func TestCompressReuseBuffer(t *testing.T) {
	t.Parallel()
	c := NewCompressor(AlgorithmZSTD, LevelDefault)
	data := makeRepetitive(4096, "test")
	// Compress into a pre-allocated dst.
	dst := make([]byte, 0, 8192)
	compressed := c.Compress(dst, data)
	got, err := c.Decompress(nil, compressed)
	if err != nil {
		t.Fatalf("Decompress: %v", err)
	}
	if len(got) != len(data) {
		t.Fatalf("len = %d, want %d", len(got), len(data))
	}
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
		if got := tc.alg.String(); got != tc.want {
			t.Errorf("Algorithm(%d).String() = %q, want %q", tc.alg, got, tc.want)
		}
	}
}

func TestCompressorAlgorithm(t *testing.T) {
	t.Parallel()
	c := NewCompressor(AlgorithmZSTD, LevelDefault)
	if c.Algorithm() != AlgorithmZSTD {
		t.Errorf("Algorithm() = %v, want ZSTD", c.Algorithm())
	}
}

func TestDecompressBadFlag(t *testing.T) {
	t.Parallel()
	c := NewCompressor(AlgorithmZSTD, LevelDefault)
	_, err := c.Decompress(nil, []byte{0xFF, 0x00})
	if err == nil {
		t.Fatal("expected error for bad flag")
	}
}

func TestDecompressEmpty(t *testing.T) {
	t.Parallel()
	c := NewCompressor(AlgorithmZSTD, LevelDefault)
	got, err := c.Decompress(nil, nil)
	if err != nil || len(got) != 0 {
		t.Fatalf("Decompress(nil) = %v, %v", got, err)
	}
}

func TestCompressRawFallback(t *testing.T) {
	t.Parallel()
	// Very small input: ZSTD overhead > payload → raw fallback.
	c := NewCompressor(AlgorithmZSTD, LevelDefault)
	data := []byte{0x01}
	compressed := c.Compress(nil, data)
	// Should start with FlagRaw.
	if compressed[0] != FlagRaw {
		t.Errorf("expected FlagRaw for tiny input, got %d", compressed[0])
	}
	got, err := c.Decompress(nil, compressed)
	if err != nil {
		t.Fatalf("Decompress: %v", err)
	}
	if got[0] != data[0] {
		t.Errorf("got %d, want %d", got[0], data[0])
	}
}

func TestCompressNoneAlgorithm(t *testing.T) {
	t.Parallel()
	c := NewCompressor(AlgorithmNone, LevelDefault)
	data := []byte("hello world")
	compressed := c.Compress(nil, data)
	// AlgorithmNone always uses FlagRaw.
	if compressed[0] != FlagRaw {
		t.Errorf("expected FlagRaw, got %d", compressed[0])
	}
	got, err := c.Decompress(nil, compressed)
	if err != nil {
		t.Fatalf("Decompress: %v", err)
	}
	if !bytes.Equal(got, data) {
		t.Errorf("got %q, want %q", got, data)
	}
}

func TestCompressLZ4(t *testing.T) {
	t.Parallel()
	c := NewCompressor(AlgorithmLZ4, LevelDefault)
	data := makeRepetitive(1024, "hello")
	compressed := c.Compress(nil, data)
	got, err := c.Decompress(nil, compressed)
	if err != nil {
		t.Fatalf("Decompress: %v", err)
	}
	if len(got) != len(data) {
		t.Fatalf("len = %d, want %d", len(got), len(data))
	}
}

func TestLevelConstants(t *testing.T) {
	t.Parallel()
	c := NewCompressor(AlgorithmZSTD, LevelFast)
	if c == nil {
		t.Fatal("NewCompressor(LevelFast) returned nil")
	}
	c = NewCompressor(AlgorithmZSTD, LevelBest)
	if c == nil {
		t.Fatal("NewCompressor(LevelBest) returned nil")
	}
}

func TestErrBadFlag(t *testing.T) {
	t.Parallel()
	e := badFlagError{flag: 0xFF}
	if e.Error() != "compress: unknown block flag" {
		t.Errorf("Error() = %q", e.Error())
	}
}

func TestCompressZSTDRandomData(t *testing.T) {
	t.Parallel()
	// Random data: ZSTD can't compress → raw fallback path.
	data := makeRandom(256)
	c := NewCompressor(AlgorithmZSTD, LevelDefault)
	compressed := c.Compress(nil, data)
	// Should be either raw (if ZSTD didn't help) or compressed.
	got, err := c.Decompress(nil, compressed)
	if err != nil {
		t.Fatalf("Decompress: %v", err)
	}
	if len(got) != len(data) {
		t.Fatalf("len = %d, want %d", len(got), len(data))
	}
	for i := range data {
		if got[i] != data[i] {
			t.Fatalf("byte %d mismatch", i)
		}
	}
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

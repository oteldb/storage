package block

import (
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/encoding/chunk"
	"github.com/oteldb/storage/encoding/compress"
)

// encodeLegacyBlocked encodes a column in the pre-framing layout — one compressed block per decode
// granule — so the tests can prove parts written before frame packing still read.
func encodeLegacyBlocked(t *testing.T, c Column, codec chunk.Codec, comp *compress.Compressor, blockRows int) []byte {
	t.Helper()

	n := c.rows()

	var blocks [][]byte

	for lo := 0; lo < n; lo += blockRows {
		stream, err := appendBlockStream(nil, c, codec, 0, lo, min(lo+blockRows, n))
		require.NoError(t, err)

		blocks = append(blocks, comp.Compress(nil, stream))
	}

	dst := binary.AppendUvarint(nil, uint64(len(blocks)))
	dst = binary.AppendUvarint(dst, uint64(blockRows))

	for _, b := range blocks {
		dst = binary.AppendUvarint(dst, uint64(len(b)))
	}

	for _, b := range blocks {
		dst = append(dst, b...)
	}

	return dst
}

// readColumn decodes a column through every blocked read path and asserts they agree, returning the
// values as float64 (int64 columns are converted) so the callers can compare uniformly.
func readColumn(t *testing.T, tc blockCase, desc ColumnDesc, obj []byte, comp func() *compress.Compressor, n int) []float64 {
	t.Helper()

	out := make([]float64, 0, n)

	if tc.int64 {
		full, err := newColumnReader(desc, obj, comp(), n).Int64(nil)
		require.NoError(t, err)

		for _, v := range full {
			out = append(out, float64(v))
		}
	} else {
		full, err := newColumnReader(desc, obj, comp(), n).Float64(nil)
		require.NoError(t, err)
		out = append(out, full...)
	}

	if n == 0 || desc.Const {
		return out
	}

	// Range decode over every window, the block-set decode, and the streaming cursor must all
	// reproduce the whole-column decode — each takes a different path through the frame cache.
	for lo := range n {
		for hi := lo + 1; hi <= n; hi++ {
			assert.Equal(t, out[lo:hi], rangeAsFloats(t, tc, desc, obj, comp, n, lo, hi), "range [%d,%d)", lo, hi)
		}
	}

	// Only the DoD timestamp codec has an int64 cursor; T64 has none.
	if !tc.int64 || desc.Codec == chunk.CodecDoD {
		assert.Equal(t, out, cursorAsFloats(t, tc, desc, obj, comp, n), "cursor")
	}

	return out
}

func rangeAsFloats(
	t *testing.T, tc blockCase, desc ColumnDesc, obj []byte, comp func() *compress.Compressor, n, lo, hi int,
) []float64 {
	t.Helper()

	got := make([]float64, 0, hi-lo)

	if tc.int64 {
		vals, err := newColumnReader(desc, obj, comp(), n).RangeInt64(nil, lo, hi)
		require.NoError(t, err)

		for _, v := range vals {
			got = append(got, float64(v))
		}

		return got
	}

	vals, err := newColumnReader(desc, obj, comp(), n).RangeFloat64(nil, lo, hi)
	require.NoError(t, err)

	return append(got, vals...)
}

func cursorAsFloats(
	t *testing.T, tc blockCase, desc ColumnDesc, obj []byte, comp func() *compress.Compressor, n int,
) []float64 {
	t.Helper()

	got := make([]float64, 0, n)
	r := newColumnReader(desc, obj, comp(), n)

	if tc.int64 {
		cur, err := r.TsCursor()
		require.NoError(t, err)

		for range n {
			v, err := cur.Next()
			require.NoError(t, err)
			got = append(got, float64(v))
		}

		return got
	}

	cur, err := r.FloatCursor()
	require.NoError(t, err)

	for range n {
		v, err := cur.Next()
		require.NoError(t, err)
		got = append(got, v)
	}

	return got
}

// TestFramePackingRoundTrip checks that the compression-frame target does not change what a column
// decodes to: from one granule per frame up to the whole column in one frame, every read path
// reproduces the reference (unblocked) decode.
func TestFramePackingRoundTrip(t *testing.T) {
	t.Parallel()

	const blockRows, n = 4, 26

	for _, tc := range blockCases() {
		for _, comp := range []struct {
			name string
			c    func() *compress.Compressor
		}{{"none", noneComp}, {"zstd", zstdComp}} {
			for _, cb := range []int{1, 16, 64, defaultCompressBlockBytes} {
				t.Run(tc.name+"/"+comp.name+"/cb="+itoaT(cb), func(t *testing.T) {
					t.Parallel()

					c := tc.col(n)

					desc, obj, err := buildColumn(c, comp.c(), blockRows, cb)
					require.NoError(t, err)
					assert.True(t, desc.Framed, "the writer only emits the framed layout")

					ref := c
					ref.Block = false
					refDesc, refObj, err := buildColumn(ref, comp.c(), blockRows, cb)
					require.NoError(t, err)

					want := readColumn(t, tc, refDesc, refObj, comp.c, n)
					assert.Equal(t, want, readColumn(t, tc, desc, obj, comp.c, n))
				})
			}
		}
	}
}

// TestFramePackingDirectory pins the packing itself: a frame holds consecutive granules until the
// byte target is met, so a large target yields far fewer frames than granules and a target of 1 byte
// degenerates to one frame per granule.
func TestFramePackingDirectory(t *testing.T) {
	t.Parallel()

	const blockRows, n = 4, 400

	c := blockCases()[0].col(n)
	granules := (n + blockRows - 1) / blockRows

	for _, tc := range []struct {
		compressBytes int
		wantFrames    int
	}{
		{1, granules},
		{defaultCompressBlockBytes, 1},
	} {
		desc, obj, err := buildColumn(c, noneComp(), blockRows, tc.compressBytes)
		require.NoError(t, err)

		dir, err := parseBlockDir(obj, desc)
		require.NoError(t, err)

		assert.Equal(t, granules, dir.nBlocks(), "decode granularity is independent of the frame size")
		assert.Len(t, dir.frameOff, tc.wantFrames+1, "compressBytes=%d", tc.compressBytes)
	}
}

// TestFramePackingShrinksZSTD is the point of the framing: with one granule per compressed block the
// entropy coder restarts every ~1.6 KB, so packing granules into a 64 KiB frame is materially denser
// at the same codec and granule size.
func TestFramePackingShrinksZSTD(t *testing.T) {
	t.Parallel()

	const blockRows, n = 1024, 1 << 16

	c := blockCases()[0].col(n)

	_, framed, err := buildColumn(c, zstdComp(), blockRows, defaultCompressBlockBytes)
	require.NoError(t, err)

	perGranule := encodeLegacyBlocked(t, c, chunk.CodecDoD, zstdComp(), blockRows)

	assert.Less(t, len(framed), len(perGranule),
		"frame-packed (%d B) must beat one-block-per-granule (%d B)", len(framed), len(perGranule))
}

// benchTimestamps is a jittered nanosecond timestamp column — the shape the framing exists for (a
// 15 s scrape interval with per-sample jitter, which the DoD codec cannot absorb but zstd can).
func benchTimestamps(n int) []int64 {
	ts := make([]int64, n)
	t := int64(1_700_000_000_000_000_000)

	for i := range ts {
		t += 15_000_000_000 + int64(i%97)*131_071
		ts[i] = t
	}

	return ts
}

// BenchmarkBlockedColumn measures encode and decode against the compression-frame target, reporting
// the encoded bytes per sample so the ratio side of the trade is visible next to the speed side.
// Throughput is sized by the logical column (rows × 8 B) on both sides.
func BenchmarkBlockedColumn(b *testing.B) {
	const blockRows, n = 1024, 1 << 17

	col := Column{Name: "ts", Kind: KindInt64, Codec: chunk.CodecDoD, Int64: benchTimestamps(n), Block: true}
	logical := int64(n * 8)

	for _, cb := range []int{1, defaultCompressBlockBytes} {
		name := "frame=granule"
		if cb > 1 {
			name = "frame=64KiB"
		}

		b.Run("encode/"+name, func(b *testing.B) {
			b.SetBytes(logical)
			b.ReportAllocs()

			var obj []byte

			for range b.N {
				_, o, err := buildColumn(col, zstdComp(), blockRows, cb)
				if err != nil {
					b.Fatal(err)
				}

				obj = o
			}

			b.ReportMetric(float64(len(obj))/float64(n), "B/sample")
		})

		desc, obj, err := buildColumn(col, zstdComp(), blockRows, cb)
		if err != nil {
			b.Fatal(err)
		}

		comp := zstdComp()

		b.Run("decode/"+name, func(b *testing.B) {
			b.SetBytes(logical)
			b.ReportAllocs()

			dst := make([]int64, 0, n)

			for range b.N {
				dst, err = newColumnReader(desc, obj, comp, n).Int64(dst)
				if err != nil {
					b.Fatal(err)
				}
			}
		})

		b.Run("decodeBlock/"+name, func(b *testing.B) {
			b.SetBytes(int64(blockRows * 8))
			b.ReportAllocs()

			dec, err := newColumnReader(desc, obj, comp, n).BlockDecoder()
			if err != nil {
				b.Fatal(err)
			}

			dst := make([]int64, 0, blockRows)

			for i := range b.N {
				if dst, err = dec.DecodeInt64Into(i%dec.NumBlocks(), dst); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// TestFramePackingHighlyCompressible covers the case where a frame compresses to far less than the
// granule streams it holds: the directory's granule lengths are measured in the *decompressed*
// frame, so they are not bounded by the object length and must not be rejected as such.
func TestFramePackingHighlyCompressible(t *testing.T) {
	t.Parallel()

	const blockRows, n = 64, 1 << 14

	vals := make([]int64, n)
	for i := range vals {
		vals[i] = int64(i % 8) // trivially compressible, so the object is far smaller than the streams
	}

	c := Column{Name: "c", Kind: KindInt64, Codec: chunk.CodecT64, Int64: vals, Block: true}

	desc, obj, err := buildColumn(c, zstdComp(), blockRows, defaultCompressBlockBytes)
	require.NoError(t, err)

	dir, err := parseBlockDir(obj, desc)
	require.NoError(t, err)
	require.Greater(t, int(dir.gLen[0])*dir.nBlocks(), len(obj), "the test needs streams larger than the object")

	got, err := newColumnReader(desc, obj, zstdComp(), n).Int64(nil)
	require.NoError(t, err)
	assert.Equal(t, vals, got)
}

// TestCompressionLevelManifest pins that the written compression level round-trips through the
// manifest (the fixed point of the merge engine's compression ladder) and that an uncompressed
// column records none.
func TestCompressionLevelManifest(t *testing.T) {
	t.Parallel()

	vals := []float64{1, 2, 3, 4}

	for _, tc := range []struct {
		name string
		comp *compress.Compressor
		want compress.Level
		alg  compress.Algorithm
	}{
		{"zstd/level3", compress.NewCompressor(compress.AlgorithmZSTD, 3), 3, compress.AlgorithmZSTD},
		{"zstd/default", compress.NewCompressor(compress.AlgorithmZSTD, compress.LevelDefault), 0, compress.AlgorithmZSTD},
		{"none/level3", compress.NewCompressor(compress.AlgorithmNone, 3), 0, compress.AlgorithmNone},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			desc, _, err := buildColumn(Column{Name: "v", Kind: KindFloat64, Float64: vals}, tc.comp,
				defaultGranuleSize, defaultCompressBlockBytes)
			require.NoError(t, err)
			assert.Equal(t, tc.want, desc.Level)

			m := Manifest{Version: manifestVersion, RowCount: len(vals), Columns: []ColumnDesc{desc}}

			got, err := DecodeManifest(m.Encode(nil))
			require.NoError(t, err)
			assert.Equal(t, tc.want, got.Columns[0].Level)
			assert.Equal(t, tc.alg, got.Columns[0].Compress)
		})
	}
}

// TestLegacyBlockedLayout checks that a column written before frame packing (Blocked, not Framed)
// still decodes through every read path.
func TestLegacyBlockedLayout(t *testing.T) {
	t.Parallel()

	const blockRows, n = 4, 26

	for _, tc := range blockCases() {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			c := tc.col(n)

			desc, obj, err := buildColumn(c, zstdComp(), blockRows, defaultCompressBlockBytes)
			require.NoError(t, err)

			legacyDesc := desc
			legacyDesc.Framed = false
			legacyObj := encodeLegacyBlocked(t, c, desc.Codec, zstdComp(), blockRows)

			assert.Equal(t,
				readColumn(t, tc, desc, obj, zstdComp, n),
				readColumn(t, tc, legacyDesc, legacyObj, zstdComp, n),
			)
		})
	}
}

// TestFramedManifestFlag pins that the Framed descriptor flag survives manifest encode/decode
// independently of Blocked.
func TestFramedManifestFlag(t *testing.T) {
	t.Parallel()

	m := Manifest{
		Version: manifestVersion, RowCount: 8, GranuleSize: 4,
		Columns: []ColumnDesc{
			{Name: "ts", Kind: KindInt64, Codec: chunk.CodecDoD, Blocked: true, Framed: true, MinInt64: 1, MaxInt64: 9},
			{Name: "legacy", Kind: KindInt64, Codec: chunk.CodecDoD, Blocked: true, MinInt64: 1, MaxInt64: 9},
		},
	}

	got, err := DecodeManifest(m.Encode(nil))
	require.NoError(t, err)
	assert.True(t, got.Columns[0].Framed)
	assert.False(t, got.Columns[1].Framed, "a legacy blocked column stays unframed")
}

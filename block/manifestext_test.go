package block

import (
	"encoding/binary"
	"encoding/hex"
	"hash/crc32"
	"math"
	"testing"

	"github.com/go-faster/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/encoding/chunk"
	"github.com/oteldb/storage/encoding/compress"
)

// trailerDesc is a trailer-dictionary column descriptor as the writer records it.
func trailerDesc() ColumnDesc {
	return ColumnDesc{
		Name: "attrs", Kind: KindBytes, Codec: chunk.CodecDict, Compress: compress.AlgorithmZSTD,
		Blocked: true, Framed: true, SharedDict: true, Footer: true, TrailerDict: true, Bytes: 1000,
		DictOff: 700, DictLen: 200, DictRaw: 600, DictEntries: 40, Checked: true, HasSizing: true,
		Sizing: ColumnSizing{
			DirLen: 96, NumGranules: 8, NumFrames: 2, MaxFrameBytes: 400, MaxFrameRaw: 900,
			MaxGranuleRaw: 300, StreamRaw: 1500,
		},
	}
}

func v3Manifest() Manifest {
	return Manifest{
		Version: manifestVersionExt, RowCount: 64, MinTime: 1, MaxTime: 64, GranuleSize: 8,
		Columns: []ColumnDesc{
			{
				Name: "ts", Kind: KindInt64, Codec: chunk.CodecDoD, Compress: compress.AlgorithmZSTD,
				Blocked: true, Framed: true, Bytes: 120, MinInt64: 1, MaxInt64: 64, Checked: true,
				HasSizing: true,
				Sizing: ColumnSizing{
					DirLen: 40, NumGranules: 8, NumFrames: 1, MaxFrameBytes: 80, MaxFrameRaw: 200,
					MaxGranuleRaw: 30, StreamRaw: 200,
				},
			},
			trailerDesc(),
			{
				Name: "body", Kind: KindBytes, Codec: chunk.CodecDict, Compress: compress.AlgorithmZSTD,
				Bytes: 500, Checked: true, HasSizing: true, Sizing: ColumnSizing{StreamRaw: 4000},
			},
			{
				Name: "unit", Kind: KindBytes, Codec: chunk.CodecDict, Checked: true, Const: true, ConstBytes: []byte("ms"),
			},
		},
		DiskBytes: 1620, RawBytes: 6000,
	}
}

// TestManifestGoldenV3 pins the version-3 layout: xflags after flags, then the trailer dictionary's
// four fields and the seven sizing fields after the object size.
func TestManifestGoldenV3(t *testing.T) {
	t.Parallel()

	m := v3Manifest()

	const golden = "4f54504d034002800108040274730001014c027828080150c8011ec80102800105617474727302" +
		"0301ec03e807bc05c801d8042860080290038407ac02dc0b04626f64790203014002f403000000000000a01f" +
		"04756e69740203000100026d73d40cf02e1e2874c1"

	enc := m.Encode(nil)
	assert.Equal(t, golden, hex.EncodeToString(enc))

	got, err := DecodeManifest(enc)
	require.NoError(t, err)
	assert.Equal(t, m, got)
}

func TestWriterVersion(t *testing.T) {
	t.Parallel()

	plain := sampleManifest().Columns
	assert.Equal(t, manifestVersionChecked, writerVersion(plain))
	assert.Equal(t, manifestVersionExt, writerVersion(append(plain, trailerDesc())))

	sized := ColumnDesc{Name: "c", Kind: KindInt64, Bytes: 10, HasSizing: true, Sizing: ColumnSizing{StreamRaw: 8}}
	assert.Equal(t, manifestVersionExt, writerVersion([]ColumnDesc{sized}))

	// A version-2 encode carries no xflags byte at all, so an older reader parses it.
	m := sampleManifest()
	m.Version = manifestVersionChecked
	got, err := DecodeManifest(m.Encode(nil))
	require.NoError(t, err)
	assert.Equal(t, checkedColumns(m), got)
}

// v3Mutations are version-3 manifests each carrying one field past what a writer produces.
func v3Mutations() []struct {
	name        string
	mutate      func(*Manifest)
	unsupported bool
} {
	trailer := func(f func(*ColumnDesc)) func(*Manifest) {
		return func(m *Manifest) { f(&m.Columns[1]) }
	}
	sizing := func(col int, f func(*ColumnSizing)) func(*Manifest) {
		return func(m *Manifest) { f(&m.Columns[col].Sizing) }
	}

	return []struct {
		name        string
		mutate      func(*Manifest)
		unsupported bool
	}{
		{"DictOff MaxUint64", trailer(func(c *ColumnDesc) { c.DictOff = -1 }), false},
		{"DictLen MaxUint64", trailer(func(c *ColumnDesc) { c.DictLen = -1 }), false},
		{"DictRaw MaxUint64", trailer(func(c *ColumnDesc) { c.DictRaw = -1 }), false},
		{"DictEntries MaxUint64", trailer(func(c *ColumnDesc) { c.DictEntries = -1 }), false},
		{"DictOff+DictLen wraps", trailer(func(c *ColumnDesc) { c.DictOff, c.DictLen = 900, math.MaxInt64 }), false},
		{"DictOff past object", trailer(func(c *ColumnDesc) { c.DictOff = 1001 }), false},
		{"no room for the footer", trailer(func(c *ColumnDesc) { c.DictLen = 298 }), false},
		{"DictRaw above the ceiling", trailer(func(c *ColumnDesc) { c.DictRaw = maxSharedDictRaw + 1 }), false},
		{"65537 entries", trailer(func(c *ColumnDesc) { c.DictEntries, c.DictRaw = 65537, 1<<20 }), false},
		{"more entries than bytes", trailer(func(c *ColumnDesc) { c.DictEntries = 601 }), false},
		{"DictLen below the minimum", trailer(func(c *ColumnDesc) { c.DictLen = 6 }), false},
		{"DictLen above the writer maximum", trailer(func(c *ColumnDesc) { c.DictRaw, c.DictEntries = 10, 1 }), false},
		{"trailer on an unframed column", trailer(func(c *ColumnDesc) { c.Framed = false }), false},
		{"trailer on a const column", trailer(func(c *ColumnDesc) { c.Const, c.ConstBytes = true, []byte("x") }), false},
		{"trailer on a raw column", trailer(func(c *ColumnDesc) { c.Codec = chunk.CodecBytesRaw }), false},
		{"trailer without its size", trailer(func(c *ColumnDesc) { c.Bytes = 0 }), false},
		{"footer shared dict without a trailer", trailer(func(c *ColumnDesc) { c.TrailerDict, c.HasSizing = false, false }), false},
		{"trailer DirLen off by one", sizing(1, func(s *ColumnSizing) { s.DirLen++ }), false},
		{"DirLen past the object", sizing(0, func(s *ColumnSizing) { s.DirLen = 200 }), false},
		{"more frames than granules", sizing(0, func(s *ColumnSizing) { s.NumFrames = 9 }), false},
		{"more granules than rows", sizing(0, func(s *ColumnSizing) { s.NumGranules, s.DirLen = 65, 100 }), false},
		{"more granules than directory bytes", sizing(0, func(s *ColumnSizing) { s.NumGranules = 41 }), false},
		{"frame past the object", sizing(0, func(s *ColumnSizing) { s.MaxFrameBytes = 121 }), false},
		{"granule past its frame", sizing(0, func(s *ColumnSizing) { s.MaxGranuleRaw = 201 }), false},
		{"frame past int32", sizing(0, func(s *ColumnSizing) { s.MaxFrameRaw, s.MaxGranuleRaw = math.MaxInt32+1, 1 }), false},
		{"StreamRaw MaxUint64", sizing(0, func(s *ColumnSizing) { s.StreamRaw = -1 }), false},
		{"directory fields on an unframed column", sizing(2, func(s *ColumnSizing) { s.NumGranules = 1 }), false},
		{"sizing on a const column", func(m *Manifest) { m.Columns[3].HasSizing = true }, false},
		{"sizing on the legacy blocked layout", func(m *Manifest) { m.Columns[0].Framed = false }, false},
		{"unknown xflags bit", func(m *Manifest) { m.Columns[0].Sizing.DirLen = -2 }, true},
	}
}

// v3Body encodes m without its CRC, applying the unknown-xflags mutation at the byte level: no
// ColumnDesc field sets an unknown bit.
func v3Body(tb testing.TB, m Manifest, unsupported bool) []byte {
	tb.Helper()

	if unsupported {
		m.Columns[0].Sizing = v3Manifest().Columns[0].Sizing
	}

	enc := m.Encode(nil)
	body := enc[:len(enc)-4]

	if unsupported {
		// The first column's xflags byte follows magic, version, rows, minTime, a 2-byte maxTime,
		// granule size, the column count, its name and its kind/codec/compress/flags bytes.
		const xflagsAt = 4 + 1 + 1 + 1 + 2 + 1 + 1 + 1 + 2 + 4
		require.Equal(tb, xSizing, body[xflagsAt])
		body[xflagsAt] |= 1 << 7
	}

	return body
}

func TestDecodeManifestRejectsV3Fields(t *testing.T) {
	t.Parallel()

	for _, tc := range v3Mutations() {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			m := v3Manifest()
			tc.mutate(&m)

			body := v3Body(t, m, tc.unsupported)
			src := binary.BigEndian.AppendUint32(body, crc32.Checksum(body, castagnoli))

			_, err := DecodeManifest(src)
			require.ErrorIs(t, err, ErrCorrupt)
			assert.Equal(t, tc.unsupported, errors.Is(err, ErrUnsupportedVersion))
		})
	}
}

func TestMaxDictRegion(t *testing.T) {
	t.Parallel()

	comp := compress.NewCompressor(compress.AlgorithmZSTD, compress.LevelDefault)

	for _, raw := range []int{0, 1, 127, 128, 16383, 16384, 1 << 20} {
		// Incompressible bytes take the raw fallback: the largest region a writer produces.
		blob := make([]byte, raw)
		for i := range blob {
			blob[i] = byte(i*7919 + i>>8)
		}

		region := dictRegion(comp, blob, min(raw, 1))
		assert.LessOrEqual(t, uint64(len(region)), maxDictRegion(uint64(min(raw, 1)), uint64(raw)), "raw %d", raw)
		assert.GreaterOrEqual(t, len(region), minDictRegion)
	}
}

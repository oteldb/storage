package block

import (
	"encoding/binary"
	"encoding/hex"
	"hash/crc32"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/encoding/bitstream"
	"github.com/oteldb/storage/encoding/chunk"
	"github.com/oteldb/storage/encoding/compress"
)

func sampleManifest() Manifest {
	return Manifest{
		Version:     manifestVersion,
		RowCount:    3,
		MinTime:     1000,
		MaxTime:     3000,
		GranuleSize: 8192,
		Columns: []ColumnDesc{
			{
				Name: "ts", Kind: KindInt64, Codec: chunk.CodecDoD, Compress: compress.AlgorithmNone,
				MinInt64: 1000, MaxInt64: 3000,
			},
			{
				Name: "value", Kind: KindFloat64, Codec: chunk.CodecGorilla, Compress: compress.AlgorithmZSTD,
				MinFloat64: -1.5, MaxFloat64: 42.25,
			},
			{
				Name: "unit", Kind: KindBytes, Codec: chunk.CodecDict, Compress: compress.AlgorithmNone,
				Const: true, ConstBytes: []byte("ms"),
			},
			{
				Name: "shard", Kind: KindInt64, Codec: chunk.CodecT64, Compress: compress.AlgorithmNone,
				Const: true, ConstInt64: 7, MinInt64: 7, MaxInt64: 7,
			},
			{
				Name: "ratio", Kind: KindFloat64, Codec: chunk.CodecDecimal, Compress: compress.AlgorithmNone,
				Const: true, ConstFloat64: 0.5, MinFloat64: 0.5, MaxFloat64: 0.5,
			},
			{
				Name: "name", Kind: KindBytes, Codec: chunk.CodecDict, Compress: compress.AlgorithmZSTD,
			},
			{
				Name: "series", Kind: KindInt128, Codec: chunk.CodecID128, Compress: compress.AlgorithmNone,
			},
		},
	}
}

// checkedColumns marks every column as carrying its data checksums, which is what a manifest at
// [manifestVersion] decodes to — the flag follows from the version rather than from the bytes.
func checkedColumns(m Manifest) Manifest {
	for i := range m.Columns {
		m.Columns[i].Checked = true
	}

	return m
}

func TestManifestRoundTrip(t *testing.T) {
	t.Parallel()

	m := sampleManifest()
	got, err := DecodeManifest(m.Encode(nil))
	require.NoError(t, err)
	assert.Equal(t, checkedColumns(sampleManifest()), got)
}

func TestManifestEncodeAppends(t *testing.T) {
	t.Parallel()

	m := sampleManifest()
	prefix := []byte("PREFIX")
	out := m.Encode(append([]byte{}, prefix...))
	assert.Equal(t, prefix, out[:len(prefix)], "Encode must append, not overwrite dst")

	// The manifest portion (after the prefix) decodes on its own.
	got, err := DecodeManifest(out[len(prefix):])
	require.NoError(t, err)
	assert.Equal(t, checkedColumns(sampleManifest()), got)
}

func TestManifestEmpty(t *testing.T) {
	t.Parallel()

	m := Manifest{Version: manifestVersion}
	got, err := DecodeManifest(m.Encode(nil))
	require.NoError(t, err)
	assert.Empty(t, got.Columns)
	assert.Equal(t, m.Version, got.Version)
}

func TestManifestRejectsCRCMismatch(t *testing.T) {
	t.Parallel()

	enc := sampleManifest().Encode(nil)
	enc[len(enc)-1] ^= 0xFF // corrupt the trailing CRC

	_, err := DecodeManifest(enc)
	require.ErrorIs(t, err, ErrCorrupt)
}

func TestManifestRejectsBitFlip(t *testing.T) {
	t.Parallel()

	enc := sampleManifest().Encode(nil)
	enc[2] ^= 0x01 // corrupt a body byte; CRC must catch it

	_, err := DecodeManifest(enc)
	require.ErrorIs(t, err, ErrCorrupt)
}

func TestManifestRejectsShort(t *testing.T) {
	t.Parallel()

	_, err := DecodeManifest([]byte{0x00, 0x01})
	require.ErrorIs(t, err, ErrCorrupt)
}

func TestManifestRejectsBadMagic(t *testing.T) {
	t.Parallel()

	enc := sampleManifest().Encode(nil)
	enc[0] ^= 0xFF
	// Recompute a valid CRC so we get past the CRC check to the magic check.
	crc := crc32.Checksum(enc[:len(enc)-4], castagnoli)
	binary.BigEndian.PutUint32(enc[len(enc)-4:], crc)

	_, err := DecodeManifest(enc)
	require.ErrorIs(t, err, ErrCorrupt)
}

func TestManifestRejectsBadVersion(t *testing.T) {
	t.Parallel()

	m := sampleManifest()
	m.Version = 999

	_, err := DecodeManifest(m.Encode(nil))
	require.ErrorIs(t, err, ErrCorrupt)
}

// TestManifestTruncationSweep rebuilds a valid CRC over every prefix of a valid
// manifest body, so DecodeManifest passes the CRC check and exercises each inner
// field's truncation (EOF) branch — all must return ErrCorrupt, none may panic.
func TestManifestTruncationSweep(t *testing.T) {
	t.Parallel()

	full := sampleManifest().Encode(nil)
	body := full[:len(full)-4] // drop the real CRC

	for n := range body { // every strict prefix
		truncated := make([]byte, n+4)
		copy(truncated, body[:n])
		binary.BigEndian.PutUint32(truncated[n:], crc32.Checksum(body[:n], castagnoli))

		got, err := DecodeManifest(truncated)
		if err == nil {
			// DiskBytes and RawBytes are trailing optional fields, so the prefixes that drop
			// exactly them are valid manifests in the older layouts — that is what makes the fields
			// readable both ways. Every other prefix must be rejected.
			want := checkedColumns(sampleManifest())
			want.DiskBytes, want.RawBytes = 0, 0
			require.Equalf(t, want, got, "prefix len %d decoded, so it must be an older-layout manifest", n)

			continue
		}

		require.ErrorIsf(t, err, ErrCorrupt, "prefix len %d", n)
	}

	// The full body (with its real CRC) decodes successfully.
	_, err := DecodeManifest(full)
	require.NoError(t, err)
}

// TestManifestRejectsHugeColCount covers the OOM guard: a column count far larger than
// the body must be rejected before allocating.
func TestManifestRejectsHugeColCount(t *testing.T) {
	t.Parallel()

	w := bitstream.NewWriter(nil)
	binary.BigEndian.PutUint32(w.AppendBytes(4), manifestMagic)
	w.WriteUvarint(uint64(manifestVersion))
	w.WriteUvarint(0)       // rowCount
	w.WriteVarint(0)        // minTime
	w.WriteVarint(0)        // maxTime
	w.WriteUvarint(8192)    // granuleSize
	w.WriteUvarint(1 << 40) // colCount — absurd
	w.PadToByte()

	body := w.Bytes()
	out := binary.BigEndian.AppendUint32(body, crc32.Checksum(body, castagnoli))

	_, err := DecodeManifest(out)
	require.ErrorIs(t, err, ErrCorrupt)
}

// TestManifestGolden pins the exact on-disk bytes of a fixed manifest so an accidental
// format change is caught. A purely additive change (a trailing field, a new flag bit) keeps the
// version and updates this golden; only a change that an existing reader cannot parse bumps
// manifestVersion.
func TestManifestGolden(t *testing.T) {
	t.Parallel()

	m := Manifest{
		Version: 1, RowCount: 2, MinTime: 100, MaxTime: 200, GranuleSize: 8192,
		Columns: []ColumnDesc{{
			Name: "ts", Kind: KindInt64, Codec: chunk.CodecDoD,
			Compress: compress.AlgorithmNone, MinInt64: 100, MaxInt64: 200,
		}},
	}

	const golden = "4f54504d0102c801900380400102747300010000c80190030000d19bb2b9"
	assert.Equal(t, golden, hex.EncodeToString(m.Encode(nil)))

	// And it must round-trip from the golden bytes.
	raw, err := hex.DecodeString(golden)
	require.NoError(t, err)
	got, err := DecodeManifest(raw)
	require.NoError(t, err)
	assert.Equal(t, m, got)

	// A manifest written before DiskBytes existed — the same bytes without the trailing field —
	// must still decode, reporting an unknown size rather than failing.
	const beforeDiskBytes = "4f54504d0102c801900380400102747300010000c80190032cc1aa66"

	raw, err = hex.DecodeString(beforeDiskBytes)
	require.NoError(t, err)
	got, err = DecodeManifest(raw)
	require.NoError(t, err)
	assert.Equal(t, m, got, "an older manifest reads identically, with DiskBytes zero")

	// The generation between the two: DiskBytes present, RawBytes not yet.
	const beforeRawBytes = "4f54504d0102c801900380400102747300010000c8019003001536e671"

	raw, err = hex.DecodeString(beforeRawBytes)
	require.NoError(t, err)
	got, err = DecodeManifest(raw)
	require.NoError(t, err)
	assert.Equal(t, m, got, "a manifest predating RawBytes reads identically, with RawBytes zero")

	// Both trailing sizes round-trip when set.
	sized := m
	sized.DiskBytes, sized.RawBytes = 4096, 65536

	got, err = DecodeManifest(sized.Encode(nil))
	require.NoError(t, err)
	assert.Equal(t, sized, got)
}

func TestKindString(t *testing.T) {
	t.Parallel()
	assert.Equal(t, "int64", KindInt64.String())
	assert.Equal(t, "float64", KindFloat64.String())
	assert.Equal(t, "bytes", KindBytes.String())
	assert.Equal(t, "int128", KindInt128.String())
	assert.Equal(t, "unknown", Kind(99).String())
}

// FuzzManifestDecode asserts DecodeManifest never panics on arbitrary input, and that
// any manifest it accepts re-encodes to bytes it accepts again (decode∘encode stable).
func FuzzManifestDecode(f *testing.F) {
	f.Add(sampleManifest().Encode(nil))
	f.Add([]byte{})
	f.Add([]byte{0, 0, 0, 0})

	f.Fuzz(func(t *testing.T, data []byte) {
		m, err := DecodeManifest(data)
		if err != nil {
			return
		}
		// Accepted ⇒ re-encode must round-trip.
		got, err := DecodeManifest(m.Encode(nil))
		require.NoError(t, err)
		assert.Equal(t, m, got)
	})
}

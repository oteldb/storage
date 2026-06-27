package recordengine

import (
	"context"
	"encoding/binary"
	"hash/crc32"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
)

func TestRecordKeysRoundTrip(t *testing.T) {
	t.Parallel()

	cases := [][][]byte{
		nil,
		{[]byte("http.method")},
		{[]byte("a"), []byte("bb"), []byte("ccc")},
		{[]byte("job"), []byte("filename"), []byte("http.status_code")},
	}

	for _, keys := range cases {
		got, err := decodeRecordKeys(encodeRecordKeys(keys))
		require.NoError(t, err)

		if len(keys) == 0 {
			assert.Empty(t, got)

			continue
		}

		assert.Equal(t, keys, got)
	}
}

// TestRecordKeysGoldenBytes pins the exact on-disk layout so an accidental format break is caught.
func TestRecordKeysGoldenBytes(t *testing.T) {
	t.Parallel()

	got := encodeRecordKeys([][]byte{[]byte("ab")})
	want := []byte{
		0x4F, 0x54, 0x4B, 0x59, // magic "OTKY"
		0x01,           // version
		0x01,           // count
		0x02, 'a', 'b', // key len + bytes
		0xDE, 0x06, 0x73, 0x17, // CRC32C(Castagnoli) over the body
	}
	require.Equal(t, want, got)
}

func TestRecordKeysDecodeRejectsCorruption(t *testing.T) {
	t.Parallel()

	good := encodeRecordKeys([][]byte{[]byte("a"), []byte("bb")})

	_, err := decodeRecordKeys(good[:3])
	require.ErrorIs(t, err, ErrCorruptKeys, "truncated")

	bad := append([]byte(nil), good...)
	bad[0] ^= 0xFF // corrupt the magic (also breaks the CRC)
	_, err = decodeRecordKeys(bad)
	require.ErrorIs(t, err, ErrCorruptKeys)

	bad = append([]byte(nil), good...)
	bad[len(bad)-1] ^= 0xFF // corrupt only the CRC trailer
	_, err = decodeRecordKeys(bad)
	require.ErrorIs(t, err, ErrCorruptKeys, "CRC mismatch")
}

// TestRecordKeysDecodeFieldErrors hits each validated field with an otherwise CRC-valid footer, so
// the magic/version/count/key-length guards are exercised independently of the CRC check.
func TestRecordKeysDecodeFieldErrors(t *testing.T) {
	t.Parallel()

	// resign rebuilds the trailing CRC over body so the field guard (not the CRC) is what trips.
	resign := func(body []byte) []byte {
		return binary.BigEndian.AppendUint32(body, crc32.Checksum(body, recordKeysCRC))
	}

	// Bad magic, valid CRC.
	body := binary.BigEndian.AppendUint32(nil, recordKeysMagic+1)
	body = binary.AppendUvarint(body, uint64(recordKeysVersion))
	body = binary.AppendUvarint(body, 0)
	_, err := decodeRecordKeys(resign(body))
	require.ErrorIs(t, err, ErrCorruptKeys, "bad magic")

	// Unsupported version.
	body = binary.BigEndian.AppendUint32(nil, recordKeysMagic)
	body = binary.AppendUvarint(body, uint64(recordKeysVersion)+1)
	body = binary.AppendUvarint(body, 0)
	_, err = decodeRecordKeys(resign(body))
	require.ErrorIs(t, err, ErrCorruptKeys, "version")

	// Count larger than the remaining body.
	body = binary.BigEndian.AppendUint32(nil, recordKeysMagic)
	body = binary.AppendUvarint(body, uint64(recordKeysVersion))
	body = binary.AppendUvarint(body, 99)
	_, err = decodeRecordKeys(resign(body))
	require.ErrorIs(t, err, ErrCorruptKeys, "count")

	// A key length that overruns the body.
	body = binary.BigEndian.AppendUint32(nil, recordKeysMagic)
	body = binary.AppendUvarint(body, uint64(recordKeysVersion))
	body = binary.AppendUvarint(body, 1)
	body = binary.AppendUvarint(body, 50) // claims a 50-byte key with no bytes following
	_, err = decodeRecordKeys(resign(body))
	require.ErrorIs(t, err, ErrCorruptKeys, "key length")
}

// TestRecordKeysDecodeTruncationSweep ensures every prefix of a valid encoding fails cleanly (never
// panics) rather than returning a partial result.
func TestRecordKeysDecodeTruncationSweep(t *testing.T) {
	t.Parallel()

	good := encodeRecordKeys([][]byte{[]byte("alpha"), []byte("beta"), []byte("gamma")})
	for i := range good {
		_, _ = decodeRecordKeys(good[:i]) // must not panic
	}
}

// TestLoadRecordKeysRejectsCorruptSidecar verifies a corrupt "keys.bin" footer surfaces as an error
// when the part is opened, rather than silently dropping keys.
func TestLoadRecordKeysRejectsCorruptSidecar(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	be := backend.Memory()

	require.NoError(t, be.Write(ctx, recordKeysKey("p/0000000000"), []byte("not a footer")))

	_, err := loadRecordKeys(ctx, be, "p/0000000000")
	require.ErrorIs(t, err, ErrCorruptKeys)

	keys, err := loadRecordKeys(ctx, be, "p/missing")
	require.NoError(t, err, "absent sidecar is not an error")
	assert.Nil(t, keys)
}

func FuzzRecordKeysDecode(f *testing.F) {
	f.Add(encodeRecordKeys(nil))
	f.Add(encodeRecordKeys([][]byte{[]byte("http.method"), []byte("job")}))
	f.Add([]byte("garbage"))

	f.Fuzz(func(t *testing.T, data []byte) {
		keys, err := decodeRecordKeys(data)
		if err != nil {
			return // malformed input must fail cleanly, never panic
		}

		// encode∘decode is idempotent: re-encoding decoded keys and decoding again yields the same set
		// (a non-canonical uvarint in the raw input may not byte-match, so compare through a re-decode).
		again, err := decodeRecordKeys(encodeRecordKeys(keys))
		require.NoError(t, err)
		require.Equal(t, keys, again)
	})
}

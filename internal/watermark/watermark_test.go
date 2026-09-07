package watermark_test

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"hash/crc32"
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/internal/watermark"
	"github.com/oteldb/storage/signal"
)

func entries() []watermark.Entry {
	return []watermark.Entry{
		{ID: signal.SeriesID{Hi: 0, Lo: 0}, Max: math.MinInt64},
		{ID: signal.SeriesID{Hi: 1, Lo: 2}, Max: -1},
		{ID: signal.SeriesID{Hi: math.MaxUint64, Lo: math.MaxUint64}, Max: math.MaxInt64},
		{ID: signal.SeriesID{Hi: 7, Lo: 9}, Max: 1_700_000_000_000_000_000},
	}
}

func TestRoundTrip(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct {
		name string
		in   []watermark.Entry
	}{
		{"empty", []watermark.Entry{}},
		{"one", entries()[:1]},
		{"many", entries()},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := watermark.Decode(watermark.Encode(nil, tt.in))
			require.NoError(t, err)
			assert.Equal(t, tt.in, got)
		})
	}
}

func TestEncodeAppends(t *testing.T) {
	t.Parallel()

	dst := []byte("prefix")
	out := watermark.Encode(dst, entries())
	require.Equal(t, "prefix", string(out[:6]))

	got, err := watermark.Decode(out[6:])
	require.NoError(t, err)
	assert.Equal(t, entries(), got)
}

// TestGolden pins the on-disk framing: a format change that is not deliberate breaks here.
func TestGolden(t *testing.T) {
	t.Parallel()

	const want = "4f54574d02" +
		"0000000000000001000000000000000200000000000000ff" +
		"000000000000000300000000000000040000000000000000" +
		"488bbc00"

	assert.Equal(t, want, hex.EncodeToString(watermark.Encode(nil, []watermark.Entry{
		{ID: signal.SeriesID{Hi: 1, Lo: 2}, Max: 255},
		{ID: signal.SeriesID{Hi: 3, Lo: 4}, Max: 0},
	})))
}

// entryWidth mirrors the encoder's per-entry size; the corrupt cases cut the body by it.
const entryWidth = 24

// reseal appends a valid CRC32C to body, so a case exercises the field it targets rather than
// tripping the checksum first.
func reseal(body []byte) []byte {
	b := append([]byte(nil), body...)

	return binary.BigEndian.AppendUint32(b, crc32.Checksum(b, crc32.MakeTable(crc32.Castagnoli)))
}

func TestDecodeCorrupt(t *testing.T) {
	t.Parallel()

	good := watermark.Encode(nil, entries())
	body := good[:len(good)-4]

	badMagic := append([]byte(nil), body...)
	badMagic[0] ^= 0xFF

	badCRC := append([]byte(nil), good...)
	badCRC[len(badCRC)-1] ^= 0xFF

	for _, tt := range []struct {
		name string
		data []byte
	}{
		{"empty", nil},
		{"short", good[:7]},
		{"bad crc", badCRC},
		{"bad magic", reseal(badMagic)},
		{"unaligned body", reseal(body[:len(body)-8])},
		{"short count", reseal([]byte{0x4f, 0x54, 0x57, 0x4d, 0xFF})},
		{"count overflow", reseal(append([]byte{0x4f, 0x54, 0x57, 0x4d},
			0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x01))},
		{"count mismatch", reseal(body[:len(body)-entryWidth])},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, err := watermark.Decode(tt.data)
			require.ErrorIs(t, err, watermark.ErrCorrupt)
		})
	}
}

func FuzzDecode(f *testing.F) {
	f.Add(watermark.Encode(nil, nil))
	f.Add(watermark.Encode(nil, entries()))
	f.Add([]byte("not a sidecar at all"))

	f.Fuzz(func(t *testing.T, data []byte) {
		ents, err := watermark.Decode(data)
		if err != nil {
			return
		}

		// Whatever decodes must re-encode to the exact bytes it came from: the framing has no
		// slack, so decode∘encode is the identity on every accepted input.
		got := watermark.Encode(nil, ents)
		if !bytes.Equal(got, data) {
			t.Fatalf("re-encode mismatch: %x != %x", got, data)
		}

		again, err := watermark.Decode(got)
		if err != nil {
			t.Fatalf("re-decode: %v", err)
		}

		if len(again) != len(ents) {
			t.Fatalf("re-decode length %d != %d", len(again), len(ents))
		}
	})
}

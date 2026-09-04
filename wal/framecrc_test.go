package wal

import (
	"encoding/binary"
	"hash/crc32"
	"slices"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// replaySides replays data and returns the side payloads applied before it stopped.
func replaySides(data []byte) ([]string, error) {
	var got []string

	err := Replay(data, Handlers{
		OnSide: func(payload []byte) error { got = append(got, string(payload)); return nil },
	})

	return got, err
}

// threeFrames returns three side records and the offset of the second one's length varint.
func threeFrames() (data []byte, lenOff int) {
	data = appendFrame(nil, recordSide, []byte("rec1"))
	lenOff = len(data)
	data = appendFrame(data, recordSide, []byte("rec2"))
	data = appendFrame(data, recordSide, []byte("rec3"))

	return data, lenOff
}

// TestLengthPrefixInflatingBitFlip: one bit set in a frame's length varint makes it claim a body
// that runs past the end of the log. The completeness bound is reached before the checksum can be,
// so the only thing standing between that flip and a silent short read is Replay refusing to treat
// a record that does not fit as an end of stream.
func TestLengthPrefixInflatingBitFlip(t *testing.T) {
	t.Parallel()

	data, lenOff := threeFrames()
	data[lenOff] |= 0x80 // the varint continuation bit: the length swallows the type byte

	got, err := replaySides(data)
	require.ErrorIs(t, err, ErrCorrupt)
	assert.Equal(t, []string{"rec1"}, got, "the records before the flip are still applied")
}

// TestLengthPrefixDeflatingBitFlip: the same flip downward shortens the frame, so the checksum lands
// on the wrong bytes and fails.
func TestLengthPrefixDeflatingBitFlip(t *testing.T) {
	t.Parallel()

	data, lenOff := threeFrames()
	data[lenOff] ^= 0x01

	_, err := replaySides(data)
	require.ErrorIs(t, err, ErrCorrupt)
}

// TestLengthPrefixCoveredByCRC: an inflated length whose frame still fits inside the log is caught
// by the checksum itself, which now spans the length varint as well as the body.
func TestLengthPrefixCoveredByCRC(t *testing.T) {
	t.Parallel()

	// A body long enough that inflating the first frame's length keeps it inside the buffer.
	data := appendFrame(nil, recordSide, make([]byte, 200))
	data = appendFrame(data, recordSide, make([]byte, 200))

	data[0]++ // one more body byte than was written: the frame still fits, the CRC no longer matches

	_, err := replaySides(data)
	require.ErrorIs(t, err, ErrCorrupt)
}

// TestCorruptLengthCannotForgeARecord is what covering the length actually buys. A frame's payload
// can hold the checksum of a *prefix* of itself, so a corrupted length alone can carve a shorter,
// checksum-valid record out of a longer one — a record the writer never wrote, dispatched without
// complaint, leaving the reader parsing from the wrong offset. With the length inside the checksum
// the forged frame no longer verifies and nothing is dispatched.
func TestCorruptLengthCannotForgeARecord(t *testing.T) {
	t.Parallel()

	forged := []byte{recordSide, 0xAA, 0xBB} // the body a corrupted length would carve out
	crc := crc32.Checksum(forged, castagnoli)

	payload := slices.Concat(forged[1:], binary.BigEndian.AppendUint32(nil, crc), make([]byte, 10)) // the filler makes the real frame the longer one

	data := appendFrame(nil, recordSide, payload)
	require.Equal(t, byte(1+len(payload)), data[0], "the length is one varint byte")
	data[0] = byte(len(forged))

	got, err := replaySides(data)
	require.ErrorIs(t, err, ErrCorrupt)
	assert.Empty(t, got, "no record is dispatched from a frame whose length was corrupted")
}

// FuzzFrameBitFlip is [TestSingleBitFlipNeverReadsShort] over arbitrary payloads: a single flipped
// bit anywhere in a three-record log either leaves every record intact or is reported. A silent
// short read — fewer records, no error — is the failure this framing exists to prevent.
func FuzzFrameBitFlip(f *testing.F) {
	f.Add([]byte("rec1"), []byte("rec2"), []byte("rec3"), uint32(0))
	f.Add([]byte(""), []byte("x"), make([]byte, 300), uint32(7))

	f.Fuzz(func(t *testing.T, p1, p2, p3 []byte, pos uint32) {
		want := []string{string(p1), string(p2), string(p3)}

		data := appendFrame(nil, recordSide, p1)
		data = appendFrame(data, recordSide, p2)
		data = appendFrame(data, recordSide, p3)

		bit := pos % uint32(8*len(data))
		data[bit/8] ^= 1 << (bit % 8)

		got, err := replaySides(data)
		if err != nil {
			return
		}

		if len(got) != len(want) {
			t.Fatalf("bit %d: read %d records without an error, want %d", bit, len(got), len(want))
		}

		for i := range want {
			if got[i] != want[i] {
				t.Fatalf("bit %d: record %d = %q, want %q", bit, i, got[i], want[i])
			}
		}
	})
}

// TestSingleBitFlipNeverReadsShort is the property the framing owes a reader: no single bit flip
// anywhere in a log turns it into a *shorter, error-free* log. Either the records round-trip
// unchanged, or replay reports an error.
func TestSingleBitFlipNeverReadsShort(t *testing.T) {
	t.Parallel()

	want := []string{"rec1", "rec2", "rec3"}

	clean, _ := threeFrames()
	got, err := replaySides(clean)
	require.NoError(t, err)
	require.Equal(t, want, got)

	for i := range clean {
		for bit := range 8 {
			data, _ := threeFrames()
			data[i] ^= 1 << bit

			got, err := replaySides(data)
			if err != nil {
				continue
			}

			assert.Equal(t, want, got, "byte %d bit %d read short without an error", i, bit)
		}
	}
}

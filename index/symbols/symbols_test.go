package symbols

import (
	"bytes"
	"encoding/binary"
	"hash/crc32"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestInternDedupsAndAssignsSequentialIDs(t *testing.T) {
	t.Parallel()

	tbl := New()
	defer tbl.Release()

	a := tbl.Intern([]byte("job"))
	b := tbl.Intern([]byte("instance"))
	a2 := tbl.Intern([]byte("job"))

	assert.Equal(t, ID(0), a)
	assert.Equal(t, ID(1), b)
	assert.Equal(t, a, a2, "re-interning returns the same id")
	assert.Equal(t, 2, tbl.Len())
}

func TestInternOwnsACopy(t *testing.T) {
	t.Parallel()

	tbl := New()
	defer tbl.Release()

	buf := []byte("value")
	id := tbl.Intern(buf)
	buf[0] = 'X' // caller mutates its buffer after interning

	got, ok := tbl.Get(id)
	require.True(t, ok)
	assert.Equal(t, []byte("value"), got, "the table must own a copy")
}

func TestLookupAndGet(t *testing.T) {
	t.Parallel()

	tbl := New()
	defer tbl.Release()
	id := tbl.Intern([]byte("api"))

	got, ok := tbl.Lookup([]byte("api"))
	assert.True(t, ok)
	assert.Equal(t, id, got)

	_, ok = tbl.Lookup([]byte("missing"))
	assert.False(t, ok)

	_, ok = tbl.Get(ID(999))
	assert.False(t, ok, "out-of-range id")
}

func TestRoundTrip(t *testing.T) {
	t.Parallel()

	tbl := New()
	for _, s := range []string{"job", "instance", "", "a-much-longer-symbol-value"} {
		tbl.Intern([]byte(s))
	}

	got, err := Decode(tbl.Encode(nil))
	require.NoError(t, err)
	require.Equal(t, tbl.Len(), got.Len())

	for id := range ID(tbl.Len()) {
		want, _ := tbl.Get(id)
		have, ok := got.Get(id)
		require.True(t, ok)
		assert.True(t, bytes.Equal(want, have), "symbol %d: %q vs %q", id, want, have)
	}
	// And lookups work on the decoded table.
	id, ok := got.Lookup([]byte("instance"))
	require.True(t, ok)
	assert.Equal(t, ID(1), id)
}

func TestRoundTripEmpty(t *testing.T) {
	t.Parallel()

	got, err := Decode(New().Encode(nil))
	require.NoError(t, err)
	assert.Equal(t, 0, got.Len())
}

func TestEncodeAppends(t *testing.T) {
	t.Parallel()

	tbl := New()
	tbl.Intern([]byte("x"))
	prefix := []byte("PRE")
	out := tbl.Encode(append([]byte{}, prefix...))
	assert.Equal(t, prefix, out[:len(prefix)])

	got, err := Decode(out[len(prefix):])
	require.NoError(t, err)
	assert.Equal(t, 1, got.Len())
}

func TestDecodeRejectsCorruption(t *testing.T) {
	t.Parallel()

	tbl := New()
	tbl.Intern([]byte("a"))
	tbl.Intern([]byte("b"))
	enc := tbl.Encode(nil)

	bad := append([]byte(nil), enc...)
	bad[len(bad)-1] ^= 0xFF
	_, err := Decode(bad)
	require.ErrorIs(t, err, ErrCorrupt)

	_, err = Decode([]byte{0x00})
	require.ErrorIs(t, err, ErrCorrupt)
}

// TestDecodeTruncationSweep rebuilds a valid CRC over every prefix of a valid table so
// Decode passes the CRC check and exercises each inner field's truncation branch.
func TestDecodeTruncationSweep(t *testing.T) {
	t.Parallel()

	tbl := New()
	tbl.Intern([]byte("job"))
	tbl.Intern([]byte("instance"))
	full := tbl.Encode(nil)
	body := full[:len(full)-4]

	for n := range body {
		truncated := make([]byte, n+4)
		copy(truncated, body[:n])
		binary.BigEndian.PutUint32(truncated[n:], crc32.Checksum(body[:n], castagnoli))

		_, err := Decode(truncated)
		require.ErrorIsf(t, err, ErrCorrupt, "prefix len %d", n)
	}
}

func FuzzDecode(f *testing.F) {
	tbl := New()
	tbl.Intern([]byte("job"))
	tbl.Intern([]byte("instance"))
	f.Add(tbl.Encode(nil))
	f.Add([]byte{})

	f.Fuzz(func(t *testing.T, data []byte) {
		got, err := Decode(data)
		if err != nil {
			return
		}
		// Accepted ⇒ re-encode round-trips.
		again, err := Decode(got.Encode(nil))
		require.NoError(t, err)
		assert.Equal(t, got.Len(), again.Len())
	})
}

package block

import (
	"encoding/binary"
	"hash/crc32"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBuildMarks(t *testing.T) {
	t.Parallel()

	// 5 rows, granule size 2 → 3 granules: [0,1], [2,3], [4].
	ts := []int64{10, 20, 30, 40, 50}
	m := BuildMarks(ts, 2)

	require.Len(t, m.Granules, 3)
	assert.Equal(t, 2, m.GranuleSize)
	assert.Equal(t, Granule{FirstRow: 0, MinKey: 10, MaxKey: 20}, m.Granules[0])
	assert.Equal(t, Granule{FirstRow: 2, MinKey: 30, MaxKey: 40}, m.Granules[1])
	assert.Equal(t, Granule{FirstRow: 4, MinKey: 50, MaxKey: 50}, m.Granules[2])
}

func TestBuildMarksEmpty(t *testing.T) {
	t.Parallel()
	m := BuildMarks(nil, 8192)
	assert.Empty(t, m.Granules)
	assert.Equal(t, 8192, m.GranuleSize)
}

func TestBuildMarksUnsortedMinMax(t *testing.T) {
	t.Parallel()
	// Min/max computed by scan, not assuming sortedness.
	m := BuildMarks([]int64{30, 10, 20, 5}, 4)
	require.Len(t, m.Granules, 1)
	assert.Equal(t, int64(5), m.Granules[0].MinKey)
	assert.Equal(t, int64(30), m.Granules[0].MaxKey)
}

func TestBuildMarksPanicsOnZeroGranule(t *testing.T) {
	t.Parallel()
	assert.Panics(t, func() { BuildMarks([]int64{1}, 0) })
}

func TestMarksOverlapping(t *testing.T) {
	t.Parallel()

	m := BuildMarks([]int64{10, 20, 30, 40, 50, 60}, 2) // granules [10,20],[30,40],[50,60]

	assert.Len(t, m.Overlapping(35, 45), 1, "only [30,40] intersects [35,45]")
	assert.Equal(t, 2, m.Overlapping(35, 45)[0].FirstRow)

	assert.Len(t, m.Overlapping(15, 55), 3, "all granules touch [15,55]")
	assert.Empty(t, m.Overlapping(100, 200), "no granule above 60")
	assert.Empty(t, m.Overlapping(-10, 5), "no granule below 10")
	// Boundary: hi == granule MinKey still overlaps (inclusive).
	assert.Len(t, m.Overlapping(20, 30), 2)
}

func TestMarksRoundTrip(t *testing.T) {
	t.Parallel()

	m := BuildMarks([]int64{100, 250, 400, 401, 999, 1000, 1001}, 3)
	got, err := DecodeMarks(m.Encode(nil))
	require.NoError(t, err)
	assert.Equal(t, m, got)
}

func TestMarksRoundTripEmpty(t *testing.T) {
	t.Parallel()

	m := Marks{GranuleSize: 8192}
	got, err := DecodeMarks(m.Encode(nil))
	require.NoError(t, err)
	assert.Equal(t, 8192, got.GranuleSize)
	assert.Empty(t, got.Granules)
}

func TestMarksRejectsCorruption(t *testing.T) {
	t.Parallel()

	enc := BuildMarks([]int64{1, 2, 3, 4}, 2).Encode(nil)

	bad := append([]byte(nil), enc...)
	bad[len(bad)-1] ^= 0xFF // CRC
	_, err := DecodeMarks(bad)
	require.ErrorIs(t, err, ErrCorrupt)

	_, err = DecodeMarks([]byte{0x00})
	require.ErrorIs(t, err, ErrCorrupt)
}

func TestMarksTruncationSweep(t *testing.T) {
	t.Parallel()

	full := BuildMarks([]int64{5, 15, 25, 35, 45}, 2).Encode(nil)
	body := full[:len(full)-4]

	for n := range body {
		truncated := make([]byte, n+4)
		copy(truncated, body[:n])
		binary.BigEndian.PutUint32(truncated[n:], crc32.Checksum(body[:n], castagnoli))

		_, err := DecodeMarks(truncated)
		require.ErrorIsf(t, err, ErrCorrupt, "prefix len %d", n)
	}
}

func FuzzMarksDecode(f *testing.F) {
	f.Add(BuildMarks([]int64{1, 2, 3}, 2).Encode(nil))
	f.Add([]byte{})

	f.Fuzz(func(t *testing.T, data []byte) {
		m, err := DecodeMarks(data)
		if err != nil {
			return
		}
		got, err := DecodeMarks(m.Encode(nil))
		require.NoError(t, err)
		assert.Equal(t, m, got)
	})
}

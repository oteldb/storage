package block

import (
	"encoding/binary"
	"math"
	"slices"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/encoding/chunk"
)

func TestDirectoryUint(t *testing.T) {
	t.Parallel()
	for width := range uint8(9) {
		t.Run(strconv.Itoa(int(width)), func(t *testing.T) {
			t.Parallel()
			value := uint64(math.MaxUint64)
			if width < 8 {
				value = uint64(1)<<(8*width) - 1
			}
			data := make([]byte, 3*int(width)+7)
			appendDirectoryUint(data, 1, width, value)
			require.Equal(t, width, directoryWidth(value))
			require.EqualValues(t, 0, directoryUint(data, 0, width))
			require.Equal(t, value, directoryUint(data, 1, width))
			require.EqualValues(t, 0, directoryUint(data, 2, width))
			for i, n := range []uint64{value, value / 2, value / 3} {
				appendDirectoryUint(data, i, width, n)
			}
			for i, n := range []uint64{value, value / 2, value / 3} {
				require.Equal(t, n, directoryUint(data, i, width))
			}
		})
	}
}

type geometryEntry struct {
	rows     uint64
	min, max int64
}

func encodeGeometry(entries []geometryEntry, timeBounds bool) []byte {
	var out []byte
	var previous int64
	for _, e := range entries {
		out = binary.AppendUvarint(out, e.rows)
		if timeBounds {
			out = binary.AppendVarint(out, e.min-previous)
			out = binary.AppendVarint(out, e.max-e.min)
			previous = e.min
		}
	}
	return out
}

func requireChunkGeometry(t *testing.T, entries []geometryEntry, times bool) {
	t.Helper()
	raw := encodeGeometry(entries, times)
	chk := dirCheck{checked: true, maxBlockRows: chunk.MaxFloatGroupRows, timeBounds: times}
	for i, entry := range entries {
		chk.rows += int(entry.rows)
		if i == 0 {
			chk.minTime, chk.maxTime = entry.min, entry.max
		} else {
			chk.minTime, chk.maxTime = min(chk.minTime, entry.min), max(chk.maxTime, entry.max)
		}
	}
	chk.timeRange = times
	d := blockDir{granules: len(entries), frameRaw: make([]int32, len(entries))}
	c := dirCursor{object: raw}
	require.NoError(t, readChunkRows(&c, &d, chk))
	require.Equal(t, len(raw), c.pos)
	clear(raw)
	var row int
	decoder := Decoder{rows: chk.rows}
	decoder.streams.dir = d
	clone := decoder.Clone(t.Context())
	for i, entry := range entries {
		lo, hi := clone.BlockSpan(i)
		require.Equal(t, row, lo)
		row += int(entry.rows)
		require.Equal(t, row, hi)
		for _, at := range []int{lo, hi - 1} {
			block, err := clone.BlockForRow(at)
			require.NoError(t, err)
			require.Equal(t, i, block)
		}
		minT, maxT, ok := clone.BlockTimeBounds(i)
		require.Equal(t, times, ok)
		if times {
			require.Equal(t, entry.min, minT)
			require.Equal(t, entry.max, maxT)
		}
	}
	for i := range slices.Backward(entries) {
		lo, hi := clone.BlockSpan(i)
		got, err := clone.BlockForRow(hi - 1)
		require.NoError(t, err)
		require.Equal(t, i, got)
		require.EqualValues(t, entries[i].rows, hi-lo)
	}
	_, _, ok := clone.BlockTimeBounds(-1)
	require.False(t, ok)
	_, _, ok = clone.BlockTimeBounds(len(entries))
	require.False(t, ok)
	lo, hi := clone.BlockSpan(len(entries))
	require.Equal(t, chk.rows, lo)
	require.Equal(t, chk.rows, hi)
}

func chunkGeometryCases() []struct {
	name string
	rows []geometryEntry
} {
	return []struct {
		name string
		rows []geometryEntry
	}{
		{"one", []geometryEntry{{1, 17, 17}}},
		{"constant", []geometryEntry{{4, -100, -50}, {4, -100, -50}, {4, -100, -50}}},
		{"late-variation", []geometryEntry{{1, 7, 10}, {1, 7, 10}, {1, 7, 10}, {2, 8, 9}}},
		{"mixed", []geometryEntry{{1, 1700000000000000000, 1700000000000000000}, {2, 1700000001000000000, 1700000002000000000}, {17, -7, 91}}},
		{"signed-span", []geometryEntry{{1, math.MinInt64, math.MaxInt64}, {3, math.MaxInt64, math.MaxInt64}, {2, math.MinInt64, math.MinInt64}}},
	}
}

func TestPackedChunkGeometry(t *testing.T) {
	t.Parallel()
	for _, tc := range chunkGeometryCases() {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			requireChunkGeometry(t, tc.rows, true)
			requireChunkGeometry(t, tc.rows, false)
		})
	}
}

func TestChunkGeometryMemoryBound(t *testing.T) {
	t.Parallel()
	for _, mixed := range []bool{false, true} {
		t.Run(strconv.FormatBool(mixed), func(t *testing.T) {
			t.Parallel()
			raw, frameBytes, chk := chunkGeometryFixture(t, 65536, mixed)
			d, err := parseDetachedDir(raw, frameBytes, chk)
			require.NoError(t, err)
			desc := ColumnDesc{Kind: KindInt64, Blocked: true, Framed: true, Checked: true,
				MaxBlockRows: chk.maxBlockRows, TimeBounds: true, CompactDir: true, HasSizing: true,
				MinInt64: chk.minTime, MaxInt64: chk.maxTime, Sizing: *chk.sizing}
			bound := sizedInput(desc, int64(chk.rows))
			require.LessOrEqual(t, d.residentBytes(), bound.DirBytes)
			require.Less(t, d.residentBytes(), int64(36*d.granules))
		})
	}
}

func TestPackedChunkGeometryWideRows(t *testing.T) {
	t.Parallel()
	if strconv.IntSize < 64 {
		t.Skip("wide part row counts require a 64-bit platform")
	}
	entries := make([]geometryEntry, 8193)
	for i := range entries {
		entries[i] = geometryEntry{rows: chunk.MaxFloatGroupRows - uint64(i%2), min: int64(i), max: int64(i) + 17}
	}
	requireChunkGeometry(t, entries, true)
}

func TestPackedChunkGeometryValidation(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name string
		raw  []byte
		rows int
	}{
		{"zero", encodeGeometry([]geometryEntry{{0, 0, 1}}, true), 0},
		{"above-cap", encodeGeometry([]geometryEntry{{5, 0, 1}}, true), 0},
		{"row-total", encodeGeometry([]geometryEntry{{1, 0, 1}}, true), 2},
		{"bounds-order", encodeGeometry([]geometryEntry{{1, 1, 0}}, true), 1},
		{"below-manifest", encodeGeometry([]geometryEntry{{1, -1, 1}}, true), 1},
		{"above-manifest", encodeGeometry([]geometryEntry{{1, 0, 11}}, true), 1},
		{"truncated-rows", nil, 1},
		{"truncated-minimum", []byte{1}, 1},
		{"truncated-maximum", []byte{1, 0}, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			chk := dirCheck{checked: true, rows: tc.rows, maxBlockRows: 4, timeBounds: true, timeRange: true, minTime: 0, maxTime: 10}
			d := blockDir{granules: 1, frameRaw: []int32{1}}
			c := dirCursor{object: tc.raw}
			require.ErrorIs(t, readChunkRows(&c, &d, chk), ErrCorrupt)
		})
	}
}

func FuzzPackedChunkGeometry(f *testing.F) {
	for _, tc := range chunkGeometryCases() {
		f.Add(encodeGeometry(tc.rows, true), true)
		f.Add(encodeGeometry(tc.rows, false), false)
	}

	f.Fuzz(func(t *testing.T, data []byte, times bool) {
		if len(data) > 4096 {
			return
		}
		var entries []geometryEntry
		var previous int64
		for len(data) > 0 {
			rows, n := binary.Uvarint(data)
			if n <= 0 || rows == 0 || rows > chunk.MaxFloatGroupRows {
				return
			}
			data = data[n:]
			e := geometryEntry{rows: rows}
			if times {
				delta, n := binary.Varint(data)
				if n <= 0 {
					return
				}
				data = data[n:]
				span, n := binary.Varint(data)
				if n <= 0 {
					return
				}
				data = data[n:]
				e.min = previous + delta
				e.max = e.min + span
				if e.min > e.max {
					return
				}
				previous = e.min
			}
			entries = append(entries, e)
		}
		requireChunkGeometry(t, entries, times)
	})
}

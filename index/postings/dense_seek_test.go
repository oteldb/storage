package postings

import (
	"encoding/binary"
	"slices"
	"sort"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/signal"
)

// binaryDenseSeek retains the previous full-suffix search as an independent oracle.
func binaryDenseSeek(ids []uint32, current *int, target signal.SeriesID) bool {
	if target.Hi != 0 || target.Lo > uint64(^uint32(0)) {
		*current = len(ids)
		return false
	}
	*current = max(*current, 0)
	if *current >= len(ids) {
		return false
	}
	if uint64(ids[*current]) >= target.Lo {
		return true
	}
	base := *current
	*current = base + sort.Search(len(ids)-base, func(i int) bool { return uint64(ids[base+i]) >= target.Lo })
	return *current < len(ids)
}

func TestDenseSeekAgainstBinarySearch(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name string
		ids  []uint32
	}{
		{"empty", nil}, {"one", []uint32{7}}, {"bounds", []uint32{0, 1, 7, 1 << 31, ^uint32(0)}},
		{"nonzero start", []uint32{99, 100, 102, 107, 121, 155, 200}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			for _, target := range []signal.SeriesID{{}, {Lo: 1}, {Lo: 7}, {Lo: 98}, {Lo: 100}, {Lo: 101}, {Lo: 155}, {Lo: 1 << 31}, {Lo: uint64(^uint32(0))}, {Lo: 1 << 32}, {Hi: 1}} {
				got := &densePostings{ids: tc.ids, i: -1}
				old := -1
				require.Equal(t, binaryDenseSeek(tc.ids, &old, target), got.Seek(target))
				if old < len(tc.ids) {
					require.Equal(t, uint64(tc.ids[old]), got.At().Lo)
				}
				require.Equal(t, binaryDenseSeek(tc.ids, &old, signal.SeriesID{}), got.Seek(signal.SeriesID{}), "backward seek")
			}
		})
	}
	for _, n := range []int{2, 3, 4, 7, 8, 9, 15, 16, 17, 31, 32, 33, 63, 64, 65, 127, 128, 129, 255, 256, 257, 1024} {
		ids := make([]uint32, n)
		for i := range ids {
			ids[i] = uint32(i*3 + 17)
		}
		for _, stride := range []int{1, 2, 3, 7, 31, 128, 1024} {
			got := &densePostings{ids: ids, i: -1}
			old := -1
			for target := uint64(0); target <= uint64(ids[n-1])+1; target += uint64(stride) {
				want := binaryDenseSeek(ids, &old, signal.SeriesID{Lo: target})
				require.Equal(t, want, got.Seek(signal.SeriesID{Lo: target}))
				if want {
					require.Equal(t, uint64(ids[old]), got.At().Lo)
				}
			}
			require.False(t, got.Seek(signal.SeriesID{Hi: 1}))
			require.False(t, got.Seek(signal.SeriesID{}), "exhaustion is terminal")
		}
	}
}

func FuzzDenseSeekAgainstBinarySearch(f *testing.F) {
	for _, ids := range [][]uint32{nil, {7}, {0, 1, 7, 1 << 31, ^uint32(0)}, {99, 100, 102, 107, 121, 155, 200}} {
		var data, ops []byte
		for _, id := range ids {
			data = binary.LittleEndian.AppendUint32(data, id)
			ops = append(ops, 0)
			ops = binary.LittleEndian.AppendUint64(ops, uint64(id))
		}
		ops = append(ops, 4)
		ops = binary.LittleEndian.AppendUint64(ops, 1<<32)
		f.Add(data, ops)
	}
	f.Fuzz(func(t *testing.T, data, ops []byte) {
		data = data[:min(len(data), 16384)]
		ops = ops[:min(len(ops), 36864)]
		ids := make([]uint32, 0, len(data)/4)
		for len(data) >= 4 {
			ids = append(ids, binary.LittleEndian.Uint32(data))
			data = data[4:]
		}
		slices.Sort(ids)
		ids = slices.Compact(ids)
		got := &densePostings{ids: ids, i: -1}
		old := -1
		for len(ops) >= 9 {
			op := ops[0]
			target := signal.SeriesID{Lo: binary.LittleEndian.Uint64(ops[1:])}
			ops = ops[9:]
			var want, ok bool
			if op&1 != 0 {
				old++
				want = old < len(ids)
				ok = got.Next()
			} else {
				if op&63 != 4 {
					target.Lo = uint64(uint32(target.Lo))
				}
				if op&63 == 2 {
					target.Hi = 1
				}
				want = binaryDenseSeek(ids, &old, target)
				ok = got.Seek(target)
			}
			if ok != want {
				t.Fatalf("result differs: got %v, want %v", ok, want)
			}
			if ok && got.At().Lo != uint64(ids[old]) {
				t.Fatalf("position differs: got %v, want %v", got.At().Lo, ids[old])
			}
		}
	})
}

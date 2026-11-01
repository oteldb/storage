package postings

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/signal"
)

func TestDenseRangePostings(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		name string
		ids  []uint32
	}{
		{"empty", nil},
		{"zero", []uint32{0}},
		{"offset", []uint32{41, 42, 43, 44}},
		{"upper bound", []uint32{^uint32(0) - 2, ^uint32(0) - 1, ^uint32(0)}},
		{"gapped", []uint32{0, 1, 3, 7}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := fromDenseSlice(tt.ids)
			want := &densePostings{ids: tt.ids, i: -1}
			for _, target := range []signal.SeriesID{{}, {Lo: 2}, {Lo: 1}, {Lo: 42},
				{Lo: uint64(^uint32(0))}, {Lo: 1 << 32}, {Hi: 1}, {}} {
				ok := want.Seek(target)
				require.Equal(t, ok, got.Seek(target))
				if ok {
					require.Equal(t, want.At(), got.At())
				}
			}
			got = fromDenseSlice(tt.ids)
			for _, id := range tt.ids {
				require.True(t, got.Next())
				require.Equal(t, signal.SeriesID{Lo: uint64(id)}, got.At())
			}
			require.False(t, got.Next())
			require.False(t, got.Seek(signal.SeriesID{}), "exhaustion is terminal")
		})
	}
}

func FuzzDenseRangeIterator(f *testing.F) {
	for _, start := range []uint32{0, 41, ^uint32(0)} {
		for _, n := range []uint16{0, 1, 2, 1024} {
			f.Add(start, n, uint64(17))
		}
	}
	f.Fuzz(func(t *testing.T, start uint32, count uint16, seed uint64) {
		n := int(count % 4096)
		if n > 0 {
			start = uint32(uint64(start) % ((uint64(1) << 32) - uint64(n) + 1))
		}
		ids := make([]uint32, n)
		for i := range ids {
			ids[i] = start + uint32(i)
		}
		got := fromDenseSlice(ids)
		want := &densePostings{ids: ids, i: -1}
		for range 128 {
			seed = seed*6364136223846793005 + 1
			var actual, expected bool
			switch seed & 7 {
			case 0, 1:
				actual, expected = got.Next(), want.Next()
			case 2:
				actual, expected = got.Seek(signal.SeriesID{}), want.Seek(signal.SeriesID{})
			default:
				target := signal.SeriesID{Lo: uint64(start) + seed%uint64(n+3)}
				actual, expected = got.Seek(target), want.Seek(target)
			}
			require.Equal(t, expected, actual)
			if actual {
				require.Equal(t, want.At(), got.At())
			}
		}
	})
}

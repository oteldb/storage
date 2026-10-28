package postings

import (
	"slices"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/signal"
)

func TestDensePostings(t *testing.T) {
	t.Parallel()
	p := NewDense()
	for _, id := range []uint32{0, 1, 7, ^uint32(0)} {
		p.Add(id, 1, 2)
		p.Add(id, 3, 4)
	}
	got, err := ToSlice(Intersect(p.Get(1, 2), p.Get(3, 4)))
	require.NoError(t, err)
	assert.Equal(t, []signal.SeriesID{{Lo: 0}, {Lo: 1}, {Lo: 7}, {Lo: uint64(^uint32(0))}}, got)
	assert.Len(t, p.all, 4, "one all-set entry per consecutively registered series")
	assert.False(t, p.All().Seek(signal.SeriesID{Hi: 1}))
	assert.False(t, p.All().Seek(signal.SeriesID{Lo: 1 << 32}))
}

func FuzzDenseIntersection(f *testing.F) {
	f.Add([]byte{0, 1, 2, 3, 0, 2, 255})
	f.Add([]byte{})
	f.Fuzz(func(t *testing.T, data []byte) {
		p := NewDense()
		masks := make(map[uint32]byte)
		for i, v := range data {
			ref := uint32(v)
			value := uint32(i % 2)
			p.Add(ref, 0, value)
			masks[ref] |= 1 << value
		}
		var want []signal.SeriesID
		for id, mask := range masks {
			if mask == 3 {
				want = append(want, signal.SeriesID{Lo: uint64(id)})
			}
		}
		slices.SortFunc(want, signal.SeriesID.Compare)
		got, err := ToSlice(Intersect(p.Get(0, 0), p.Get(0, 1)))
		if err != nil || !slices.Equal(want, got) {
			t.Fatalf("intersection differs: got %v, want %v, error %v", got, want, err)
		}
	})
}

package memsize

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestOf(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		got  int64
		want int64
	}{
		{"byte", Of[byte](), 1},
		{"int64", Of[int64](), 8},
		{"slice header", Of[[]byte](), 24},
		{"string header", Of[string](), 16},
		{"map is one pointer", Of[map[int]int](), 8},
		{"array", Of[[2]int64](), 16},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.want, tt.got)
		})
	}
}

func TestSliceCountsCapacity(t *testing.T) {
	t.Parallel()

	s := make([]int64, 2, 10)

	assert.Equal(t, int64(80), Slice(s), "capacity, not length, is what stays resident")
	assert.Zero(t, Slice[int64](nil))
}

func TestMapEntry(t *testing.T) {
	t.Parallel()

	// (key + value + control byte) × the occupancy midpoint.
	assert.Equal(t, (8+8+1)*mapLoadNum/mapLoadDen, int(MapEntry[int64, int64]()))
	assert.Greater(t, MapEntry[[16]byte, int64](), MapEntry[int64, int64](),
		"a wider key costs more per entry")
}

func TestMap(t *testing.T) {
	t.Parallel()

	assert.Equal(t, int64(MapBase), Map(map[int64]int64{}), "an empty map still costs its header")
	assert.Equal(t, MapBase+2*MapEntry[int64, int64](), Map(map[int64]int64{1: 1, 2: 2}))
}

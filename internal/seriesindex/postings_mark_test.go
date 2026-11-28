package seriesindex

import (
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/require"
)

func testPostingChunk(first, step, count uint32) ([]byte, uint32) {
	data := binary.AppendUvarint(nil, uint64(count))
	data = binary.AppendUvarint(data, uint64(first))
	for i := uint32(1); i < count; i++ {
		data = binary.AppendUvarint(data, uint64(step))
	}
	return data, first + (count-1)*step
}

func TestMarkPostingChunk(t *testing.T) {
	t.Parallel()
	r := Index{count: 2048}
	for _, tc := range []struct {
		name               string
		first, step, count uint32
	}{
		{name: "one word", first: 41, step: 1, count: 22},
		{name: "cross-word", first: 60, step: 1, count: 8},
		{name: "whole words", first: 64, step: 1, count: 128},
		{name: "large unaligned", first: 65, step: 1, count: 1024},
		{name: "sparse", first: 5, step: 3, count: 101},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			data, last := testPostingChunk(tc.first, tc.step, tc.count)
			var set, clearBits, want [32]uint64
			for i := range clearBits {
				clearBits[i] = ^uint64(0)
			}
			for i := tc.first; i <= last; i += tc.step {
				want[i/64] |= uint64(1) << (i % 64)
			}
			require.NoError(t, r.mark(data, last, set[:], true))
			require.Equal(t, want, set)
			require.NoError(t, r.mark(data, last, clearBits[:], false))
			for i := range want {
				require.Equal(t, ^want[i], clearBits[i])
			}
		})
	}
	data, last := testPostingChunk(63, 1, 128)
	var words [32]uint64
	require.Error(t, r.mark(data, last-1, words[:], true))
	require.Error(t, r.mark(append(data, 1), last, words[:], true))
	data[len(data)-2], data[len(data)-1] = 0, 2
	require.Error(t, r.mark(data, last, words[:], true))
}

func FuzzMarkPostingChunk(f *testing.F) {
	for _, seed := range [][3]uint16{{63, 1, 1024}, {64, 1, 64}, {1, 3, 300}} {
		f.Add(seed[0], seed[1], seed[2])
	}
	f.Fuzz(func(t *testing.T, first, delta, count uint16) {
		start, step, n := uint32(first)%64, 1+uint32(delta)%3, 1+uint32(count)%1024
		data, last := testPostingChunk(start, step, n)
		r := Index{count: last + 1}
		bits := make([]uint64, last/64+1)
		require.NoError(t, r.mark(data, last, bits, true))
		for i := range last + 1 {
			match := bits[i/64]&(uint64(1)<<(i%64)) != 0
			require.Equal(t, i >= start && (i-start)%step == 0, match)
		}
	})
}

func BenchmarkMarkPostingChunk(b *testing.B) {
	for _, tc := range []struct {
		name string
		step uint32
	}{
		{name: "contiguous", step: 1},
		{name: "sparse", step: 3},
	} {
		b.Run(tc.name, func(b *testing.B) {
			data, last := testPostingChunk(55, tc.step, postingChunk)
			r := Index{count: last + 1}
			bits := make([]uint64, last/64+1)
			b.ReportAllocs()
			for b.Loop() {
				if err := r.mark(data, last, bits, true); err != nil {
					b.Fatal(err)
				}
			}
			b.SetBytes(postingChunk * 4)
		})
	}
}

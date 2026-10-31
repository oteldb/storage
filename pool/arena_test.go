package pool_test

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/pool"
)

func TestArenaAlloc(t *testing.T) {
	t.Parallel()
	for _, size := range []int{0, 3, 512, 513} {
		t.Run(fmt.Sprintf("size=%d", size), func(t *testing.T) {
			t.Parallel()
			var a pool.Arena[int]
			first := a.Alloc(size)
			require.Empty(t, first)
			require.Equal(t, size, cap(first))
			for i := range size {
				first = append(first, i)
			}
			second := a.Alloc(1024)
			for range 1024 {
				second = append(second, -1)
			}
			for i, value := range first {
				require.Equal(t, i, value, "growing the arena preserves earlier slices")
			}
			a.Reset()
			reused := a.Alloc(size)
			require.Empty(t, reused)
			require.Equal(t, size, cap(reused))
			if size > 0 {
				require.Same(t, &first[0], &reused[:size][0])
			}
		})
	}
}

func TestArenaConcat(t *testing.T) {
	t.Parallel()
	var a pool.Arena[byte]
	require.Empty(t, a.Concat())
	first := []byte("abc")
	second := []byte("def")
	joined := a.Concat(first, nil, second)
	require.Equal(t, []byte("abcdef"), joined)
	joined[0] = 'x'
	require.Equal(t, []byte("abc"), first)
	a.Reset()
	require.Equal(t, []byte("abc"), a.Concat(first))
}

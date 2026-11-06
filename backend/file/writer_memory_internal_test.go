package file

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestObjectWriterMemorySaturates(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		name     string
		dir, key int64
	}{
		{"oversized", math.MaxInt64 / 2, 0},
		{"overflow", math.MaxInt64, 1},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, int64(math.MaxInt64), objectWriterMemory(tt.dir, tt.key))
		})
	}
}

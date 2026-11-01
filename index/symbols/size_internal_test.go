package symbols

import (
	"encoding/binary"
	"math"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestUvarintLen(t *testing.T) {
	t.Parallel()
	for _, value := range []uint64{0, 1, 127, 128, 255, 16383, 16384, 1 << 32, math.MaxUint64} {
		var buffer [binary.MaxVarintLen64]byte
		require.Equal(t, binary.PutUvarint(buffer[:], value), uvarintLen(value), "value %d", value)
	}
}

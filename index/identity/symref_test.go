package identity

import (
	"encoding/binary"
	"math"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/index/symbols"
)

// TestSymRefRejectsOutOfRange checks that a symbol reference past the table errors instead of being
// truncated into a valid id: a ref of 2^32+k used to decode as symbol k, silently yielding a
// different identity than the object names.
func TestSymRefRejectsOutOfRange(t *testing.T) {
	t.Parallel()

	tab := symbols.New()
	defer tab.Release()

	tab.Intern([]byte("job"))
	tab.Intern([]byte("instance"))

	for _, ref := range []uint64{1 << 32, 1<<32 + 1, math.MaxUint64, 2} {
		d := &decoder{rest: binary.AppendUvarint(nil, ref), tab: tab}

		_, err := d.sym()
		require.ErrorIsf(t, err, ErrCorrupt, "ref %d", ref)
	}
}

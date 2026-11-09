package pool_test

import (
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/index/symbols"
	"github.com/oteldb/storage/pool"
)

//nolint:paralleltest // Replaces the process-local pool to make oversized reuse deterministic.
func TestSymbolTableDoesNotRetainEncoderMap(t *testing.T) {
	large := pool.NewByteIntMap()
	for i := range 16384 {
		large.Put(strconv.AppendInt(nil, int64(i), 10), i)
	}
	require.Greater(t, large.SizeBytes(), int64(1<<20))
	t.Cleanup(pool.ReplaceByteIntMapPoolForTest(func() *pool.ByteIntMap { return large }))

	table := symbols.New()
	defer table.Release()
	require.Less(t, table.SizeBytes(), int64(4<<10), "an empty long-lived table must not retain an encoder's oversized map")
	id := table.Intern([]byte("alive"))
	value, ok := table.Get(id)
	require.True(t, ok)
	require.Equal(t, []byte("alive"), value)
}

package identity

import (
	"encoding/binary"
	"hash/crc32"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/index/symbols"
	"github.com/oteldb/storage/signal"
)

func TestDecodeLegacyDuplicateEmptySymbols(t *testing.T) {
	t.Parallel()
	// Older map growth could assign several symbol IDs to the same empty payload.
	table := binary.BigEndian.AppendUint32(nil, 0x4f545359)
	table = append(table, 1, 2, 0, 0)
	table = binary.BigEndian.AppendUint32(table, crc32.Checksum(table, castagnoli))
	tab, err := symbols.Decode(table)
	require.NoError(t, err)
	defer tab.Release()
	id := (signal.Series{}).Hash()
	body := binary.AppendUvarint(nil, 1)
	body = binary.BigEndian.AppendUint64(body, id.Hi)
	body = binary.BigEndian.AppendUint64(body, id.Lo)
	body = append(body, 0, 0, 1, 0, 1, 0, 0)
	data := appendObject(nil, tab, body)
	count := 0
	require.NoError(t, Decode(data, func(got signal.SeriesID, s signal.Series) error {
		count++
		require.Equal(t, id, got)
		require.Equal(t, got, s.Hash())
		require.True(t, s.Equal(signal.Series{}))
		return nil
	}))
	require.Equal(t, 1, count)
}

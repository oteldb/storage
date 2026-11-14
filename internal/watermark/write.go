package watermark

import (
	"context"
	"encoding/binary"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/internal/sidecar"
)

// WriteEntries streams the existing checked format through bounded scratch.
func WriteEntries(ctx context.Context, b backend.Backend, key string, count int, entry func(int) Entry) error {
	var scratch [16]byte
	header := binary.BigEndian.AppendUint32(scratch[:0], magic)
	header = binary.AppendUvarint(header, uint64(count))
	return sidecar.Write(ctx, b, key, header, count, entryWidth, func(dst []byte, i int) []byte {
		e := entry(i)
		dst = binary.BigEndian.AppendUint64(dst, e.ID.Hi)
		dst = binary.BigEndian.AppendUint64(dst, e.ID.Lo)
		return binary.BigEndian.AppendUint64(dst, uint64(e.Max))
	})
}

package recordengine

import (
	"bytes"
	"encoding/binary"

	"github.com/oteldb/storage/wal"
)

// EncodeWAL frames a batch as a replication/WAL payload — a stream-identity record followed by the
// batch's records — exactly the form [Engine.ApplyPrimary] and [Engine.ApplyReplicated] replay. The
// cluster write path builds a tenant's payload by concatenating EncodeWAL over its streams and
// routing it to the ring primary.
func EncodeWAL(b *Batch) []byte {
	var buf bytes.Buffer

	w := wal.NewWriter(&buf)
	_ = w.WriteSeries(b.Stream, b.Identity())
	_ = w.WriteRecords(b.Stream, encodeBatchRecs(b))

	if len(b.Side) > 0 {
		_ = w.WriteSide(b.Side)
	}

	return buf.Bytes()
}

// encodeBatchRecs encodes a batch's records in the same layout [decodeRecs] reads: a uvarint count,
// then per record the timestamp (varint), the int columns (varint each, schema order), and the byte
// columns (length-prefixed, schema order).
func encodeBatchRecs(b *Batch) []byte {
	dst := binary.AppendUvarint(nil, uint64(b.Len()))
	for i := range b.Ts {
		dst = binary.AppendVarint(dst, b.Ts[i])
		for k := range b.Ints {
			dst = binary.AppendVarint(dst, b.Ints[k][i])
		}

		for k := range b.Bytes {
			dst = binary.AppendUvarint(dst, uint64(len(b.Bytes[k][i])))
			dst = append(dst, b.Bytes[k][i]...)
		}
	}

	return dst
}

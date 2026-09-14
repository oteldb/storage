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
		dst = appendBatchRec(dst, b, i)
	}

	return dst
}

// appendBatchRec appends record i of b in the per-record layout of [encodeBatchRecs].
func appendBatchRec(dst []byte, b *Batch, i int) []byte {
	dst = binary.AppendVarint(dst, b.Ts[i])
	for k := range b.Ints {
		dst = binary.AppendVarint(dst, b.Ints[k][i])
	}

	for k := range b.Bytes {
		dst = binary.AppendUvarint(dst, uint64(len(b.Bytes[k][i])))
		dst = append(dst, b.Bytes[k][i]...)
	}

	return dst
}

// recsCountRoom is the room [openRecs] reserves for a payload's record count, which is known only once
// every record has been encoded.
const recsCountRoom = binary.MaxVarintLen64

// openRecs starts an [encodeBatchRecs]-layout payload in dst, to be filled by [appendBatchRec] and
// finished by [sealRecs].
func openRecs(dst []byte) []byte {
	return append(dst[:0], make([]byte, recsCountRoom)...)
}

// sealRecs writes the record count into the room [openRecs] reserved and returns the payload, which
// is byte-identical to encoding the same records with [encodeBatchRecs].
func sealRecs(buf []byte, count int) []byte {
	var n [binary.MaxVarintLen64]byte

	w := binary.PutUvarint(n[:], uint64(count))
	start := recsCountRoom - w
	copy(buf[start:recsCountRoom], n[:w])

	return buf[start:]
}

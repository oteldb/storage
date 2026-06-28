package recordengine

import (
	"context"
	"encoding/binary"
	"hash/crc32"
	"sort"

	"github.com/go-faster/errors"

	"github.com/oteldb/storage/backend"
)

// A part's distinct per-record attribute keys are persisted next to its column blooms as a small
// footer sidecar at "{prefix}/keys.bin", so [Engine.Keys] can enumerate record-attribute keys
// without scanning the part's attrs column. The set is bounded by the stream schema (keys, not
// values), so the sidecar is tiny. Stream-identity keys are NOT stored here — they are recovered
// from the authoritative series index.
func recordKeysKey(prefix string) string { return prefix + "/keys.bin" }

const (
	recordKeysMagic   uint32 = 0x4F544B59 // "OTKY"
	recordKeysVersion uint32 = 1
)

// ErrCorruptKeys is returned when a serialized record-keys footer fails to parse.
var ErrCorruptKeys = errors.New("recordengine: corrupt record-keys footer")

var recordKeysCRC = crc32.MakeTable(crc32.Castagnoli)

// distinctRecordKeys decodes the serialized-attributes blobs of a part's attrs column (if any) and
// returns their distinct keys, sorted, as owned copies. Empty when the schema has no attrs column.
func distinctRecordKeys(schema *Schema, cols *recordCols) [][]byte {
	k, ok := schema.attrsByteCol()
	if !ok {
		return nil
	}

	seen := make(map[string]struct{})
	bc := &cols.bytes[k]
	for i := range bc.rows() {
		forEachAttrKey(bc.at(i), func(key []byte) { seen[string(key)] = struct{}{} })
	}

	if len(seen) == 0 {
		return nil
	}

	out := make([][]byte, 0, len(seen))
	for key := range seen {
		out = append(out, []byte(key))
	}

	sort.Slice(out, func(i, j int) bool { return string(out[i]) < string(out[j]) })

	return out
}

// encodeRecordKeys serializes a part's distinct record-attribute keys. Layout:
//
//	[u32 magic][uvarint version][uvarint count]
//	  per key in sorted order: [uvarint len][bytes]
//	[u32 CRC32C]
func encodeRecordKeys(keys [][]byte) []byte {
	dst := binary.BigEndian.AppendUint32(nil, recordKeysMagic)
	dst = binary.AppendUvarint(dst, uint64(recordKeysVersion))
	dst = binary.AppendUvarint(dst, uint64(len(keys)))

	for _, key := range keys {
		dst = binary.AppendUvarint(dst, uint64(len(key)))
		dst = append(dst, key...)
	}

	crc := crc32.Checksum(dst, recordKeysCRC)

	return binary.BigEndian.AppendUint32(dst, crc)
}

// decodeRecordKeys parses [encodeRecordKeys] output. It verifies the CRC and bounds-checks every
// field, returning an [ErrCorruptKeys]-wrapping error on malformed input; it never panics. Returned
// keys are owned copies (do not alias src).
func decodeRecordKeys(src []byte) ([][]byte, error) {
	if len(src) < 8 {
		return nil, errors.Wrap(ErrCorruptKeys, "too short")
	}

	body := src[:len(src)-4]
	if crc32.Checksum(body, recordKeysCRC) != binary.BigEndian.Uint32(src[len(src)-4:]) {
		return nil, errors.Wrap(ErrCorruptKeys, "CRC mismatch")
	}

	if binary.BigEndian.Uint32(body) != recordKeysMagic {
		return nil, errors.Wrap(ErrCorruptKeys, "bad magic")
	}

	body = body[4:]

	version, n := binary.Uvarint(body)
	if n <= 0 || version != uint64(recordKeysVersion) {
		return nil, errors.Wrap(ErrCorruptKeys, "version")
	}

	body = body[n:]

	count, n := binary.Uvarint(body)
	if n <= 0 || count > uint64(len(body)) {
		return nil, errors.Wrap(ErrCorruptKeys, "count")
	}

	body = body[n:]

	out := make([][]byte, 0, count)
	for range count {
		l, n := binary.Uvarint(body)
		if n <= 0 || l > uint64(len(body)-n) {
			return nil, errors.Wrap(ErrCorruptKeys, "key length")
		}

		body = body[n:]
		out = append(out, append([]byte(nil), body[:l]...))
		body = body[l:]
	}

	return out, nil
}

// writeRecordKeys persists the part's distinct record-attribute keys as the "{prefix}/keys.bin"
// footer sidecar. No-op when the schema has no attrs column or the part holds no record attributes.
func writeRecordKeys(ctx context.Context, b backend.Backend, schema *Schema, prefix string, cols *recordCols) error {
	keys := distinctRecordKeys(schema, cols)
	if len(keys) == 0 {
		return nil
	}

	if err := b.Write(ctx, recordKeysKey(prefix), encodeRecordKeys(keys)); err != nil {
		return errors.Wrap(err, "write record-keys footer")
	}

	return nil
}

// loadRecordKeys reads the part's record-keys footer. A missing sidecar yields nil (an older part,
// or one with no record attributes) — the part simply contributes no record-scope keys.
func loadRecordKeys(ctx context.Context, b backend.Backend, prefix string) ([][]byte, error) {
	data, err := b.Read(ctx, recordKeysKey(prefix))
	if err != nil {
		if errors.Is(err, backend.ErrNotExist) {
			return nil, nil
		}

		return nil, errors.Wrap(err, "read record-keys footer")
	}

	keys, err := decodeRecordKeys(data)
	if err != nil {
		return nil, err
	}

	return keys, nil
}

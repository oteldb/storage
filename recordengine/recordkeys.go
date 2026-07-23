package recordengine

import (
	"context"
	"encoding/binary"
	"hash/crc32"

	"github.com/go-faster/errors"

	"github.com/oteldb/storage/backend"
)

// A part's distinct per-record attribute keys are persisted next to its column blooms as a small
// footer sidecar at "{prefix}/keys.bin", so [Engine.Keys] can enumerate them without scanning the
// part's attrs columns. The set is bounded by the stream schema (keys, not values), so the sidecar
// is tiny. Each key carries the [KeyScope] of the column it came from, so a schema that stores a
// stream's resource attributes per record reports them as resource keys rather than record ones.
// Stream-identity keys are NOT stored here — they are recovered from the authoritative series index.
func recordKeysKey(prefix string) string { return prefix + "/keys.bin" }

const (
	recordKeysMagic uint32 = 0x4F544B59 // "OTKY"
	// recordKeysVersion 2 appends a scope byte per key; version 1 (scope-less) is still read, its
	// keys taken as [KeyScopeRecord] — the only scope that version could store.
	recordKeysVersion   uint32 = 2
	recordKeysVersionV1 uint32 = 1
)

// ErrCorruptKeys is returned when a serialized record-keys footer fails to parse.
var ErrCorruptKeys = errors.New("recordengine: corrupt record-keys footer")

var recordKeysCRC = crc32.MakeTable(crc32.Castagnoli)

// distinctRecordKeys decodes the serialized-attributes blobs of a part's [BloomAttrs] columns and
// returns their distinct keys, sorted, as owned copies, each tagged with its column's [KeyScope] (a
// key found in several columns carries the union). Empty when the schema has no attrs column.
func distinctRecordKeys(schema *Schema, cols *recordCols) []KeyInfo {
	seen := make(map[string]KeyScope)

	for _, k := range schema.attrsByteCols() {
		sc := schema.keyScope(k)
		bc := &cols.bytes[k]

		for i := range bc.rows() {
			forEachAttrKey(bc.at(i), func(key []byte) { seen[string(key)] |= sc })
		}
	}

	return keyInfoSlice(seen)
}

// encodeRecordKeys serializes a part's distinct record-attribute keys. Layout:
//
//	[u32 magic][uvarint version][uvarint count]
//	  per key in sorted order: [uvarint len][bytes][u8 scope]
//	[u32 CRC32C]
func encodeRecordKeys(keys []KeyInfo) []byte {
	dst := binary.BigEndian.AppendUint32(nil, recordKeysMagic)
	dst = binary.AppendUvarint(dst, uint64(recordKeysVersion))
	dst = binary.AppendUvarint(dst, uint64(len(keys)))

	for _, k := range keys {
		dst = binary.AppendUvarint(dst, uint64(len(k.Key)))
		dst = append(dst, k.Key...)
		dst = append(dst, byte(k.Scope))
	}

	crc := crc32.Checksum(dst, recordKeysCRC)

	return binary.BigEndian.AppendUint32(dst, crc)
}

// decodeRecordKeys parses [encodeRecordKeys] output. It verifies the CRC and bounds-checks every
// field, returning an [ErrCorruptKeys]-wrapping error on malformed input; it never panics. Returned
// keys are owned copies (do not alias src).
func decodeRecordKeys(src []byte) ([]KeyInfo, error) {
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
	if n <= 0 || (version != uint64(recordKeysVersion) && version != uint64(recordKeysVersionV1)) {
		return nil, errors.Wrap(ErrCorruptKeys, "version")
	}

	scoped := version == uint64(recordKeysVersion)
	body = body[n:]

	count, n := binary.Uvarint(body)
	if n <= 0 || count > uint64(len(body)) {
		return nil, errors.Wrap(ErrCorruptKeys, "count")
	}

	body = body[n:]

	out := make([]KeyInfo, 0, count)
	for range count {
		l, n := binary.Uvarint(body)
		if n <= 0 || l > uint64(len(body)-n) {
			return nil, errors.Wrap(ErrCorruptKeys, "key length")
		}

		body = body[n:]
		info := KeyInfo{Key: append([]byte(nil), body[:l]...), Scope: KeyScopeRecord}
		body = body[l:]

		if scoped {
			if len(body) == 0 {
				return nil, errors.Wrap(ErrCorruptKeys, "key scope")
			}

			info.Scope = KeyScope(body[0])
			body = body[1:]
		}

		out = append(out, info)
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
func loadRecordKeys(ctx context.Context, b backend.Backend, prefix string) ([]KeyInfo, error) {
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

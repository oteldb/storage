package partsync

import (
	"encoding/binary"
	"testing"
)

// FuzzDecodeKeyList exercises the list-framing decoder against arbitrary input: it must never
// panic or over-allocate, and a well-formed encoding must round-trip.
func FuzzDecodeKeyList(f *testing.F) {
	// Seed with a valid two-key frame and a few malformed ones.
	valid := binary.AppendUvarint(nil, 2)
	valid = binary.AppendUvarint(valid, 3)
	valid = append(valid, "a/1"...)
	valid = binary.AppendUvarint(valid, 3)
	valid = append(valid, "b/2"...)
	f.Add(valid)
	f.Add([]byte{})
	f.Add([]byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x01}) // huge count, no data
	f.Add(binary.AppendUvarint(nil, 1))                                       // count without a key

	f.Fuzz(func(t *testing.T, data []byte) {
		keys, err := decodeKeyList(data)
		if err != nil {
			return
		}

		// Successful decode must re-encode to a decodable frame with the same keys.
		buf := binary.AppendUvarint(nil, uint64(len(keys)))
		for _, k := range keys {
			buf = binary.AppendUvarint(buf, uint64(len(k)))
			buf = append(buf, k...)
		}

		again, err := decodeKeyList(buf)
		if err != nil {
			t.Fatalf("re-decode failed: %v", err)
		}

		if len(again) != len(keys) {
			t.Fatalf("round-trip length: got %d want %d", len(again), len(keys))
		}

		for i := range keys {
			if again[i] != keys[i] {
				t.Fatalf("round-trip key %d: got %q want %q", i, again[i], keys[i])
			}
		}
	})
}

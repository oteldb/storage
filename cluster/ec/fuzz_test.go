package ec

import (
	"bytes"
	"testing"
)

// FuzzDecodeMeta exercises the sidecar decoder against arbitrary input: it must never panic or
// over-allocate, and any accepted payload must re-encode to an equal decodable payload.
func FuzzDecodeMeta(f *testing.F) {
	valid := (&Meta{
		Scheme:  Scheme{Data: 2, Parity: 1},
		Objects: []ObjectMeta{{Name: "c/0", Size: 42, Checksums: []uint64{1, 2, 3}}},
	}).AppendBinary(nil)
	f.Add(valid)
	f.Add([]byte{})
	f.Add(valid[:len(valid)-1])
	f.Add([]byte{metaVersion, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x01})

	f.Fuzz(func(t *testing.T, data []byte) {
		m, err := DecodeMeta(data)
		if err != nil {
			return
		}

		again, err := DecodeMeta(m.AppendBinary(nil))
		if err != nil {
			t.Fatalf("re-decode failed: %v", err)
		}

		if len(again.Objects) != len(m.Objects) || again.Scheme != m.Scheme {
			t.Fatalf("round-trip mismatch: %+v vs %+v", again, m)
		}
	})
}

// FuzzEncodeReconstruct drives the codec with arbitrary payloads and loss patterns: encoding
// must round-trip through reconstruction whenever at most Parity shards are lost.
func FuzzEncodeReconstruct(f *testing.F) {
	f.Add([]byte("hello world"), uint8(4), uint8(2), uint16(0b11))
	f.Add([]byte{}, uint8(1), uint8(1), uint16(0))

	f.Fuzz(func(t *testing.T, data []byte, k, m uint8, lossMask uint16) {
		s := Scheme{Data: int(k%16) + 1, Parity: int(m%8) + 1}

		shards, err := Encode(s, data)
		if err != nil {
			t.Fatalf("encode: %v", err)
		}

		lost := 0

		for i := range shards {
			if lost < s.Parity && lossMask&(1<<uint(i%16)) != 0 {
				shards[i] = nil
				lost++
			}
		}

		if err := Reconstruct(s, shards); err != nil {
			t.Fatalf("reconstruct with %d <= %d losses: %v", lost, s.Parity, err)
		}

		got, err := Join(s, shards, int64(len(data)))
		if err != nil {
			t.Fatalf("join: %v", err)
		}

		if !bytes.Equal(got, data) {
			t.Fatal("identity violated")
		}
	})
}

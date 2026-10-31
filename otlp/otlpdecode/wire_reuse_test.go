package otlpdecode_test

import (
	"encoding/binary"
	"fmt"
	"slices"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/otlp/otlpdecode"
)

func TestMalformedMessageReuse(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name  string
		paths [][]uint32
		make  func() (convert func([]byte) error, empty func() bool)
	}{
		{name: "logs", paths: [][]uint32{nil, {1}, {1, 1}, {1, 2}, {1, 2, 1}, {1, 2, 2},
			{1, 1, 1}, {1, 1, 1, 2}, {1, 2, 2, 5}, {1, 2, 2, 6}, {1, 2, 2, 6, 2},
			{1, 2, 2, 5, 5}, {1, 2, 2, 5, 6}, {1, 2, 2, 5, 6, 1}}, make: func() (func([]byte) error, func() bool) {
			var c otlpdecode.LogsConverter
			return func(raw []byte) error { _, _, err := c.Convert(raw); return err }, func() bool {
				batch, dropped, err := c.Convert(nil)
				return err == nil && dropped == 0 && len(batch.Resources) == 0
			}
		}},
		{name: "traces", paths: [][]uint32{nil, {1}, {1, 1}, {1, 2}, {1, 2, 1}, {1, 2, 2},
			{1, 2, 2, 9}, {1, 2, 2, 9, 2}, {1, 2, 2, 11}, {1, 2, 2, 13}, {1, 2, 2, 15}}, make: func() (func([]byte) error, func() bool) {
			var c otlpdecode.TracesConverter
			return func(raw []byte) error { _, err := c.Convert(raw); return err }, func() bool {
				batch, err := c.Convert(nil)
				return err == nil && len(batch.Resources) == 0
			}
		}},
		{name: "metrics", paths: [][]uint32{nil, {1}, {1, 1}, {1, 2}, {1, 2, 1}, {1, 2, 2},
			{1, 2, 2, 5}, {1, 2, 2, 7}, {1, 2, 2, 9}, {1, 2, 2, 10}, {1, 2, 2, 11},
			{1, 2, 2, 5, 1}, {1, 2, 2, 5, 1, 5}, {1, 2, 2, 5, 1, 5, 7},
			{1, 2, 2, 9, 1}, {1, 2, 2, 10, 1}, {1, 2, 2, 10, 1, 8}, {1, 2, 2, 10, 1, 9},
			{1, 2, 2, 11, 1}, {1, 2, 2, 11, 1, 6}}, make: func() (func([]byte) error, func() bool) {
			var c otlpdecode.MetricsConverter
			return func(raw []byte) error { _, _, err := c.Convert(raw); return err }, func() bool {
				batch, dropped, err := c.Convert(nil)
				return err == nil && dropped == (otlpdecode.Dropped{}) && len(batch.Resources) == 0
			}
		}},
		{name: "profiles", paths: [][]uint32{nil, {2}, {2, 1}, {2, 2}, {2, 2, 3}, {2, 3}, {2, 4},
			{2, 6}, {2, 6, 2}, {2, 7}, {1}, {1, 1}, {1, 2}, {1, 2, 1}, {1, 2, 2},
			{1, 2, 2, 1}, {1, 2, 2, 2}, {1, 2, 2, 5}}, make: func() (func([]byte) error, func() bool) {
			var c otlpdecode.ProfilesConverter
			return func(raw []byte) error { _, err := c.Convert(raw); return err }, func() bool {
				batch, err := c.Convert(nil)
				return err == nil && len(batch.Resources) == 0
			}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			convert, empty := tc.make()
			for _, path := range tc.paths {
				t.Run(fmt.Sprint(path), func(t *testing.T) {
					require.Error(t, convert(wrapWireMessage(tc.name, path, []byte{0x80})), "truncated nested framing is rejected")
					for field := range uint32(20) {
						for _, payload := range wireFields(field) {
							_ = convert(wrapWireMessage(tc.name, path, payload))
							require.True(t, empty(), "a rejected or unknown field must not poison the next request")
						}
					}
				})
			}
		})
	}
}

func wrapWireMessage(sig string, path []uint32, raw []byte) []byte {
	for i, field := range slices.Backward(path) {
		// Make the parent record representable so lazy conversion reaches its nested fields.
		if sig == "logs" && i == 2 {
			prefix := binary.LittleEndian.AppendUint64([]byte{1<<3 | 1}, 100)
			raw = append(prefix, raw...)
		}
		if sig == "metrics" && len(path) >= 5 && path[3] == 5 && i == 4 {
			prefix := binary.LittleEndian.AppendUint64([]byte{4<<3 | 1}, 0)
			raw = append(prefix, raw...)
		}
		if sig == "metrics" && len(path) >= 6 && path[3] == 5 && path[5] == 5 && i == 5 {
			prefix := binary.LittleEndian.AppendUint64([]byte{3<<3 | 1}, 0)
			raw = append(prefix, raw...)
		}
		out := binary.AppendUvarint(nil, uint64(field<<3|2))
		out = binary.AppendUvarint(out, uint64(len(raw)))
		raw = append(out, raw...)
	}
	return raw
}

func wireFields(field uint32) [][]byte {
	var out [][]byte
	for _, kind := range []uint32{0, 1, 2, 5} {
		tag := binary.AppendUvarint(nil, uint64(field<<3|kind))
		out = append(out, append([]byte(nil), tag...))
		switch kind {
		case 0:
			out = append(out, append(tag, 1), append(append([]byte(nil), tag...), 0x80))
		case 1:
			out = append(out, append(tag, make([]byte, 8)...))
		case 2:
			out = append(out, append(tag, 0), append(append([]byte(nil), tag...), 1, 0x80))
		case 5:
			out = append(out, append(tag, make([]byte, 4)...))
		}
	}
	return out
}

package watermark

import (
	"encoding/binary"
	"hash/crc32"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/backend/backendtest"
	"github.com/oteldb/storage/signal"
)

func TestReader(t *testing.T) {
	t.Parallel()
	for _, count := range []int{0, 1, 4097} {
		t.Run(strconv.Itoa(count), func(t *testing.T) {
			t.Parallel()
			entries := make([]Entry, count)
			for i := range entries {
				entries[i] = Entry{ID: signal.SeriesID{Hi: uint64(i + 1)}, Max: int64(i) - 7}
			}
			b := backendtest.NewByteCounter(backend.Memory())
			require.NoError(t, b.Write(t.Context(), "smax", Encode(nil, entries)))
			r, err := OpenReader(t.Context(), b, "smax", count)
			require.NoError(t, err)
			b.Reset()
			for _, i := range []int{0, count / 2, count - 1} {
				if i < 0 || i >= count {
					continue
				}
				value, ok, err := r.Lookup(t.Context(), entries[i].ID)
				require.NoError(t, err)
				require.True(t, ok)
				require.Equal(t, entries[i].Max, value)
			}
			_, ok, err := r.Lookup(t.Context(), signal.SeriesID{Hi: uint64(count + 10)})
			require.NoError(t, err)
			require.False(t, ok)
			require.LessOrEqual(t, b.Bytes(), int64(4*24*14), "lookups must not re-read the complete object")
		})
	}
}

func TestReaderRejectsCorruption(t *testing.T) {
	t.Parallel()
	entries := []Entry{{ID: signal.SeriesID{Lo: 1}, Max: 3}, {ID: signal.SeriesID{Lo: 2}, Max: 4}}
	for _, mode := range []string{"short", "checksum", "header", "count", "unsorted"} {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			raw := Encode(nil, entries)
			switch mode {
			case "short":
				raw = raw[:3]
			case "checksum":
				raw[len(raw)-1] ^= 1
			case "header":
				raw[0] ^= 1
			case "count":
				raw[4]++
			case "unsorted":
				copy(raw[5+entryWidth:], raw[5:5+entryWidth])
				binary.BigEndian.PutUint32(raw[len(raw)-4:], crc32.Checksum(raw[:len(raw)-4], crcTable))
			}
			b := backend.Memory()
			require.NoError(t, b.Write(t.Context(), "smax", raw))
			_, err := OpenReader(t.Context(), b, "smax", len(entries))
			require.ErrorIs(t, err, ErrCorrupt)
		})
	}
}

func FuzzReader(f *testing.F) {
	f.Add(Encode(nil, nil), 0)
	f.Add(Encode(nil, []Entry{{ID: signal.SeriesID{Lo: 1}, Max: 3}}), 1)
	f.Add([]byte("bad"), 0)
	f.Fuzz(func(t *testing.T, raw []byte, count int) {
		b := backend.Memory()
		if err := b.Write(t.Context(), "smax", raw); err != nil {
			t.Fatal(err)
		}
		r, err := OpenReader(t.Context(), b, "smax", count)
		if err != nil {
			return
		}
		expected, err := Decode(raw)
		if err != nil {
			t.Fatal(err)
		}
		for _, e := range expected {
			value, ok, err := r.Lookup(t.Context(), e.ID)
			if err != nil || !ok || value != e.Max {
				t.Fatalf("lookup %v: %d, %v, %v", e.ID, value, ok, err)
			}
		}
	})
}

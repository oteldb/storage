package backend_test

import (
	"bytes"
	"context"
	"math"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/backend/file"
)

type legacyReadBackend struct{ backend.Backend }

func TestReadAtAppend(t *testing.T) {
	t.Parallel()
	for _, kind := range []string{"memory", "legacy", "file", "cached-file"} {
		t.Run(kind, func(t *testing.T) {
			t.Parallel()
			b := backend.Memory()
			if kind == "file" || kind == "cached-file" {
				f, err := file.New(t.TempDir())
				require.NoError(t, err)
				b = f
			}
			if kind == "legacy" {
				b = legacyReadBackend{b}
			}
			if kind == "cached-file" {
				b = backend.Cached(b, 1<<20)
			}
			require.NoError(t, b.Write(t.Context(), "data", []byte("abcdef")))
			for _, tt := range []struct {
				name   string
				off, n int64
				want   string
				bad    bool
			}{
				{"middle", 1, 3, "bcd", false},
				{"clamped", 4, 10, "ef", false},
				{"empty", 0, 0, "", false},
				{"end", 6, 1, "", false},
				{"past-end", math.MaxInt64, math.MaxInt64, "", false},
				{"overflow-clamped", 2, math.MaxInt64, "cdef", false},
				{"negative-offset", -1, 1, "", true},
				{"negative-size", 0, -1, "", true},
			} {
				t.Run(tt.name, func(t *testing.T) {
					dst := make([]byte, 7, 128)
					copy(dst, "prefix:")
					out, err := backend.ReadAtAppend(t.Context(), b, dst, "data", tt.off, tt.n)
					if tt.bad {
						require.Error(t, err)
					} else {
						require.NoError(t, err)
					}
					require.Equal(t, "prefix:"+tt.want, string(out))
					require.Same(t, &dst[0], &out[0])
				})
			}
			dst := []byte("prefix:")
			out, err := backend.ReadAtAppend(t.Context(), b, dst, "missing", 0, 1)
			require.ErrorIs(t, err, backend.ErrNotExist)
			require.Equal(t, dst, out)
			out, err = backend.ReadAtAppend(t.Context(), b, nil, "data", 0, 6)
			require.NoError(t, err)
			out[0] = '!'
			stored, err := b.Read(t.Context(), "data")
			require.NoError(t, err)
			require.Equal(t, "abcdef", string(stored))
		})
	}
}

func FuzzReadAtAppend(f *testing.F) {
	f.Add([]byte("abcdef"), []byte("prefix:"), int64(1), int64(3))
	f.Add([]byte("abcdef"), []byte{}, int64(2), int64(math.MaxInt64))
	f.Add([]byte{}, []byte("prefix:"), int64(0), int64(0))
	f.Fuzz(func(t *testing.T, data, prefix []byte, off, n int64) {
		if len(data)+len(prefix) > 1<<20 {
			t.Skip()
		}
		b := backend.Memory()
		ctx := context.Background()
		if err := b.Write(ctx, "object", data); err != nil {
			t.Fatal(err)
		}
		out, err := backend.ReadAtAppend(ctx, b, append([]byte(nil), prefix...), "object", off, n)
		if off < 0 || n < 0 {
			if err == nil {
				t.Fatal("accepted negative range")
			}
			return
		}
		if err != nil {
			t.Fatal(err)
		}
		var tail []byte
		if off < int64(len(data)) {
			tail = data[off : off+min(n, int64(len(data))-off)]
		}
		want := append(append([]byte(nil), prefix...), tail...)
		if !bytes.Equal(out, want) {
			t.Fatal("range or prefix changed")
		}
	})
}

package sidecar

import (
	"bytes"
	"context"
	"encoding/binary"
	"hash/crc32"
	"io"
	"testing"

	"github.com/go-faster/errors"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
)

type testBackend struct {
	backend.Backend

	writer *testWriter
}
type testWriter struct {
	bytes.Buffer

	fail, short, commitFail bool
	aborted, committed      bool
	largest                 int
}

func (b *testBackend) CreateObject(context.Context, string) (backend.ObjectWriter, error) {
	return b.writer, nil
}
func (*testBackend) StreamsWrites() bool { return true }
func (w *testWriter) Write(p []byte) (int, error) {
	w.largest = max(w.largest, len(p))
	if w.fail {
		return 0, io.ErrClosedPipe
	}
	if w.short {
		return len(p) - 1, nil
	}
	return w.Buffer.Write(p)
}
func (w *testWriter) Commit(context.Context) error {
	if w.commitFail {
		return io.ErrClosedPipe
	}
	w.committed = true
	return nil
}
func (w *testWriter) Abort() {
	if !w.committed {
		w.aborted = true
	}
}

func TestWriteCheckedChunks(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name                string
		fail, short, commit bool
		want                error
	}{
		{name: "complete"}, {name: "write", fail: true, want: io.ErrClosedPipe}, {name: "short", short: true, want: io.ErrShortWrite}, {name: "commit", commit: true, want: io.ErrClosedPipe},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			w := &testWriter{fail: tc.fail, short: tc.short, commitFail: tc.commit}
			b := &testBackend{Backend: backend.Memory(), writer: w}
			header := []byte("header")
			err := Write(t.Context(), b, "object", header, 10000, 8, func(dst []byte, i int) []byte { return binary.BigEndian.AppendUint64(dst, uint64(i)) })
			if tc.want != nil {
				require.ErrorIs(t, err, tc.want)
				require.True(t, w.aborted)
				require.False(t, w.committed)
				return
			}
			require.NoError(t, err)
			require.True(t, w.committed)
			require.LessOrEqual(t, w.largest, ScratchBytes)
			want := bytes.Clone(header)
			for i := range 10000 {
				want = binary.BigEndian.AppendUint64(want, uint64(i))
			}
			want = binary.BigEndian.AppendUint32(want, crc32.Checksum(want, checksums))
			require.Equal(t, want, w.Bytes())
		})
	}
}

func TestWriteRejectsInvalidOrCanceledInput(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct{ count, size int }{{-1, 8}, {1, 0}, {1, ScratchBytes}} {
		require.Error(t, Write(t.Context(), backend.Memory(), "object", nil, tc.count, tc.size, nil))
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	require.ErrorIs(t, Write(ctx, backend.Memory(), "object", nil, 1, 8, nil), context.Canceled)
	be := backend.Memory()
	err := Write(t.Context(), be, "object", nil, 1, 1, func(dst []byte, _ int) []byte { return append(dst, 1, 2) })
	require.Error(t, err)
	_, err = be.Read(t.Context(), "object")
	require.ErrorIs(t, err, backend.ErrNotExist)
}

func FuzzWriteCheckedChunks(f *testing.F) {
	for _, data := range [][]byte{nil, {1}, {0, 255, 128}, bytes.Repeat([]byte{42}, 100)} {
		f.Add(data)
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) > 100000 {
			return
		}
		w := &testWriter{}
		b := &testBackend{Backend: backend.Memory(), writer: w}
		if err := Write(t.Context(), b, "object", []byte{0x42}, len(data), 1, func(dst []byte, i int) []byte { return append(dst, data[i]) }); err != nil {
			t.Fatal(err)
		}
		got := w.Bytes()
		want := append([]byte{0x42}, data...)
		want = binary.BigEndian.AppendUint32(want, crc32.Checksum(want, checksums))
		if !bytes.Equal(got, want) {
			t.Fatal(errors.New("encoded bytes differ"))
		}
	})
}

package compress_test

import (
	"bytes"
	"io"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/encoding/compress"
)

func TestZSTDFramerRoundTrip(t *testing.T) {
	t.Parallel()

	f := compress.NewZSTDFramer(compress.LevelFast)

	for _, src := range [][]byte{
		nil,
		[]byte("x"),
		bytes.Repeat([]byte("log line\n"), 1000),
	} {
		enc := f.Compress(nil, src)

		r, err := f.Reader(bytes.NewReader(enc))
		require.NoError(t, err)

		got, err := io.ReadAll(r)
		require.NoError(t, err)
		require.NoError(t, r.Close())
		assert.Len(t, got, len(src))
		assert.True(t, bytes.Equal(src, got))

		require.NoError(t, r.Close(), "close is idempotent")

		n, err := r.Read(make([]byte, 4))
		assert.Zero(t, n)
		assert.ErrorIs(t, err, io.EOF, "a closed reader is drained, not a use-after-free of the pooled decoder")
	}
}

// The compressed form is a bare zstd frame, so the flag-byte [compress.Compressor] framing must not
// be able to read it — that is what makes it interchangeable with any zstd implementation.
func TestZSTDFramerIsBareFrame(t *testing.T) {
	t.Parallel()

	src := bytes.Repeat([]byte("payload"), 100)
	enc := compress.NewZSTDFramer(compress.LevelFast).Compress(nil, src)

	assert.NotEqual(t, compress.FlagCompressed, enc[0])
	assert.NotEqual(t, compress.FlagRaw, enc[0])
	assert.Equal(t, []byte{0x28, 0xb5, 0x2f, 0xfd}, enc[:4], "zstd magic")
}

func TestZSTDFramerCorruptFrame(t *testing.T) {
	t.Parallel()

	f := compress.NewZSTDFramer(compress.LevelFast)

	r, err := f.Reader(bytes.NewReader([]byte("definitely not zstd")))
	if err == nil {
		_, err = io.ReadAll(r)
		require.NoError(t, r.Close())
	}

	require.Error(t, err)
}

func TestZSTDFramerConcurrent(t *testing.T) {
	t.Parallel()

	f := compress.NewZSTDFramer(compress.LevelFast)
	src := bytes.Repeat([]byte("concurrent"), 500)

	var wg sync.WaitGroup

	for range 8 {
		wg.Go(func() {
			// testify's FailNow is not safe off the test goroutine, so this loop reports and stops.
			for range 20 {
				r, err := f.Reader(bytes.NewReader(f.Compress(nil, src)))
				if err != nil {
					t.Error(err)

					return
				}

				got, err := io.ReadAll(r)
				if err != nil || !bytes.Equal(src, got) {
					t.Errorf("round-trip: %d bytes, err %v", len(got), err)
				}

				if err := r.Close(); err != nil {
					t.Error(err)

					return
				}
			}
		})
	}

	wg.Wait()
}

package symbols

import (
	"bytes"
	"io"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestWriteToMatchesEncode(t *testing.T) {
	t.Parallel()
	for i, values := range [][]string{nil, {""}, {"a", "", "value", "a"}, {strings.Repeat("x", 128<<10)}} {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			t.Parallel()
			tab := New()
			defer tab.Release()
			for _, value := range values {
				tab.Intern([]byte(value))
			}
			var out bytes.Buffer
			n, err := tab.WriteTo(&out)
			require.NoError(t, err)
			require.Equal(t, int64(out.Len()), n)
			require.Equal(t, tab.Encode(nil), out.Bytes())
		})
	}
}

type invalidSymbolWriter struct {
	call, at, count int
}

func (w *invalidSymbolWriter) Write(data []byte) (int, error) {
	w.call++
	if w.call == w.at {
		return w.count, nil
	}
	return len(data), nil
}

func TestWriteToInvalidWriterCount(t *testing.T) {
	t.Parallel()
	tab := New()
	defer tab.Release()
	for _, call := range []int{1, 2} {
		for _, count := range []int{-1, 100} {
			n, err := tab.WriteTo(&invalidSymbolWriter{at: call, count: count})
			require.ErrorIs(t, err, io.ErrShortWrite)
			require.GreaterOrEqual(t, n, int64(0))
		}
	}
}

type shortSymbolWriter struct{ remaining int }

func (w *shortSymbolWriter) Write(p []byte) (int, error) {
	n := min(w.remaining, len(p))
	w.remaining -= n
	return n, nil
}

func TestWriteToShortWrite(t *testing.T) {
	t.Parallel()
	tab := New()
	defer tab.Release()
	tab.Intern([]byte("value"))
	for limit := range len(tab.Encode(nil)) {
		n, err := tab.WriteTo(&shortSymbolWriter{remaining: limit})
		require.ErrorIs(t, err, io.ErrShortWrite)
		require.Equal(t, int64(limit), n)
	}
}

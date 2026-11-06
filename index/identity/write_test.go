package identity_test

import (
	"bytes"
	"io"
	"slices"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/index/identity"
	"github.com/oteldb/storage/index/series"
	"github.com/oteldb/storage/signal"
)

func TestWriteSortedCompatibleWithExistingDecoder(t *testing.T) {
	t.Parallel()
	for _, count := range []int{0, 1, 17, 1024} {
		t.Run(strconv.Itoa(count), func(t *testing.T) {
			t.Parallel()
			entries := make([]series.Entry, count)
			for i := range entries {
				entries[i] = mkEntry(i)
			}
			slices.SortFunc(entries, func(a, b series.Entry) int { return a.ID.Compare(b.ID) })
			entry := func(i int, _ signal.Attributes) series.Entry { return entries[i] }
			legacy := identity.EncodeSorted(nil, count, entry)
			var out bytes.Buffer
			written, err := identity.WriteSorted(&out, count, entry)
			require.NoError(t, err)
			require.Equal(t, int64(out.Len()), written)
			n, err := identity.Count(out.Bytes())
			require.NoError(t, err)
			require.Equal(t, count, n)
			require.Equal(t, legacy[:5], out.Bytes()[:5], "same magic and format version")
			require.Equal(t, collect(t, legacy), collect(t, out.Bytes()))
			require.Equal(t, legacy, identity.EncodeSorted(nil, count, entry), "streaming must not change the existing encoder")
		})
	}
}

func TestWriteSortedReusesAttributes(t *testing.T) {
	t.Parallel()
	index := series.New()
	for i := range 3 {
		index.Add(mkEntry(i).Series)
	}
	view := index.View()
	refs := []int{0, 1, 2}
	slices.SortFunc(refs, func(a, b int) int { return view.ID(a).Compare(view.ID(b)) })
	var prior signal.Attributes
	var out bytes.Buffer
	_, err := identity.WriteSorted(&out, len(refs), func(i int, dst signal.Attributes) series.Entry {
		if i > 0 {
			require.Same(t, &prior[0], &dst[0])
		}
		ref := refs[i]
		value := view.At(ref, dst)
		prior = value.Attributes
		return series.Entry{ID: view.ID(ref), Series: value}
	})
	require.NoError(t, err)
	require.Len(t, collect(t, out.Bytes()), len(refs))
}

type limitedIdentityWriter struct {
	bytes.Buffer

	remaining int
	err       error
}

type invalidIdentityWriter int

func (w invalidIdentityWriter) Write([]byte) (int, error) { return int(w), nil }

func TestWriteSortedInvalidWriterCount(t *testing.T) {
	t.Parallel()
	for _, count := range []int{-1, 100} {
		n, err := identity.WriteSorted(invalidIdentityWriter(count), 0, nil)
		require.ErrorIs(t, err, io.ErrShortWrite)
		require.Zero(t, n)
	}
}

func (w *limitedIdentityWriter) Write(p []byte) (int, error) {
	n := min(w.remaining, len(p))
	w.remaining -= n
	_, _ = w.Buffer.Write(p[:n])
	if n != len(p) {
		return n, w.err
	}
	return n, nil
}

func TestWriteSortedWriterErrors(t *testing.T) {
	t.Parallel()
	entry := func(int, signal.Attributes) series.Entry { return mkEntry(0) }
	var valid bytes.Buffer
	_, err := identity.WriteSorted(&valid, 1, entry)
	require.NoError(t, err)
	for _, cause := range []error{nil, io.ErrClosedPipe} {
		for limit := range valid.Len() {
			w := &limitedIdentityWriter{remaining: limit, err: cause}
			n, err := identity.WriteSorted(w, 1, entry)
			want := cause
			if want == nil {
				want = io.ErrShortWrite
			}
			require.ErrorIs(t, err, want)
			require.Equal(t, int64(limit), n)
			require.True(t, bytes.Equal(valid.Bytes()[:limit], w.Bytes()))
		}
	}
	_, err = identity.WriteSorted(io.Discard, -1, entry)
	require.Error(t, err)
}

func TestWriteSortedWideIdentity(t *testing.T) {
	t.Parallel()
	attrs := make(signal.Attributes, 1024)
	for i := range attrs {
		attrs[i] = kv(strconv.Itoa(i), signal.IntValue(int64(i)))
	}
	s := signal.Series{Attributes: signal.NewAttributes(attrs...)}
	var out bytes.Buffer
	_, err := identity.WriteSorted(&out, 1, func(int, signal.Attributes) series.Entry {
		return series.Entry{ID: s.Hash(), Series: s}
	})
	require.NoError(t, err)
	require.True(t, s.Equal(collect(t, out.Bytes())[s.Hash()]))
}

func FuzzWriteSortedRoundTrip(f *testing.F) {
	f.Add("name", "value", int64(7))
	f.Add("", "", int64(0))
	f.Add("a\x00b", "\xff", int64(-1))
	f.Fuzz(func(t *testing.T, key, value string, number int64) {
		s := signal.Series{Attributes: signal.NewAttributes(kv(key, str(value)), kv(key+"_int", signal.IntValue(number)))}
		entry := series.Entry{ID: s.Hash(), Series: s}
		var out bytes.Buffer
		_, err := identity.WriteSorted(&out, 1, func(int, signal.Attributes) series.Entry { return entry })
		if err != nil {
			t.Fatal(err)
		}
		count := 0
		if err := identity.Decode(out.Bytes(), func(id signal.SeriesID, got signal.Series) error {
			if id != entry.ID || !got.Equal(s) || got.Hash() != id {
				t.Fatal("identity changed during streaming")
			}
			count++
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		if count != 1 {
			t.Fatalf("decoded %d identities, want 1", count)
		}
	})
}

func BenchmarkIdentityDestination(b *testing.B) {
	const count = 100_000
	entries := make([]series.Entry, count)
	var logical int64
	for i := range entries {
		entries[i] = mkEntry(i)
		logical += int64(16 + len(entries[i].Series.AppendHashInput(nil)))
	}
	slices.SortFunc(entries, func(a, b series.Entry) int { return a.ID.Compare(b.ID) })
	entry := func(i int, _ signal.Attributes) series.Entry { return entries[i] }
	b.Run("slice", func(b *testing.B) {
		b.SetBytes(logical)
		b.ReportAllocs()
		for b.Loop() {
			_, _ = io.Discard.Write(identity.EncodeSorted(nil, count, entry))
		}
	})
	b.Run("writer", func(b *testing.B) {
		b.SetBytes(logical)
		b.ReportAllocs()
		for b.Loop() {
			if _, err := identity.WriteSorted(io.Discard, count, entry); err != nil {
				b.Fatal(err)
			}
		}
	})
}

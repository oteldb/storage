// Package backendtest provides a shared conformance suite that every
// [backend.Backend] implementation must pass, proving the implementations are
// interchangeable (DESIGN.md §2: "backends are interchangeable behind backend.Backend").
package backendtest

import (
	"context"
	"fmt"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/go-faster/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
)

// Run executes the full conformance suite against a backend produced by factory — the
// object operations plus the conditional-write (CAS) [Backend.PutIfAbsent]. Each subtest gets
// a fresh, empty backend. Call it from each implementation's test package.
func Run(t *testing.T, factory func(t *testing.T) backend.Backend) {
	t.Helper()

	ctx := context.Background()

	t.Run("WriteRead", func(t *testing.T) {
		b := factory(t)
		require.NoError(t, b.Write(ctx, "a/b/c", []byte("hello")))

		got, err := b.Read(ctx, "a/b/c")
		require.NoError(t, err)
		assert.Equal(t, []byte("hello"), got)
	})

	t.Run("Overwrite", func(t *testing.T) {
		b := factory(t)
		require.NoError(t, b.Write(ctx, "k", []byte("first")))
		require.NoError(t, b.Write(ctx, "k", []byte("second")))

		got, err := b.Read(ctx, "k")
		require.NoError(t, err)
		assert.Equal(t, []byte("second"), got)
	})

	t.Run("ReadMissing", func(t *testing.T) {
		b := factory(t)
		_, err := b.Read(ctx, "nope")
		require.Error(t, err)
		assert.ErrorIs(t, err, backend.ErrNotExist)
	})

	t.Run("DeleteMissing", func(t *testing.T) {
		b := factory(t)
		err := b.Delete(ctx, "nope")
		require.Error(t, err)
		assert.ErrorIs(t, err, backend.ErrNotExist)
	})

	t.Run("DeleteThenRead", func(t *testing.T) {
		b := factory(t)
		require.NoError(t, b.Write(ctx, "k", []byte("v")))
		require.NoError(t, b.Delete(ctx, "k"))

		_, err := b.Read(ctx, "k")
		assert.ErrorIs(t, err, backend.ErrNotExist)
	})

	t.Run("SizeOf", func(t *testing.T) {
		b := factory(t)
		require.NoError(t, b.Write(ctx, "sized", []byte("twelve bytes")))

		n, err := backend.SizeOf(ctx, b, "sized")
		require.NoError(t, err)
		assert.Equal(t, int64(len("twelve bytes")), n)

		_, err = backend.SizeOf(ctx, b, "absent")
		assert.ErrorIs(t, err, backend.ErrNotExist)
	})

	t.Run("EmptyValue", func(t *testing.T) {
		b := factory(t)
		require.NoError(t, b.Write(ctx, "empty", []byte{}))

		got, err := b.Read(ctx, "empty")
		require.NoError(t, err, "empty value must be stored, distinct from absent")
		assert.Empty(t, got)
	})

	t.Run("LargeValue", func(t *testing.T) {
		b := factory(t)
		big := make([]byte, 1<<20)
		for i := range big {
			big[i] = byte(i)
		}
		require.NoError(t, b.Write(ctx, "big", big))

		got, err := b.Read(ctx, "big")
		require.NoError(t, err)
		assert.Equal(t, big, got)
	})

	t.Run("ListByPrefix", func(t *testing.T) {
		b := factory(t)
		pKeys := []string{"p/a", "p/b", "p/c/d"} // share the "p/" prefix
		allKeys := append(append([]string{}, pKeys...), "q/a", "z")
		for _, k := range allKeys {
			require.NoError(t, b.Write(ctx, k, []byte("v")))
		}

		got, err := b.List(ctx, "p/")
		require.NoError(t, err)
		assert.Equal(t, pKeys, got, "prefixed, sorted")

		all, err := b.List(ctx, "")
		require.NoError(t, err)
		assert.Equal(t, allKeys, all, "empty prefix lists all, sorted")

		none, err := b.List(ctx, "missing/")
		require.NoError(t, err)
		assert.Empty(t, none)
	})

	t.Run("ReadReturnsIsolatedCopy", func(t *testing.T) {
		b := factory(t)
		require.NoError(t, b.Write(ctx, "k", []byte("abcd")))

		got, err := b.Read(ctx, "k")
		require.NoError(t, err)
		if len(got) > 0 {
			got[0] = 'X' // mutating the returned slice must not affect stored state
		}

		again, err := b.Read(ctx, "k")
		require.NoError(t, err)
		assert.Equal(t, []byte("abcd"), again, "stored value must be isolated from a returned slice")
	})

	t.Run("WriteCopiesInput", func(t *testing.T) {
		b := factory(t)
		buf := []byte("abcd")
		require.NoError(t, b.Write(ctx, "k", buf))
		buf[0] = 'X' // mutating the caller's buffer after Write must not affect stored state

		got, err := b.Read(ctx, "k")
		require.NoError(t, err)
		assert.Equal(t, []byte("abcd"), got, "stored value must be isolated from the caller's buffer")
	})

	t.Run("ConcurrentDistinctKeys", func(t *testing.T) {
		b := factory(t)

		const n = 64

		var wg sync.WaitGroup

		wg.Add(n)
		for i := range n {
			go func(i int) {
				defer wg.Done()

				key := fmt.Sprintf("c/%d", i)
				val := fmt.Appendf(nil, "value-%d", i)
				if err := b.Write(ctx, key, val); err != nil {
					assert.NoError(t, err)

					return
				}

				got, err := b.Read(ctx, key)
				assert.NoError(t, err)
				assert.Equal(t, val, got)
			}(i)
		}

		wg.Wait()

		all, err := b.List(ctx, "c/")
		require.NoError(t, err)
		assert.Len(t, all, n)
	})

	t.Run("EphemeralReported", func(t *testing.T) {
		b := factory(t)
		// Just exercise the method; value depends on the implementation.
		_ = b.IsEphemeral()
		_ = errors.Is(backend.ErrNotExist, backend.ErrNotExist)
	})

	t.Run("PutIfAbsentClaimsKey", func(t *testing.T) {
		b := factory(t)

		ok, err := b.PutIfAbsent(ctx, "cas/k", []byte("first"))
		require.NoError(t, err)
		assert.True(t, ok, "first PutIfAbsent claims the key")

		ok, err = b.PutIfAbsent(ctx, "cas/k", []byte("second"))
		require.NoError(t, err)
		assert.False(t, ok, "second PutIfAbsent is a no-op")

		got, err := b.Read(ctx, "cas/k")
		require.NoError(t, err)
		assert.Equal(t, []byte("first"), got, "the original value is preserved")
	})

	t.Run("PutIfAbsentVsWrite", func(t *testing.T) {
		b := factory(t)

		require.NoError(t, b.Write(ctx, "cas/w", []byte("written")))
		ok, err := b.PutIfAbsent(ctx, "cas/w", []byte("absent"))
		require.NoError(t, err)
		assert.False(t, ok, "PutIfAbsent yields to an existing Write")
	})

	t.Run("PutIfAbsentConcurrentSingleWinner", func(t *testing.T) {
		b := factory(t)

		const n = 32

		var (
			wg   sync.WaitGroup
			wins atomic.Int64
		)

		wg.Add(n)
		for i := range n {
			go func(i int) {
				defer wg.Done()

				ok, err := b.PutIfAbsent(ctx, "cas/race", fmt.Appendf(nil, "w-%d", i))
				assert.NoError(t, err)
				if ok {
					wins.Add(1)
				}
			}(i)
		}

		wg.Wait()
		assert.Equal(t, int64(1), wins.Load(), "exactly one writer claims the key")
	})

	runObjectWriter(t, ctx, factory)
}

// runObjectWriter covers the incremental-write seam ([backend.CreateObject]). It runs over every
// backend, not only those implementing [backend.ObjectCreator]: the buffering fallback must be
// indistinguishable from a native implementation, since that is what lets callers stream
// unconditionally.
func runObjectWriter(t *testing.T, ctx context.Context, factory func(t *testing.T) backend.Backend) {
	t.Helper()

	t.Run("ObjectWriterCommits", func(t *testing.T) {
		b := factory(t)

		w, err := backend.CreateObject(ctx, b, "obj/a")
		require.NoError(t, err)

		for _, chunk := range []string{"one", "two", "three"} {
			n, err := w.Write([]byte(chunk))
			require.NoError(t, err)
			assert.Equal(t, len(chunk), n, "Write must report every byte it accepted")
		}

		_, err = b.Read(ctx, "obj/a")
		require.ErrorIs(t, err, backend.ErrNotExist, "nothing is stored before the commit")

		require.NoError(t, w.Commit(ctx))

		got, err := b.Read(ctx, "obj/a")
		require.NoError(t, err)
		assert.Equal(t, []byte("onetwothree"), got)

		w.Abort() // idempotent after a commit, so `defer w.Abort()` is safe cleanup

		got, err = b.Read(ctx, "obj/a")
		require.NoError(t, err)
		assert.Equal(t, []byte("onetwothree"), got, "abort after commit must not undo it")
	})

	t.Run("ObjectWriterAborts", func(t *testing.T) {
		b := factory(t)
		require.NoError(t, b.Write(ctx, "obj/b", []byte("original")))

		w, err := backend.CreateObject(ctx, b, "obj/b")
		require.NoError(t, err)

		_, err = w.Write([]byte("replacement"))
		require.NoError(t, err)

		w.Abort()

		got, err := b.Read(ctx, "obj/b")
		require.NoError(t, err)
		assert.Equal(t, []byte("original"), got, "an aborted write leaves the key as it was")

		keys, err := b.List(ctx, "")
		require.NoError(t, err)
		assert.Equal(t, []string{"obj/b"}, keys, "an aborted write leaves no debris behind")
	})

	t.Run("ObjectWriterOverwrites", func(t *testing.T) {
		b := factory(t)
		require.NoError(t, b.Write(ctx, "obj/c", []byte("original")))

		w, err := backend.CreateObject(ctx, b, "obj/c")
		require.NoError(t, err)

		_, err = w.Write([]byte("replacement"))
		require.NoError(t, err)
		require.NoError(t, w.Commit(ctx))

		got, err := b.Read(ctx, "obj/c")
		require.NoError(t, err)
		assert.Equal(t, []byte("replacement"), got)
	})

	// Two writers over one key is how a part's rival codecs race: both encode, only the denser one
	// commits. Whichever loses must leave no trace of itself.
	t.Run("ObjectWriterRivalsOverOneKey", func(t *testing.T) {
		b := factory(t)

		winner, err := backend.CreateObject(ctx, b, "obj/d")
		require.NoError(t, err)

		loser, err := backend.CreateObject(ctx, b, "obj/d")
		require.NoError(t, err)

		_, err = winner.Write([]byte("kept"))
		require.NoError(t, err)

		_, err = loser.Write([]byte("discarded"))
		require.NoError(t, err)

		loser.Abort()
		require.NoError(t, winner.Commit(ctx))

		got, err := b.Read(ctx, "obj/d")
		require.NoError(t, err)
		assert.Equal(t, []byte("kept"), got)

		keys, err := b.List(ctx, "")
		require.NoError(t, err)
		assert.Equal(t, []string{"obj/d"}, keys)
	})

	t.Run("ObjectWriterEmpty", func(t *testing.T) {
		b := factory(t)

		w, err := backend.CreateObject(ctx, b, "obj/e")
		require.NoError(t, err)
		require.NoError(t, w.Commit(ctx))

		got, err := b.Read(ctx, "obj/e")
		require.NoError(t, err)
		assert.Empty(t, got, "a committed empty object exists and is empty, not absent")
	})

	t.Run("ObjectWriterLargeValue", func(t *testing.T) {
		b := factory(t)

		want := make([]byte, 1<<20)
		for i := range want {
			want[i] = byte(i)
		}

		w, err := backend.CreateObject(ctx, b, "obj/f")
		require.NoError(t, err)

		for off := 0; off < len(want); off += 4096 {
			_, err := w.Write(want[off:min(off+4096, len(want))])
			require.NoError(t, err)
		}

		require.NoError(t, w.Commit(ctx))

		got, err := b.Read(ctx, "obj/f")
		require.NoError(t, err)
		assert.Equal(t, want, got)
	})
}

// AtomicConditionalPut wraps an S3-compatible test server's handler, serializing every request
// that carries an If-None-Match precondition. Real S3 evaluates a conditional PUT atomically;
// the embeddable go-faster/fs server (as of v0.3.0) checks the precondition and performs the
// write as two separate steps, so two concurrent "If-None-Match: *" PUTs can both observe the
// key absent and both succeed — a TOCTOU race the conformance suite's single-winner CAS test
// rightly rejects. Serializing just the conditional requests restores real-S3 semantics for
// the tests without patching the fake. (Worth fixing upstream; this wrapper is then a no-op.)
func AtomicConditionalPut(h http.Handler) http.Handler {
	var mu sync.Mutex

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPut && r.Header.Get("If-None-Match") != "" {
			mu.Lock()
			defer mu.Unlock()
		}

		h.ServeHTTP(w, r)
	})
}

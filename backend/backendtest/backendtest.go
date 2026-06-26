// Package backendtest provides a shared conformance suite that every
// [backend.Backend] implementation must pass, proving the implementations are
// interchangeable (DESIGN.md §2: "backends are interchangeable behind backend.Backend").
package backendtest

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/go-faster/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
)

// Run executes the full conformance suite against a backend produced by factory: the core
// object operations ([RunCore]) plus the conditional-write (CAS) operations. Each subtest
// gets a fresh, empty backend. Call it from each implementation's test package.
func Run(t *testing.T, factory func(t *testing.T) backend.Backend) {
	t.Helper()

	RunCore(t, factory)
	runConditional(t, factory)
}

// RunCore executes the unconditional object operations of the conformance suite
// (Read/Write/List/Delete/IsEphemeral). It omits [Backend.PutIfAbsent], so it can validate a
// backend over an object store that lacks conditional writes (S3 If-None-Match) — the
// embeddable go-faster/fs server used by the s3 integration test does not implement them.
// Backends that do support CAS should use [Run].
func RunCore(t *testing.T, factory func(t *testing.T) backend.Backend) {
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
}

// runConditional executes the PutIfAbsent (CAS) conformance subtests.
func runConditional(t *testing.T, factory func(t *testing.T) backend.Backend) {
	t.Helper()

	ctx := context.Background()

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
}

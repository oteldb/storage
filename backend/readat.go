package backend

import (
	"context"

	"github.com/go-faster/errors"
)

// ReaderAt is an optional [Backend] capability: read a byte range of an object instead of the whole
// thing. It is the read counterpart of [ObjectCreator], and it exists for the same reason — a part
// stores one object per column, so without it touching any block of a column costs the whole column,
// and part size becomes a bound on process memory rather than on disk.
//
// Both durable backends have it natively (`pread` for file, the `Range` header for s3). Use
// [ReadAt] rather than asserting directly: it falls back to reading the whole object and slicing for
// backends without the capability, so callers stay correct everywhere and a wrapper that forgets to
// forward costs bytes rather than correctness.
type ReaderAt interface {
	// ReadAt returns the bytes of key in [off, off+n), clamped to the object's end. See [ReadAt] for
	// the contract implementations must honor.
	ReadAt(ctx context.Context, key string, off, n int64) ([]byte, error)
}

// ReadAt returns the bytes of key in [off, off+n), using b's [ReaderAt] fast path when it has one
// and otherwise reading the whole object and slicing it.
//
// The range is **clamped to the object's end**: a caller may ask for more than is there and gets
// what exists, and an off at or past the end returns empty. That is what lets a reader take an
// object's trailer without first learning its size — one round trip rather than two. A short result
// is therefore not an error, and a caller that needs exactly n bytes must check the length itself.
//
// Negative off or n is a programming error and returns one. An absent key errors like [Backend.Read].
// The returned slice is caller-owned; unlike [ReadView] it never aliases backend state.
func ReadAt(ctx context.Context, b Backend, key string, off, n int64) ([]byte, error) {
	if off < 0 || n < 0 {
		return nil, errors.Errorf("backend: read %q at [%d,+%d): negative range", key, off, n)
	}

	if r, ok := b.(ReaderAt); ok {
		return r.ReadAt(ctx, key, off, n)
	}

	data, err := b.Read(ctx, key)
	if err != nil {
		return nil, err
	}

	return clampRange(data, off, n), nil
}

// ViewerAt is [ReaderAt]'s no-copy counterpart, the ranged form of [Viewer]: it returns the range as
// a **read-only view** that may alias shared state instead of a caller-owned copy. The same contract
// applies — never mutate it, and it stays valid indefinitely because a stored value is never mutated
// in place.
//
// Only a backend already holding the object in memory can offer it ([Memory], and the read cache
// over a resident entry); file and s3 must materialize the bytes to return them, so for those
// [ReadViewAt] falls through to [ReadAt].
type ViewerAt interface {
	// ReadViewAt returns key's [off, off+n) range as a read-only view, clamped like [ReadAt].
	ReadViewAt(ctx context.Context, key string, off, n int64) ([]byte, error)
}

// ReadViewAt returns key's [off, off+n) range without a copy where the backend can manage one,
// falling back to [ReadAt] and finally to slicing a whole-object read. Either way the caller must
// treat the result as read-only — that is the contract that lets implementations skip the copy.
//
// It exists so that ranging does not cost the in-memory backend the zero-copy read it had when
// callers took whole columns: a decompressor reads its input and never retains it, so a view is
// exactly as safe there as it is for [ReadView].
func ReadViewAt(ctx context.Context, b Backend, key string, off, n int64) ([]byte, error) {
	if off < 0 || n < 0 {
		return nil, errors.Errorf("backend: read %q at [%d,+%d): negative range", key, off, n)
	}

	switch v := b.(type) {
	case ViewerAt:
		return v.ReadViewAt(ctx, key, off, n)
	case ReaderAt:
		return v.ReadAt(ctx, key, off, n)
	case Viewer:
		// A whole-object view still beats ReadAt's fallback, which copies the object to slice it.
		data, err := v.ReadView(ctx, key)
		if err != nil {
			return nil, err
		}

		return clampRange(data, off, n), nil
	}

	data, err := b.Read(ctx, key)
	if err != nil {
		return nil, err
	}

	return clampRange(data, off, n), nil
}

// clampRange slices data to [off, off+n) under [ReadAt]'s clamping contract. It is the shared
// implementation of that contract for backends holding the object in memory.
func clampRange(data []byte, off, n int64) []byte {
	if off >= int64(len(data)) {
		return []byte{}
	}

	return data[off:min(off+n, int64(len(data)))]
}

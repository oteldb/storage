package backend

import (
	"context"
	"io"

	"github.com/go-faster/errors"
)

// ObjectWriter builds one object incrementally. Bytes appended with Write are not visible under the
// object's key until [ObjectWriter.Commit]; until then the key reads as it did before (absent, or
// the previous value), so the "manifest written last" commit rule still holds.
//
// Commit publishes the bytes atomically, exactly like [Backend.Write]. Abort discards them and is
// safe to call after Commit (where it does nothing), so `defer w.Abort()` is the correct cleanup.
// A writer must not be used from more than one goroutine, but several writers may target the same
// key concurrently — only the one that commits wins, which is how a part's rival-codec columns race.
type ObjectWriter interface {
	io.Writer

	// Commit publishes everything written so far under the writer's key.
	Commit(ctx context.Context) error

	// Abort discards the writer's bytes and releases whatever it holds. It is idempotent and a
	// no-op once Commit has succeeded.
	Abort()
}

// ObjectCreator is an optional [Backend] capability: build an object incrementally rather than
// handing it over whole. It exists so a writer producing an object far larger than it wants
// resident — a merged part's column — can hand finished bytes to the backend as they are produced
// instead of holding the whole object in RAM.
//
// Use [CreateObject] rather than asserting directly: it falls back to buffering into a
// [Backend.Write] for backends without the capability, so callers stay correct everywhere. Use
// [StreamsWrites] to ask whether that fallback would be taken — the caller that *sizes* its output
// against memory needs to know, since the fallback puts the object back in RAM.
type ObjectCreator interface {
	// CreateObject returns a writer building the object stored under key. Nothing is stored until
	// the writer commits.
	CreateObject(ctx context.Context, key string) (ObjectWriter, error)
}

// CreateObject returns an [ObjectWriter] for key, using b's [ObjectCreator] fast path when it has
// one and otherwise buffering into memory and issuing a single [Backend.Write] on commit.
func CreateObject(ctx context.Context, b Backend, key string) (ObjectWriter, error) {
	if c, ok := b.(ObjectCreator); ok {
		return c.CreateObject(ctx, key)
	}

	return &bufferedObjectWriter{b: b, key: key}, nil
}

// StreamsWrites reports whether b builds objects incrementally, i.e. whether [CreateObject] returns
// a writer that keeps finished bytes out of RAM. It is a sizing question, not a correctness one:
// [CreateObject] works over any backend.
func StreamsWrites(b Backend) bool {
	_, ok := b.(ObjectCreator)

	return ok
}

// bufferedObjectWriter is the [ObjectWriter] fallback for a backend without [ObjectCreator]: it
// accumulates the object in memory and writes it whole on commit.
type bufferedObjectWriter struct {
	b    Backend
	key  string
	buf  []byte
	done bool
}

func (w *bufferedObjectWriter) Write(p []byte) (int, error) {
	if w.done {
		return 0, errors.New("backend: write after commit")
	}

	w.buf = append(w.buf, p...)

	return len(p), nil
}

func (w *bufferedObjectWriter) Commit(ctx context.Context) error {
	if w.done {
		return errors.New("backend: commit after commit")
	}

	if err := w.b.Write(ctx, w.key, w.buf); err != nil {
		return err
	}

	w.done, w.buf = true, nil

	return nil
}

func (w *bufferedObjectWriter) Abort() { w.buf = nil }

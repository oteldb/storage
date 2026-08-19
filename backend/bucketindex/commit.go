package bucketindex

import (
	"context"
	"fmt"
	"slices"
	"strconv"
	"strings"

	"github.com/go-faster/errors"

	"github.com/oteldb/storage/backend"
)

// Dir is the basename of the directory — a sibling of [Object] under the same prefix — holding the
// generation-named objects a commit claims. Its objects are commit state, not part data: a sweep
// or a mirror that walks the prefix must leave them alone (see [IsGenerationKey]).
const Dir = "bucket-index"

// ErrConflict reports a lost commit: the generation this writer prepared was claimed by another
// writer first, so its index was never installed and its caller must not treat the write as done.
// The recovery is to reload and rebuild on the newly committed state — [Commit] does exactly that.
var ErrConflict = errors.New("bucketindex: generation already claimed")

const (
	// maxCommitAttempts bounds [Commit]'s reload-and-retry loop. Each attempt rebuilds on a
	// strictly newer committed state, so a loop this long means a writer is being starved by
	// peers committing faster than it can; the flush then fails loudly rather than reporting a
	// success it did not achieve.
	maxCommitAttempts = 8

	// maxResolveAttempts bounds [Load]'s re-resolution when the generation it picked was
	// reclaimed underneath it. Reclamation only ever removes a generation a newer one
	// supersedes, so every retry starts from a newer state.
	maxResolveAttempts = 4

	// keepGenerations is how many committed generations survive reclamation. It is the window in
	// which a reader that listed the directory may still read what it found, and the LIST every
	// load pays for, so it is small — but not so small that reclamation races an ordinary load.
	keepGenerations = 8
)

// generationDir returns the prefix of the generation objects belonging to the index at key.
func generationDir(key string) string { return strings.TrimSuffix(key, ".bin") + "/" }

// GenerationKey returns the object key claiming generation g of the index stored at key.
//
// The name is fixed-width hex so lexicographic key order is generation order: a LIST of the
// directory resolves the newest committed index by taking its last entry.
func GenerationKey(key string, g Generation) string {
	return fmt.Sprintf("%s%016x-%016x.bin", generationDir(key), g.Term, g.Counter)
}

// IsGenerationKey reports whether key is a bucket index generation object.
func IsGenerationKey(key string) bool {
	dir, base := lastSlash(key)
	if !strings.HasSuffix(dir, "/"+Dir) {
		return false
	}

	_, ok := parseGeneration(base)

	return ok
}

// lastSlash splits key at its final separator into the directory and the basename.
func lastSlash(key string) (dir, base string) {
	i := strings.LastIndexByte(key, '/')
	if i < 0 {
		return "", key
	}

	return key[:i], key[i+1:]
}

// parseGeneration parses a generation object's basename.
func parseGeneration(base string) (Generation, bool) {
	name, ok := strings.CutSuffix(base, ".bin")
	if !ok {
		return Generation{}, false
	}

	term, counter, ok := strings.Cut(name, "-")
	if !ok {
		return Generation{}, false
	}

	t, err := strconv.ParseUint(term, 16, 64)
	if err != nil {
		return Generation{}, false
	}

	c, err := strconv.ParseUint(counter, 16, 64)
	if err != nil {
		return Generation{}, false
	}

	return Generation{Term: t, Counter: c}, true
}

// Commit installs the index that build renders as the successor of base, and returns the index it
// committed.
//
// The commit is the claim of a generation-named object with [backend.Backend.PutIfAbsent]: the
// generation a writer commits is the successor of the one it *observed*, so a peer that committed
// in between owns that name and this writer is told it lost instead of overwriting the peer's
// index. It then reloads, rebuilds on what actually got committed, and claims again — up to
// [maxCommitAttempts], after which it returns an [ErrConflict]-wrapping error. It never returns nil
// without having installed an index.
//
// build receives the state to build on and the generation the result will carry; it must be free of
// side effects, as it runs once per attempt.
func Commit(
	ctx context.Context,
	b backend.Backend,
	key string,
	term uint64,
	base *Index,
	build func(base *Index, g Generation) *Index,
) (*Index, error) {
	if base == nil {
		base = &Index{}
	}

	for range maxCommitAttempts {
		g := base.Generation.Next(term)

		ix := build(base, g)
		ix.Generation = g

		err := ix.commit(ctx, b, key)
		if err == nil {
			return ix, nil
		}

		if !errors.Is(err, ErrConflict) {
			return nil, err
		}

		if base, err = Load(ctx, b, key); err != nil {
			return nil, errors.Wrap(err, "reload after conflict")
		}
	}

	return nil, errors.Wrapf(ErrConflict, "index %q not committed in %d attempts", key, maxCommitAttempts)
}

// commit claims ix.Generation and, having won it, refreshes the object at key.
//
// The object at key is a full copy of the committed index, not a pointer to it: it is what every
// reader that knows only the conventional key — a deployment written by an older build, the
// replication mirror, the per-signal existence marker — goes on reading. Correctness does not
// depend on it, since [Load] takes whichever of it and the newest claimed generation is newer.
func (ix *Index) commit(ctx context.Context, b backend.Backend, key string) error {
	g := ix.Generation
	data := ix.AppendBinary(nil)

	claimed, err := b.PutIfAbsent(ctx, GenerationKey(key, g), data)
	if err != nil {
		return errors.Wrapf(err, "claim generation %d.%d of index %q", g.Term, g.Counter, key)
	}

	if !claimed {
		return errors.Wrapf(ErrConflict, "generation %d.%d of index %q", g.Term, g.Counter, key)
	}

	if err := b.Write(ctx, key, data); err != nil {
		// The claim is what makes a commit durable, so a claim left behind by a commit whose
		// caller was told it failed would be resolved and installed by the next load — a part set
		// published by a flush that reported an error. Dropping it keeps the commit point single.
		// A crash here cannot, and does not have to: the claim is a consistent index over objects
		// that were written before it.
		_ = b.Delete(ctx, GenerationKey(key, g))

		return errors.Wrapf(err, "write index %q", key)
	}

	reclaim(ctx, b, key, g)

	return nil
}

// reclaim drops the generation objects that [keepGenerations] newer ones supersede, once every
// keepGenerations commits — the LIST is not worth paying on every one, and the directory staying
// at twice the window costs nothing.
//
// It cannot delete an object a concurrent load is about to read: a load resolves the *newest*
// generation it lists, and reclamation only removes generations below the newest by the full
// window. A load unlucky enough to be that far behind reads a missing object and re-resolves
// ([maxResolveAttempts]) onto a newer one.
//
// Best-effort: a failed delete leaves an object the next reclamation collects.
func reclaim(ctx context.Context, b backend.Backend, key string, g Generation) {
	if g.Counter%keepGenerations != 0 {
		return
	}

	keys, err := b.List(ctx, generationDir(key))
	if err != nil || len(keys) <= keepGenerations {
		return
	}

	for _, k := range keys[:len(keys)-keepGenerations] {
		_ = b.Delete(ctx, k)
	}
}

// newest returns the newest generation claimed under key, if any.
func newest(ctx context.Context, b backend.Backend, key string) (Generation, bool, error) {
	keys, err := b.List(ctx, generationDir(key))
	if err != nil {
		return Generation{}, false, errors.Wrapf(err, "list generations of %q", key)
	}

	for _, k := range slices.Backward(keys) {
		_, base := lastSlash(k)
		if g, ok := parseGeneration(base); ok {
			return g, true, nil
		}
	}

	return Generation{}, false, nil
}

// read decodes the index stored at key, reporting whether the object exists.
func read(ctx context.Context, b backend.Backend, key string) (*Index, bool, error) {
	data, err := b.Read(ctx, key)
	if err != nil {
		if errors.Is(err, backend.ErrNotExist) {
			return nil, false, nil
		}

		return nil, false, errors.Wrapf(err, "read index %q", key)
	}

	ix, err := Decode(data)
	if err != nil {
		return nil, false, errors.Wrapf(err, "decode index %q", key)
	}

	return ix, true, nil
}
